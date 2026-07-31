package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// httpClient — клиент для всех HTTP-сценариев ниже.
//
// В боевом коде клиент почти всегда делают свой: у http.DefaultClient нет ни
// таймаута, ни настроенного пула соединений, и он общий на весь процесс —
// чужая библиотека может подкрутить его под себя. Таймаут здесь — страховка
// последней надежды на случай зависшего соединения; основным средством
// остановки остаётся контекст, который settle отменяет за нас при break.
var httpClient = &http.Client{
	Timeout: 5 * time.Second,
	Transport: &http.Transport{
		MaxIdleConns:        64,
		MaxIdleConnsPerHost: 16,
		IdleConnTimeout:     30 * time.Second,
	},
}

// drainLimit — сколько байт недочитанного тела мы готовы вычитать в мусор
// ради того, чтобы соединение вернулось в пул keep-alive.
//
// Порог существует потому, что оба крайних варианта плохи. Дочитывать до
// конца всегда — значит позволить чужому сервису занимать нашу горутину и
// трафик ровно столько, сколько ему вздумается: тело может быть бесконечным.
// Не дочитывать вовсе — значит терять соединение после каждого запроса.
// Компромисс: ответ, у которого остаток не влез в порог, стоит нам одного
// соединения, но не памяти и не времени. Для JSON-API, где ответы измеряются
// килобайтами, этот случай не наступает вовсе.
const drainLimit = 64 << 10

// fetchJSON выполняет GET и разбирает тело ответа как JSON в значение типа T.
//
// Три детали, без которых settle не смог бы честно отменять задачи:
//
//   - запрос строится через NewRequestWithContext. Только так отмена,
//     которую Stream делает на break, доходит до сетевого уровня: транспорт
//     закрывает соединение, а сервер на другом конце видит это как «клиент
//     ушёл». Обычный http.Get отмену проигнорирует и продолжит ждать ответ.
//   - тело дочитывается и закрывается — но не любой ценой. Транспорт вернёт
//     соединение в пул keep-alive, только если тело прочитано до EOF; Close
//     раньше времени заставляет его выбросить соединение, и следующий запрос
//     открывает новое — классическая утечка сокетов под нагрузкой. Поэтому
//     остаток дочитывается, но не дальше drainLimit: см. его описание, там
//     же и обоснование размена.
//   - чтение ограничено io.LimitReader: чужой сервис не должен иметь
//     возможности скормить нам гигабайт и выесть память.
func fetchJSON[T any](ctx context.Context, url string) (T, error) {
	var v T

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return v, err
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		// Здесь же окажется и context.Canceled после break — ошибка отмены
		// приезжает завёрнутой в *url.Error, но errors.Is её видит насквозь.
		return v, fmt.Errorf("GET %s: %w", url, err)
	}
	defer func() {
		// После Decode остаток тела обычно измеряется байтами, а на ветке с
		// ошибкой статуса — чем сервис прислал, тем и измеряется. Если он
		// длиннее drainLimit, Close случится до EOF и соединение будет
		// потеряно. Это осознанный размен, а не недосмотр.
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, drainLimit))
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return v, fmt.Errorf("GET %s: %s", url, resp.Status)
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&v); err != nil {
		return v, fmt.Errorf("GET %s: разбор ответа: %w", url, err)
	}
	return v, nil
}

// respondAfter — обработчик-заглушка: «думает» d, затем отвечает status и
// телом body (если оно не nil).
//
// Он специально уважает отмену запроса. Когда settle на break обрывает
// соединение, у обработчика отменяется r.Context() — и в выводе программы
// появляется строка «клиент отменил запрос». Это и есть доказательство, что
// отмена не остаётся внутри вашего процесса, а доезжает до чужого сервиса и
// экономит ему работу. В реальном сервере ту же роль играет ctx запроса,
// проброшенный до запроса в БД.
func respondAfter(name string, d time.Duration, status int, body any) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(d):
		case <-r.Context().Done():
			fmt.Printf("    [сервис %s] клиент отменил запрос — работу бросили\n", name)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		if body != nil {
			_ = json.NewEncoder(w).Encode(body)
		}
	}
}
