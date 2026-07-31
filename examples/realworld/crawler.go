package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"time"

	"github.com/vitikevich-landau/settle"
)

// Сценарий 6. Обход большого списка ссылок с записью в базу.
//
// Это самый «конвейерный» сценарий здесь, и он собирается целиком из готовых
// кусочков — ни одной строки собственного кода про горутины:
//
//	Map      ограничивает параллелизм и читает список лениво: ссылок может
//	         быть миллион, и ни материализовать их в срез, ни поднимать
//	         миллион горутин нельзя;
//	Retry    повторяет временные сбои (500, 429, обрыв связи) с растущей
//	         паузой, но не трогает 404 — повторять его бессмысленно;
//	Timeout  даёт дедлайн каждой ОТДЕЛЬНОЙ попытке, а не всем разом;
//	Ordered  возвращает исходный порядок ссылок, не дожидаясь конца обхода;
//	Batch    нарезает поток на пачки — единицы работы для вставки в базу.
//
// Почему повторы живут внутри задачи, а не в цикле обхода: движок обязан
// увидеть ровно один результат на ссылку. Перезапусти мы пачку целиком ради
// одной неудачной ссылки — заплатили бы всеми остальными запросами, а индексы
// и учёт разъехались бы.
//
// Запись идёт через канал в одну горутину-писателя. Это не про
// производительность, а про владение: строками владеет ровно один участник,
// поэтому мьютексы не нужны вовсе. Маленький буфер канала заодно даёт
// бэкпрешер — если база тормозит, обход сам замедлится, а не набьёт память.

// pageDoc — то, что мы достаём из страницы и складываем в базу.
type pageDoc struct {
	Title string `json:"title"`
	Size  int    `json:"size"`
}

// row — строка будущей таблицы. Позиция хранится ЯВНО: порядок должен быть
// данными, а не побочным эффектом того, в каком порядке строки доехали. Тогда
// он переживёт и параллельную запись, и повторный прогон, и докачку
// провалившихся ссылок вторым проходом.
type row struct {
	group    string
	position int
	url      string
	title    string
	size     int
	err      error
}

// httpError — ошибка со статусом ответа. Классифицировать по строке было бы
// хрупко, поэтому статус едет типом, а errors.As достаёт его из любой обёртки.
type httpError struct {
	Status int
	URL    string
}

func (e *httpError) Error() string {
	return fmt.Sprintf("GET %s: %d %s", e.URL, e.Status, http.StatusText(e.Status))
}

// isTransient — та самая предметная классификация, которую библиотека
// сознательно не берёт на себя: она принимает error и ничего не знает про
// HTTP. Повторяем 5xx, 429 и сетевые обрывы; 4xx (кроме 429) — окончательный
// отказ, повторять его значит впустую жечь время и чужие ресурсы.
func isTransient(err error) bool {
	// Отмена — не сбой сервиса: повторять нечего, мы уходим.
	//
	// А вот истёкший дедлайн отвергать НЕЛЬЗЯ, хотя соблазн велик. Задача
	// обёрнута в Timeout, и его срабатывание — это ровно тот медленный ответ,
	// ради повтора которого per-attempt дедлайн и ставят. Отличить его от
	// общего дедлайна выгрузки через errors.Is невозможно: context.DeadlineExceeded
	// одинаков у родительского и производного контекстов. Отличать и не нужно —
	// когда истечёт общий дедлайн, повторы остановит пауза: sleepCtx внутри
	// Retry вернёт ошибку контекста, не досыпая.
	if errors.Is(err, context.Canceled) {
		return false
	}
	var he *httpError
	if errors.As(err, &he) {
		return he.Status >= 500 || he.Status == http.StatusTooManyRequests
	}
	// Не HTTP-статус, а обрыв соединения или таймаут — такое повторяют.
	return true
}

func demoCrawler() {
	section("Обход списка ссылок с записью в базу",
		"Лимит параллелизма, повторы временных сбоев, исходный порядок\n"+
			"и запись пачками через канал — сборка из готовых кусочков.")

	// Стенд: страницы отвечают по-разному. Часть отдаёт 503 или 429 первые
	// две попытки и оживает на третьей, часть мертва навсегда.
	var requests atomic.Int64
	flaky := map[string]*atomic.Int64{}
	for _, p := range []string{"/docs/2", "/docs/5", "/news/1"} {
		flaky[p] = &atomic.Int64{}
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)

		if strings.HasSuffix(r.URL.Path, "/gone") {
			w.WriteHeader(http.StatusNotFound) // фатально: повторять нечего
			return
		}
		if c, ok := flaky[r.URL.Path]; ok {
			switch c.Add(1) {
			case 1:
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			case 2:
				w.WriteHeader(http.StatusTooManyRequests)
				return
			}
		}

		respondAfter("crawler", 5*time.Millisecond, http.StatusOK, pageDoc{
			Title: "страница " + r.URL.Path,
			Size:  len(r.URL.Path) * 100,
		})(w, r)
	}))
	defer srv.Close()

	// Логические группы: у каждой свой список и свой лимит параллелизма —
	// например потому, что это разные хосты с разной терпимостью к нагрузке.
	groups := []struct {
		name  string
		limit int
		paths []string
	}{
		{name: "docs", limit: 4, paths: []string{"/docs/1", "/docs/2", "/docs/3", "/docs/gone", "/docs/5", "/docs/6"}},
		{name: "news", limit: 2, paths: []string{"/news/1", "/news/2", "/news/gone"}},
	}

	// Общий дедлайн всей выгрузки — просто свойство контекста. Отдельной опции
	// у движка для этого нет и не нужно.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Канал к писателю — граница стадий. Буфер на пару пачек: пока пишется
	// одна, обход готовит следующую, но убежать далеко вперёд не может.
	batches := make(chan []row, 2)
	stored := make(chan storeReport)
	go storeRows(batches, stored)

	policy := settle.RetryPolicy{
		// Экспонента с потолком и джиттером: джиттер расталкивает повторы
		// одновременно стартовавших задач, иначе они накроют уже
		// перегруженный сервис синхронной волной.
		Backoff: settle.Jitter(settle.Cap(
			settle.Exponential(10*time.Millisecond, 2, 4), 200*time.Millisecond), 0.3),
		Retryable: isTransient,
	}

	var peak atomic.Int64
	start := time.Now()

	for _, g := range groups {
		var inFlight atomic.Int64

		// Вход — ленивая последовательность. Здесь это срез, но с тем же
		// успехом это мог бы быть курсор по таблице или построчное чтение
		// файла: Map тянет из неё ровно столько, сколько помещается в лимит.
		results := settle.Map(ctx, sliceSeq(g.paths), g.limit,
			func(ctx context.Context, path string) (pageDoc, error) {
				now := inFlight.Add(1)
				trackPeak(&peak, now)
				defer inFlight.Add(-1)

				// Порядок обёрток: Retry снаружи — значит дедлайн у каждой
				// попытки свой. Поменяй местами, и две секунды пришлось бы
				// делить на все попытки разом.
				return settle.Retry(policy,
					settle.Timeout(2*time.Second, func(ctx context.Context) (pageDoc, error) {
						return fetchPage(ctx, srv.URL+path)
					}))(ctx)
			})

		// Ordered возвращает исходный порядок, Batch нарезает на пачки.
		// Обе обёртки — обычные последовательности, поэтому читаются справа
		// налево как конвейер и ничего не знают про то, кто запустил задачи.
		for chunk := range settle.Batch(settle.Ordered(results), 4) {
			rows := make([]row, 0, len(chunk))
			for _, item := range chunk {
				rows = append(rows, row{
					group:    g.name,
					position: item.Index, // позиция во входном списке
					url:      g.paths[item.Index],
					title:    item.Value.Title,
					size:     item.Value.Size,
					err:      item.Err,
				})
			}
			batches <- rows
		}
	}

	close(batches)
	report := <-stored

	fmt.Printf("  → записано %d строк, в dead-letter %d, HTTP-запросов ушло %d за %s\n",
		report.written, len(report.failed), requests.Load(), since(start))
	fmt.Printf("     пик параллелизма: %d (лимиты групп: 4 и 2)\n", peak.Load())
	for _, f := range report.failed {
		fmt.Printf("     ✗ %s#%d %s: %s\n", f.group, f.position, f.url, shortErr(f.err))
	}
}

// storeReport — итог работы писателя.
type storeReport struct {
	written int
	failed  []row
}

// storeRows — единственный владелец «базы». Никаких мьютексов: строками
// владеет одна горутина, а данные приезжают к ней по каналу. В боевом коде
// здесь была бы транзакция с батч-вставкой (COPY или многострочный INSERT) и
// ON CONFLICT (url) DO UPDATE, чтобы повторный прогон был идемпотентным.
func storeRows(batches <-chan []row, done chan<- storeReport) {
	var report storeReport
	for batch := range batches {
		// Неудачные ссылки не смешиваем с данными: они уходят в отдельную
		// таблицу, по которой потом можно сделать второй проход, не перекачивая
		// всё заново.
		for _, r := range batch {
			if r.err != nil {
				report.failed = append(report.failed, r)
				continue
			}
			report.written++
		}
		// Имитация работы базы: пока она идёт, обход уже готовит следующую
		// пачку — ровно ради этого стадии и разделены каналом.
		time.Sleep(2 * time.Millisecond)
	}
	done <- report
}

// fetchPage — одна попытка скачивания. Статус едет в ошибке типом, чтобы
// isTransient мог принять решение, не разбирая строки.
func fetchPage(ctx context.Context, url string) (pageDoc, error) {
	doc, err := fetchJSON[pageDoc](ctx, url)
	if err == nil {
		return doc, nil
	}
	// fetchJSON отдаёт статус текстом; для примера достаточно распознать два
	// интересных случая и превратить их в типизированную ошибку.
	for _, s := range []int{
		http.StatusNotFound,
		http.StatusTooManyRequests,
		http.StatusServiceUnavailable,
	} {
		if strings.Contains(err.Error(), fmt.Sprint(s)) {
			return pageDoc{}, &httpError{Status: s, URL: url}
		}
	}
	return pageDoc{}, err
}

// sliceSeq — последовательность из среза. В боевом коде на её месте был бы
// курсор по таблице или построчное чтение файла — Map всё равно, откуда
// приходят элементы, лишь бы приходили лениво.
func sliceSeq[T any](items []T) func(func(T) bool) {
	return func(yield func(T) bool) {
		for _, v := range items {
			if !yield(v) {
				return
			}
		}
	}
}

// trackPeak запоминает максимум одновременно работающих задач.
func trackPeak(peak *atomic.Int64, now int64) {
	for {
		max := peak.Load()
		if now <= max || peak.CompareAndSwap(max, now) {
			return
		}
	}
}
