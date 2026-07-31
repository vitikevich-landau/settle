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

// Сценарий 2. Отказоустойчивое чтение: одни и те же данные лежат в нескольких
// репликах, нам нужен первый успешный ответ.
//
// Это Promise.any: ошибка одной реплики — не повод сдаваться, но и ждать всех
// незачем. Как только пришёл первый успех, остальные запросы бессмысленны, и
// break их отменяет — чужие сервисы перестают тратить на нас ресурсы.
//
// Второй приём здесь — hedged requests. Дублировать запрос сразу во все
// реплики значит утроить нагрузку на бэкенд ради редких «хвостовых» задержек.
// Поэтому дубли стартуют с задержкой: если ближняя реплика уложилась в свой
// обычный бюджет, второй и третий запрос просто не понадобятся.

// rate — котировка валютной пары, ответ реплики.
type rate struct {
	Pair  string  `json:"pair"`
	Value float64 `json:"value"`
}

// replica — описание одной реплики стенда.
type replica struct {
	name  string
	delay time.Duration // сколько «думает» сервер
	hedge time.Duration // насколько отложен старт дубля
	url   string
}

// requestsIssued считает, сколько запросов реально ушло в сеть. Весь смысл
// hedged requests в том, чтобы это число было меньше числа реплик.
var requestsIssued atomic.Int32

func demoFailover() {
	section("Реплики и hedged requests",
		"Три реплики одного API. Побеждает первый успешный ответ; отставшие\n"+
			"отменяются, а те, чья очередь ещё не подошла, вообще не стартуют.")

	// Ближняя реплика нестабильна, средняя здорова, дальняя медленная.
	replicas := []*replica{
		{name: "eu-1", delay: 15 * time.Millisecond, hedge: 0},
		{name: "eu-2", delay: 45 * time.Millisecond, hedge: 25 * time.Millisecond},
		{name: "us-1", delay: 300 * time.Millisecond, hedge: 100 * time.Millisecond},
	}

	for i, rep := range replicas {
		// В прогоне 1 ближняя реплика отвечает 503 — так виден смысл any:
		// ошибка не останавливает цикл, мы просто ждём следующего ответа.
		status, payload := http.StatusOK, any(rate{Pair: "USD/RUB", Value: 78.42 + float64(i)/100})
		if rep.name == "eu-1" {
			status, payload = http.StatusServiceUnavailable, nil
		}
		mux := http.NewServeMux()
		mux.HandleFunc("/rate", func(w http.ResponseWriter, r *http.Request) {
			// Флаг ?fail=1 роняет реплику — им пользуется прогон 2.
			if r.URL.Query().Get("fail") != "" {
				respondAfter(rep.name, 10*time.Millisecond, http.StatusServiceUnavailable, nil)(w, r)
				return
			}
			respondAfter(rep.name, rep.delay, status, payload)(w, r)
		})
		srv := httptest.NewServer(mux)
		defer srv.Close()
		rep.url = srv.URL + "/rate"
	}

	fmt.Println("\n-- прогон 1: eu-1 отвечает 503, побеждает eu-2")
	askReplicas(replicas, "")

	// Прогон 2: живых реплик нет. Цикл дойдёт до конца, ни разу не сделав
	// break, и мы соберём все ошибки — это ровно AggregateError из
	// Promise.any, только собранный через errors.Join.
	fmt.Println("\n-- прогон 2: живых реплик нет")
	askReplicas(replicas, "?fail=1")
}

// askReplicas реализует Promise.any: первый успех побеждает, ошибки
// накапливаются, и если успеха не случилось — отдаём их все разом.
func askReplicas(replicas []*replica, query string) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	requestsIssued.Store(0)

	// Все реплики возвращают один и тот же тип, поэтому здесь данные едут
	// обычным путём — через Result.Value, без раскладывания по полям
	// структуры, как это пришлось делать в сценарии с BFF.
	tasks := make([]func(context.Context) (rate, error), len(replicas))
	for i, rep := range replicas {
		url := rep.url + query
		tasks[i] = hedged(rep.hedge, func(ctx context.Context) (rate, error) {
			requestsIssued.Add(1)
			return fetchJSON[rate](ctx, url)
		})
	}

	start := time.Now()
	var (
		winner rate
		from   string
		errs   []error
	)

	for i, r := range settle.Stream(ctx, tasks...) {
		if r.Err != nil {
			// Ошибка реплики — не конец: продолжаем слушать остальных.
			fmt.Printf("  ✗ %s: %v\n", replicas[i].name, shortErr(r.Err))
			errs = append(errs, fmt.Errorf("%s: %w", replicas[i].name, r.Err))
			continue
		}
		winner, from = r.Value, replicas[i].name
		break // отменяет отставших и не даёт стартовать тем, чей дубль отложен
	}

	if from == "" {
		// errors.Join складывает ошибки в одну; errors.Is и errors.As умеют
		// искать по всем ветвям сразу, так что сентинели не теряются.
		fmt.Printf("  → ни одна реплика не ответила за %s:\n%v\n",
			since(start), indent(errors.Join(errs...).Error()))
		return
	}

	fmt.Printf("  → %s = %.2f от %s за %s; запросов ушло в сеть: %d из %d\n",
		winner.Pair, winner.Value, from, since(start), requestsIssued.Load(), len(tasks))
}

// hedged откладывает старт задачи на delay. Если победитель нашёлся раньше,
// break отменит контекст ещё до того, как эта задача что-то отправит: дубль
// не будет стоить ни запроса, ни соединения.
//
// Приём окупается, когда p50 сервиса заметно ниже p99: задержку выбирают на
// уровне где-то p90, и тогда дубли уходят только для «хвоста» запросов.
func hedged[T any](delay time.Duration, fn func(context.Context) (T, error)) func(context.Context) (T, error) {
	return func(ctx context.Context) (T, error) {
		if delay > 0 {
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				var zero T
				return zero, ctx.Err()
			}
		}
		return fn(ctx)
	}
}

// shortErr укорачивает сетевые ошибки: в выводе примера важен факт отказа, а
// не полный URL со случайным портом httptest.
func shortErr(err error) string {
	const maxLen = 96
	r := []rune(err.Error())
	if len(r) > maxLen {
		return string(r[:maxLen]) + "…"
	}
	return string(r)
}

// indent сдвигает многострочный текст ошибки, чтобы вывод не разъезжался.
func indent(s string) string {
	return "     " + strings.ReplaceAll(strings.TrimRight(s, "\n"), "\n", "\n     ")
}
