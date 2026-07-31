package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/vitikevich-landau/settle"
)

// Сценарий 3. Ручка /readyz: опросить все зависимости сервиса разом.
//
// Здесь settle работает в режиме, противоположном предыдущим сценариям:
// break запрещён. Оркестратору мало знать, что «что-то сломалось», — ему и
// дежурному нужен полный отчёт: кто именно лёг, кто ответил и за сколько.
// Прерваться на первой же неудаче значит потерять картину целиком.
//
// Ограничитель здесь один — общий дедлайн: проба, которая не уложилась,
// приезжает с context.DeadlineExceeded и трактуется как отказ. Без дедлайна
// зависшая проба подвесила бы саму ручку здоровья, и оркестратор посчитал бы
// сервис мёртвым по таймауту HTTP — с гораздо менее внятным диагнозом.

// probe — результат успешной пробы: имя зависимости и время ответа.
// Задержку полезно отдавать наружу: по ней видно деградацию задолго до того,
// как зависимость окончательно ляжет.
type probe struct {
	Name    string
	Latency time.Duration
}

// dependency — описание одной зависимости стенда.
type dependency struct {
	name     string
	delay    time.Duration
	err      error
	critical bool // без критичной зависимости сервис не готов принимать трафик
}

func demoHealth() {
	section("Ручка /readyz: опрос всех зависимостей",
		"Полный отчёт важнее скорости, поэтому здесь никакого break: цикл\n"+
			"проходится до конца, а рамку по времени задаёт общий дедлайн.")

	deps := []dependency{
		{name: "postgres", delay: 25 * time.Millisecond, critical: true},
		{name: "redis", delay: 40 * time.Millisecond, critical: true},
		{name: "kafka", delay: 15 * time.Millisecond, critical: true,
			err: errors.New("нет лидера партиции")},
		// Хранилище картинок отвечает мучительно долго и в дедлайн не влезет.
		// Оно не критично: без картинок сервис работает, просто хуже.
		{name: "s3", delay: 300 * time.Millisecond, critical: false},
	}

	// Бюджет ручки здоровья. Он должен быть заметно меньше таймаута, с
	// которым к вам ходит оркестратор, иначе диагноз не успеет доехать.
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()

	tasks := make([]func(context.Context) (probe, error), len(deps))
	for i, d := range deps {
		tasks[i] = pingDependency(d)
	}

	start := time.Now()
	// Отчёт раскладывается по индексу: результаты приезжают в порядке
	// завершения (первой будет упавшая kafka), а печатать их хочется в
	// порядке объявления — так строки отчёта не пляшут от запуска к запуску.
	report := make([]settle.Result[probe], len(deps))
	for i, r := range settle.Stream(ctx, tasks...) {
		report[i] = r
	}

	ready := true
	for i, r := range report {
		d := deps[i]
		switch {
		case r.Err == nil:
			// Имя берём из самой пробы — но только в успешной ветке: при
			// ошибке Value нулевой, и опознать зависимость можно лишь по
			// индексу, то есть по deps[i].
			fmt.Printf("  ✓ %-9s ok, %s\n", r.Value.Name, ms(r.Value.Latency))

		case errors.Is(r.Err, context.DeadlineExceeded):
			// Отдельная ветка не для красоты: таймаут и явная ошибка — это
			// разные инциденты. Первый чаще про сеть и перегрузку, второй —
			// про саму зависимость.
			fmt.Printf("  ✗ %-9s не уложилась в дедлайн\n", d.name)

		default:
			fmt.Printf("  ✗ %-9s %v\n", d.name, r.Err)
		}

		if r.Err != nil && d.critical {
			ready = false
		}
	}

	// Итоговый вердикт ручки. Обратите внимание: ответ отдаётся примерно за
	// длину дедлайна, а не за сумму задержек всех зависимостей.
	if ready {
		fmt.Printf("  → 200 ready (проверка заняла %s)\n", since(start))
	} else {
		fmt.Printf("  → 503 not ready (проверка заняла %s)\n", since(start))
	}
}

// pingDependency собирает пробу для одной зависимости.
//
// Проба обязана уважать контекст — иначе дедлайн ручки ничего не решает, а
// зависшая проба переживёт сам ответ. В настоящем коде здесь были бы
// db.PingContext, redis-клиент с ctx или HEAD-запрос с NewRequestWithContext.
//
// При ошибке проба возвращает нулевой probe: это контракт settle — если
// Err != nil, в Value смотреть не нужно.
func pingDependency(d dependency) func(context.Context) (probe, error) {
	return func(ctx context.Context) (probe, error) {
		start := time.Now()
		select {
		case <-time.After(d.delay):
		case <-ctx.Done():
			return probe{}, ctx.Err()
		}
		if d.err != nil {
			return probe{}, d.err
		}
		return probe{Name: d.name, Latency: time.Since(start)}, nil
	}
}
