package settle

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Тесты ниже проверяют не отдельные функции, а их сборки — те самые варианты
// использования, ради которых пакет и разложен на ядро, трансформеры и
// декораторы. Здесь важно, что кусочки не мешают друг другу: контракт «ровно
// один результат на задачу», индексы, порядок и уборка горутин переживают
// любую комбинацию.

// Полный конвейер обхода: ленивый вход → лимит параллелизма → повторы →
// исходный порядок → пачки для записи.
func TestPipelineMapRetryOrderedBatch(t *testing.T) {
	base := runtime.NumGoroutine()

	const (
		total = 20
		limit = 4
		batch = 6
	)
	errTemporary := errors.New("503")
	errFatal := errors.New("404")

	items := make([]int, total)
	for i := range items {
		items[i] = i
	}

	var attempts [total]atomic.Int64
	var inFlight, peak atomic.Int64

	policy := RetryPolicy{
		Backoff:   Constant(time.Millisecond, 5),
		Retryable: func(err error) bool { return errors.Is(err, errTemporary) },
	}

	results := Map(context.Background(), slices.Values(items), limit,
		func(ctx context.Context, v int) (string, error) {
			now := inFlight.Add(1)
			for {
				max := peak.Load()
				if now <= max || peak.CompareAndSwap(max, now) {
					break
				}
			}
			defer inFlight.Add(-1)

			return Retry(policy, Timeout(2*time.Second,
				func(context.Context) (string, error) {
					n := attempts[v].Add(1)
					switch {
					case v%7 == 3: // безнадёжные — фатальная ошибка без повторов
						return "", errFatal
					case v%3 == 0 && n < 3: // мигающие — оживают с третьей попытки
						return "", errTemporary
					}
					return fmt.Sprintf("item-%d", v), nil
				}))(ctx)
		})

	var (
		sizes    []int
		gotIdx   []int
		okCount  int
		errCount int
	)
	for chunk := range Batch(Ordered(results), batch) {
		sizes = append(sizes, len(chunk))
		for _, item := range chunk {
			gotIdx = append(gotIdx, item.Index)
			if item.Err != nil {
				errCount++
				continue
			}
			okCount++
			if want := fmt.Sprintf("item-%d", item.Index); item.Value != want {
				t.Errorf("index %d carries %q, want %q", item.Index, item.Value, want)
			}
		}
	}

	// Порядок входа сохранён на всём пути через две обёртки.
	want := make([]int, total)
	for i := range want {
		want[i] = i
	}
	if !slices.Equal(gotIdx, want) {
		t.Fatalf("порядок нарушен: %v", gotIdx)
	}
	if wantSizes := []int{6, 6, 6, 2}; !slices.Equal(sizes, wantSizes) {
		t.Errorf("want chunk sizes %v, got %v", wantSizes, sizes)
	}
	if got := peak.Load(); got > limit {
		t.Errorf("лимит параллелизма нарушен: пик %d при лимите %d", got, limit)
	}
	// 3, 10, 17 — фатальные; остальные обязаны доехать.
	if wantErrs := 3; errCount != wantErrs || okCount != total-wantErrs {
		t.Errorf("want %d failures and %d successes, got %d/%d", wantErrs, total-wantErrs, errCount, okCount)
	}
	// Фатальные не повторялись, мигающие повторялись ровно дважды.
	if got := attempts[3].Load(); got != 1 {
		t.Errorf("фатальная задача повторялась: %d попыток", got)
	}
	if got := attempts[6].Load(); got != 3 {
		t.Errorf("мигающая задача: want 3 attempts, got %d", got)
	}
	waitNoExtraGoroutines(t, base)
}

// Прерывание конвейера посередине: всё, что запущено, обязано свернуться.
func TestPipelineBreakCancelsEverything(t *testing.T) {
	base := runtime.NumGoroutine()

	var started, finished atomic.Int64
	items := make([]int, 200)
	for i := range items {
		items[i] = i
	}

	results := Map(context.Background(), slices.Values(items), 8,
		func(ctx context.Context, v int) (int, error) {
			started.Add(1)
			defer finished.Add(1)
			select {
			case <-time.After(10 * time.Millisecond):
				return v, nil
			case <-ctx.Done():
				return 0, ctx.Err()
			}
		})

	var seen int
	for range Ordered(results) {
		seen++
		if seen == 5 {
			break
		}
	}

	// Ленивый вход: до конца списка обход даже не дошёл.
	if got := started.Load(); got >= int64(len(items)) {
		t.Errorf("вход прочитан целиком (%d задач) несмотря на ранний выход", got)
	}
	// Структурная конкурентность: к моменту выхода все запущенные завершились.
	if s, f := started.Load(), finished.Load(); s != f {
		t.Errorf("после выхода остались работающие задачи: запущено %d, завершено %d", s, f)
	}
	waitNoExtraGoroutines(t, base)
}

// Общий дедлайн всей выгрузки — свойство контекста, а не опция движка.
func TestPipelineDeadlineAppliesToWholeRun(t *testing.T) {
	base := runtime.NumGoroutine()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	items := make([]int, 50)
	for i := range items {
		items[i] = i
	}

	start := time.Now()
	var timedOut int
	for _, r := range Map(ctx, slices.Values(items), 4,
		func(ctx context.Context, v int) (int, error) {
			select {
			case <-time.After(30 * time.Millisecond):
				return v, nil
			case <-ctx.Done():
				return 0, ctx.Err()
			}
		}) {
		if errors.Is(r.Err, context.DeadlineExceeded) {
			timedOut++
		}
	}

	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("дедлайн не остановил обход: заняло %s", elapsed)
	}
	if timedOut == 0 {
		t.Error("ни одна задача не увидела дедлайн — тест ничего не проверил")
	}
	waitNoExtraGoroutines(t, base)
}

// Логические группы: комбинатор верхнего уровня над группами, внутри каждой —
// свой Map со своим лимитом. Механизм конкурентности при этом один.
func TestGroupedWorkloads(t *testing.T) {
	base := runtime.NumGoroutine()

	group := func(name string, n int, limit int) func(context.Context) ([]string, error) {
		return func(ctx context.Context) ([]string, error) {
			items := make([]int, n)
			for i := range items {
				items[i] = i
			}
			values, err := Values(Map(ctx, slices.Values(items), limit,
				func(_ context.Context, v int) (string, error) {
					return fmt.Sprintf("%s-%d", name, v), nil
				}))
			return values, err
		}
	}

	groups := AllSettled(context.Background(),
		group("docs", 5, 2),
		group("news", 3, 3),
	)

	if len(groups) != 2 {
		t.Fatalf("want 2 groups, got %d", len(groups))
	}
	if err := groups[0].Err; err != nil {
		t.Fatalf("docs: %v", err)
	}
	if want := []string{"docs-0", "docs-1", "docs-2", "docs-3", "docs-4"}; !slices.Equal(groups[0].Value, want) {
		t.Errorf("want %v, got %v", want, groups[0].Value)
	}
	if want := []string{"news-0", "news-1", "news-2"}; !slices.Equal(groups[1].Value, want) {
		t.Errorf("want %v, got %v", want, groups[1].Value)
	}
	waitNoExtraGoroutines(t, base)
}

// Паника в одной задаче не должна ронять многочасовой обход: она приезжает
// ошибкой, остальные задачи доезжают нормально.
func TestPipelineSurvivesPanicMidway(t *testing.T) {
	base := runtime.NumGoroutine()

	items := make([]int, 30)
	for i := range items {
		items[i] = i
	}

	values, err := Values(Ordered(Map(context.Background(), slices.Values(items), 5,
		func(_ context.Context, v int) (int, error) {
			if v == 13 {
				panic("bad page")
			}
			return v, nil
		})))

	if len(values) != 29 {
		t.Fatalf("want 29 successful values, got %d", len(values))
	}
	var pe *PanicError
	if !errors.As(err, &pe) || pe.Value != "bad page" {
		t.Fatalf("want PanicError(bad page), got %v", err)
	}
	if !strings.Contains(err.Error(), "task 13") {
		t.Errorf("want the failing index in the error, got %q", err)
	}
	waitNoExtraGoroutines(t, base)
}

// Errors — форма для задач-эффектов: значения не нужны, важен список неудач.
func TestEffectsOnlyWorkload(t *testing.T) {
	base := runtime.NumGoroutine()
	errRefused := errors.New("smtp refused")

	items := []int{0, 1, 2, 3}
	err := Errors(Map(context.Background(), slices.Values(items), 2,
		func(_ context.Context, v int) (struct{}, error) {
			if v%2 == 1 {
				return struct{}{}, errRefused
			}
			return struct{}{}, nil
		}))

	if !errors.Is(err, errRefused) {
		t.Fatalf("want refused, got %v", err)
	}
	joined, ok := err.(interface{ Unwrap() []error })
	if !ok || len(joined.Unwrap()) != 2 {
		t.Fatalf("want 2 joined failures, got %v", err)
	}
	// Порядок неудач — по индексам задач, а не по времени завершения.
	lines := strings.Split(err.Error(), "\n")
	if len(lines) != 2 || !strings.HasPrefix(lines[0], "settle: task 1:") ||
		!strings.HasPrefix(lines[1], "settle: task 3:") {
		t.Errorf("want failures ordered by index, got %q", lines)
	}
	waitNoExtraGoroutines(t, base)
}

// Медленный потребитель не должен приводить к разбуханию: Ordered не читает
// источник, пока не отдал накопленное, а Map не берёт новые элементы, пока
// заняты все слоты.
func TestBackpressureThroughOrdered(t *testing.T) {
	base := runtime.NumGoroutine()
	const limit = 3

	var pulled, running atomic.Int64
	items := make([]int, 100)
	for i := range items {
		items[i] = i
	}

	src := func(yield func(int) bool) {
		for _, v := range items {
			pulled.Add(1)
			if !yield(v) {
				return
			}
		}
	}

	var consumed int
	for range Ordered(Map(context.Background(), src, limit,
		func(_ context.Context, v int) (int, error) {
			running.Add(1)
			return v, nil
		})) {
		consumed++
		// Потребитель нарочно медленный.
		time.Sleep(2 * time.Millisecond)
		if consumed == 10 {
			break
		}
	}

	// За десять потреблённых элементов обход не должен был убежать вперёд на
	// весь список: запас ограничен лимитом, буфером канала и реордер-буфером.
	if got := pulled.Load(); got > 40 {
		t.Errorf("бэкпрешера нет: при 10 потреблённых из входа вытянуто %d", got)
	}
	waitNoExtraGoroutines(t, base)
}
