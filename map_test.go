package settle

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"sync/atomic"
	"testing"
	"time"
)

// seqOf — обычная последовательность из среза, аналог slices.Values.
func seqOf[T any](items ...T) func(func(T) bool) {
	return func(yield func(T) bool) {
		for _, v := range items {
			if !yield(v) {
				return
			}
		}
	}
}

// counted оборачивает последовательность счётчиком вытянутых элементов:
// на нём видно, что Map читает вход лениво, а не материализует его целиком.
func counted[T any](items []T, pulled *atomic.Int64) func(func(T) bool) {
	return func(yield func(T) bool) {
		for _, v := range items {
			pulled.Add(1)
			if !yield(v) {
				return
			}
		}
	}
}

func TestMapIndexesByInputPosition(t *testing.T) {
	base := runtime.NumGoroutine()

	// Задачи завершаются в порядке, обратном входу, — индекс обязан остаться
	// привязанным к позиции элемента, а не к моменту завершения.
	gates := make([]chan struct{}, 4)
	for i := range gates {
		gates[i] = make(chan struct{})
	}
	close(gates[3])

	got := make(map[int]string)
	for i, r := range Map(context.Background(), seqOf(0, 1, 2, 3), 4,
		func(_ context.Context, v int) (string, error) {
			<-gates[v]
			if v > 0 {
				close(gates[v-1])
			}
			return fmt.Sprintf("v%d", v), nil
		}) {
		if r.Err != nil {
			t.Fatalf("task %d: unexpected error: %v", i, r.Err)
		}
		got[i] = r.Value
	}

	want := map[int]string{0: "v0", 1: "v1", 2: "v2", 3: "v3"}
	if len(got) != len(want) {
		t.Fatalf("want %d results, got %#v", len(want), got)
	}
	for i, v := range want {
		if got[i] != v {
			t.Errorf("index %d: want %q, got %q", i, v, got[i])
		}
	}
	waitNoExtraGoroutines(t, base)
}

func TestMapLimitsConcurrency(t *testing.T) {
	base := runtime.NumGoroutine()
	const limit = 3

	var inFlight, maxInFlight atomic.Int64
	items := make([]int, 50)
	for i := range items {
		items[i] = i
	}

	for range Map(context.Background(), slices.Values(items), limit,
		func(_ context.Context, v int) (int, error) {
			now := inFlight.Add(1)
			for {
				max := maxInFlight.Load()
				if now <= max || maxInFlight.CompareAndSwap(max, now) {
					break
				}
			}
			// Небольшая работа, чтобы окна задач реально перекрывались.
			time.Sleep(time.Millisecond)
			inFlight.Add(-1)
			return v, nil
		}) {
	}

	if got := maxInFlight.Load(); got > limit {
		t.Fatalf("want at most %d concurrent tasks, saw %d", limit, got)
	}
	if got := maxInFlight.Load(); got < 2 {
		t.Fatalf("tasks did not overlap at all (max in flight %d) — тест ничего не проверил", got)
	}
	waitNoExtraGoroutines(t, base)
}

func TestMapReadsInputLazily(t *testing.T) {
	base := runtime.NumGoroutine()
	const limit = 2

	var pulled atomic.Int64
	release := make(chan struct{})
	items := make([]int, 100)
	for i := range items {
		items[i] = i
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for range Map(context.Background(), counted(items, &pulled), limit,
			func(_ context.Context, v int) (int, error) {
				<-release
				return v, nil
			}) {
		}
	}()

	// Пока задачи держат все слоты, вход не должен вычитываться дальше:
	// в полёте limit задач плюс один элемент, который диспетчер уже достал и
	// с которым ждёт свободный слот.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if pulled.Load() >= limit {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	if got := pulled.Load(); got > limit+1 {
		t.Errorf("want at most %d pulled items while tasks are blocked, got %d", limit+1, got)
	}

	close(release)
	<-done
	if got := pulled.Load(); got != int64(len(items)) {
		t.Errorf("want all %d items pulled by the end, got %d", len(items), got)
	}
	waitNoExtraGoroutines(t, base)
}

// Главное отличие Map от обхода пачками: медленная задача не задерживает
// остальные. Если бы Map ставил барьер, этот тест не смог бы завершиться —
// задача 0 ждёт, пока закончатся все следующие.
func TestMapHasNoBatchBarrier(t *testing.T) {
	base := runtime.NumGoroutine()
	othersDone := make(chan struct{})

	var completed []int
	for i, r := range Map(context.Background(), seqOf(0, 1, 2, 3, 4, 5), 2,
		func(_ context.Context, v int) (int, error) {
			if v == 0 {
				select {
				case <-othersDone:
				case <-time.After(5 * time.Second):
					return 0, errors.New("барьер: задача 0 не дождалась остальных")
				}
			}
			return v, nil
		}) {
		if r.Err != nil {
			t.Fatalf("task %d: %v", i, r.Err)
		}
		completed = append(completed, i)
		if len(completed) == 5 {
			close(othersDone)
		}
	}

	if len(completed) != 6 {
		t.Fatalf("want 6 results, got %v", completed)
	}
	if completed[5] != 0 {
		t.Errorf("want task 0 to finish last, got order %v", completed)
	}
	waitNoExtraGoroutines(t, base)
}

func TestMapBreakCancelsAndWaits(t *testing.T) {
	base := runtime.NumGoroutine()
	started := make(chan struct{})
	cancelled := make(chan struct{})
	var once atomic.Bool

	for _, r := range Map(context.Background(), seqOf(0, 1), 2,
		func(ctx context.Context, v int) (int, error) {
			if v == 1 {
				if once.CompareAndSwap(false, true) {
					<-started
					<-ctx.Done()
					close(cancelled)
				}
				return 0, ctx.Err()
			}
			close(started)
			return v, nil
		}) {
		if r.Err == nil {
			break
		}
	}

	select {
	case <-cancelled:
	default:
		t.Fatal("Map вернулся раньше, чем оставшаяся задача увидела отмену")
	}
	waitNoExtraGoroutines(t, base)
}

func TestMapCapturesPanic(t *testing.T) {
	base := runtime.NumGoroutine()

	var results []Result[int]
	for _, r := range Map(context.Background(), seqOf(0, 1, 2), 3,
		func(_ context.Context, v int) (int, error) {
			if v == 1 {
				panic("boom")
			}
			return v, nil
		}) {
		results = append(results, r)
	}

	if len(results) != 3 {
		t.Fatalf("want 3 results even with a panicking task, got %d", len(results))
	}
	var panics int
	for _, r := range results {
		var pe *PanicError
		if errors.As(r.Err, &pe) && pe.Value == "boom" {
			panics++
		}
	}
	if panics != 1 {
		t.Fatalf("want exactly one PanicError, got %d (%v)", panics, results)
	}
	waitNoExtraGoroutines(t, base)
}

// Goexit убивает горутину задачи, но не должен «съедать» слот семафора:
// оставшиеся элементы входа обязаны быть обработаны.
func TestMapGoexitDoesNotStarvePool(t *testing.T) {
	base := runtime.NumGoroutine()

	seen := make(map[int]Result[int])
	for i, r := range Map(context.Background(), seqOf(0, 1, 2, 3, 4), 1,
		func(_ context.Context, v int) (int, error) {
			if v == 0 {
				runtime.Goexit()
			}
			return v, nil
		}) {
		seen[i] = r
	}

	if len(seen) != 5 {
		t.Fatalf("want 5 results, got %d: %v", len(seen), seen)
	}
	if !errors.Is(seen[0].Err, ErrGoexit) {
		t.Errorf("want ErrGoexit for task 0, got %v", seen[0].Err)
	}
	for i := 1; i < 5; i++ {
		if seen[i].Err != nil || seen[i].Value != i {
			t.Errorf("task %d: want (%d, nil), got (%d, %v)", i, i, seen[i].Value, seen[i].Err)
		}
	}
	waitNoExtraGoroutines(t, base)
}

func TestMapWithEmptyInput(t *testing.T) {
	base := runtime.NumGoroutine()
	n := 0
	for range Map(context.Background(), seqOf[int](), 4,
		func(_ context.Context, v int) (int, error) { return v, nil }) {
		n++
	}
	if n != 0 {
		t.Fatalf("want no results, got %d", n)
	}
	waitNoExtraGoroutines(t, base)
}

func TestMapWithCancelledContext(t *testing.T) {
	base := runtime.NumGoroutine()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	for _, r := range Map(ctx, seqOf(0, 1, 2), 2,
		func(ctx context.Context, v int) (int, error) {
			<-ctx.Done()
			return 0, ctx.Err()
		}) {
		if !errors.Is(r.Err, context.Canceled) {
			t.Errorf("want context.Canceled, got %v", r.Err)
		}
	}
	waitNoExtraGoroutines(t, base)
}

func TestMapPanicsOnNonPositiveLimit(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("want panic for n <= 0")
		}
	}()
	Map(context.Background(), seqOf(1), 0, func(_ context.Context, v int) (int, error) {
		return v, nil
	})
}

// Каждый новый обход последовательности запускает задачи заново — тот же
// контракт, что у Stream.
func TestMapRerunsOnEachRange(t *testing.T) {
	base := runtime.NumGoroutine()
	var calls atomic.Int64

	seq := Map(context.Background(), seqOf(0, 1), 2,
		func(_ context.Context, v int) (int, error) {
			calls.Add(1)
			return v, nil
		})

	for range seq {
	}
	for range seq {
	}

	if got := calls.Load(); got != 4 {
		t.Fatalf("want 4 calls over two passes, got %d", got)
	}
	waitNoExtraGoroutines(t, base)
}

// Отмена внешнего контекста обязана разрывать и чтение входа, иначе Map не
// смог бы вернуться из обхода блокирующего источника.
func TestFromChannelStopsOnCancellation(t *testing.T) {
	base := runtime.NumGoroutine()
	ctx, cancel := context.WithCancel(context.Background())

	// Канал, в который никто больше не пишет: без реакции на отмену обход
	// повис бы здесь навсегда.
	jobs := make(chan int, 1)
	jobs <- 1

	done := make(chan struct{})
	var seen int
	go func() {
		defer close(done)
		for range Map(ctx, FromChannel(ctx, jobs), 2,
			func(_ context.Context, v int) (int, error) { return v, nil }) {
			seen++
			cancel() // получили первый результат — сворачиваем всё
		}
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Map не вернулся после отмены: чтение входа не прервалось")
	}
	if seen != 1 {
		t.Errorf("want 1 result, got %d", seen)
	}
	waitNoExtraGoroutines(t, base)
}

func TestFromChannelStopsOnClose(t *testing.T) {
	base := runtime.NumGoroutine()
	jobs := make(chan int, 3)
	for i := range 3 {
		jobs <- i
	}
	close(jobs)

	var seen int
	for _, r := range Map(context.Background(), FromChannel(context.Background(), jobs), 2,
		func(_ context.Context, v int) (int, error) { return v, nil }) {
		if r.Err != nil {
			t.Fatalf("unexpected error: %v", r.Err)
		}
		seen++
	}
	if seen != 3 {
		t.Fatalf("want 3 results, got %d", seen)
	}
	waitNoExtraGoroutines(t, base)
}

// После ухода потребителя диспетчер не имеет права запустить ни одной новой
// задачи, даже если слоты свободны: выбор готовой ветви в select случаен,
// поэтому отмена проверяется до занятия слота.
func TestMapStartsNoTasksAfterBreak(t *testing.T) {
	base := runtime.NumGoroutine()

	var started atomic.Int64
	items := make([]int, 500)
	for i := range items {
		items[i] = i
	}

	for range Map(context.Background(), slices.Values(items), 2,
		func(_ context.Context, v int) (int, error) {
			started.Add(1)
			return v, nil
		}) {
		break
	}

	afterBreak := started.Load()
	time.Sleep(50 * time.Millisecond)
	if now := started.Load(); now != afterBreak {
		t.Fatalf("после break стартовали новые задачи: было %d, стало %d", afterBreak, now)
	}
	if afterBreak >= int64(len(items)) {
		t.Errorf("вход обработан целиком (%d задач) несмотря на ранний выход", afterBreak)
	}
	waitNoExtraGoroutines(t, base)
}
