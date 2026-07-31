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

// waitNoExtraGoroutines — замена goleak без внешних зависимостей: число
// горутин должно вернуться к базовому уровню, снятому в начале теста.
// (Локально для полноценной проверки подключите go.uber.org/goleak в
// TestMain.)
func waitNoExtraGoroutines(t *testing.T, base int) {
	t.Helper()
	// Даём фоновым горутинам до двух секунд на завершение: wg.Wait в Stream
	// уже отработал, но рантайму нужно время снять горутину с учёта.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= base {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	// Не дождались — печатаем стеки всех горутин, чтобы утечку было видно.
	buf := make([]byte, 1<<16)
	n := runtime.Stack(buf, true)
	t.Fatalf("goroutine leak: base=%d now=%d\n%s", base, runtime.NumGoroutine(), buf[:n])
}

// Семантика allSettled: проходим цикл до конца и получаем ровно один
// результат на каждую задачу — и успехи, и ошибки, без потерь и дублей.
func TestAllSettledCollectsEverything(t *testing.T) {
	base := runtime.NumGoroutine()
	errBroken := errors.New("broken")

	// Каждая третья задача (i % 3 == 0) возвращает ошибку, остальные —
	// значение i*10: так можно проверить оба исхода в одном прогоне.
	const n = 10
	fns := make([]func(context.Context) (int, error), n)
	for i := range n {
		fns[i] = func(ctx context.Context) (int, error) {
			if i%3 == 0 {
				return 0, fmt.Errorf("task %d: %w", i, errBroken)
			}
			return i * 10, nil
		}
	}

	// Собираем результаты в map по индексу: заодно ловим дубликаты индексов.
	got := make(map[int]Result[int], n)
	for i, r := range Stream(context.Background(), fns...) {
		if _, dup := got[i]; dup {
			t.Fatalf("duplicate index %d", i)
		}
		got[i] = r
	}

	if len(got) != n {
		t.Fatalf("want %d results, got %d", n, len(got))
	}
	// Сверяем каждый результат с ожидаемым исходом для его индекса.
	for i, r := range got {
		switch {
		case i%3 == 0 && !errors.Is(r.Err, errBroken):
			t.Errorf("task %d: want errBroken, got %v", i, r.Err)
		case i%3 != 0 && (r.Err != nil || r.Value != i*10):
			t.Errorf("task %d: want (%d, nil), got (%d, %v)", i, i*10, r.Value, r.Err)
		}
	}
	waitNoExtraGoroutines(t, base)
}

// Value едет к потребителю ровно таким, каким его вернула задача: если она
// отдала частичный результат вместе с ошибкой, Stream ничего не занулит.
// Тест закрепляет документированное поведение Result: смысл Value при ошибке
// задаёт контракт самой задачи, а рассчитывать на зануление со стороны Stream
// нельзя.
func TestPartialValueSurvivesError(t *testing.T) {
	base := runtime.NumGoroutine()
	errTruncated := errors.New("truncated")

	const partial = "прочитано наполовину"
	fns := []func(context.Context) (string, error){
		func(ctx context.Context) (string, error) { return partial, errTruncated },
		func(ctx context.Context) (string, error) { return "целиком", nil },
	}

	got := make(map[int]Result[string], len(fns))
	for i, r := range Stream(context.Background(), fns...) {
		got[i] = r
	}

	if len(got) != len(fns) {
		t.Fatalf("want %d results, got %d", len(fns), len(got))
	}
	// Главное утверждение теста: оба поля доехали одновременно.
	if r := got[0]; !errors.Is(r.Err, errTruncated) || r.Value != partial {
		t.Errorf("task 0: want (%q, errTruncated), got (%q, %v)", partial, r.Value, r.Err)
	}
	if r := got[1]; r.Err != nil || r.Value != "целиком" {
		t.Errorf("task 1: want (%q, nil), got (%q, %v)", "целиком", r.Value, r.Err)
	}
	waitNoExtraGoroutines(t, base)
}

// Единственное исключение из предыдущего теста: при панике и runtime.Goexit
// возврата задачи не существует, поэтому Result собирает сам Stream — и
// Value в нём гарантированно нулевой, что бы задача ни успела насчитать.
func TestValueIsZeroOnPanicAndGoexit(t *testing.T) {
	base := runtime.NumGoroutine()

	fns := []func(context.Context) (string, error){
		func(ctx context.Context) (string, error) { panic("boom") },
		func(ctx context.Context) (string, error) { runtime.Goexit(); return "недостижимо", nil },
	}

	count := 0
	for i, r := range Stream(context.Background(), fns...) {
		count++
		if r.Err == nil {
			t.Errorf("task %d: want error, got nil", i)
		}
		if r.Value != "" {
			t.Errorf("task %d: want zero Value, got %q", i, r.Value)
		}
	}
	if count != len(fns) {
		t.Fatalf("want %d results, got %d", len(fns), count)
	}
	waitNoExtraGoroutines(t, base)
}

// Результаты приходят в порядке завершения задач, а не в порядке аргументов.
// Порядок здесь детерминированный, без единого sleep: следующая задача
// отпирается только после того, как потребитель получил результат
// предыдущей, — цепочка happens-before вместо ставки на тайминги.
func TestCompletionOrder(t *testing.T) {
	base := runtime.NumGoroutine()

	// «Ворота» задач 0 и 2: закрытие канала разрешает задаче вернуться.
	gate0 := make(chan struct{})
	gate2 := make(chan struct{})

	fns := []func(context.Context) (int, error){
		func(ctx context.Context) (int, error) { <-gate0; return 0, nil },
		func(ctx context.Context) (int, error) { return 1, nil }, // финиширует первой
		func(ctx context.Context) (int, error) { <-gate2; return 2, nil },
	}

	var order []int
	for i := range Stream(context.Background(), fns...) {
		order = append(order, i)
		// Отпираем следующую задачу именно из тела цикла: полученный
		// результат задачи i доказывает, что её отправка уже случилась, —
		// значит, порядок [1, 2, 0] обеспечен строго, а не вероятностно.
		switch i {
		case 1:
			close(gate2)
		case 2:
			close(gate0)
		}
	}

	if want := []int{1, 2, 0}; !slices.Equal(order, want) {
		t.Fatalf("completion order = %v, want %v", order, want)
	}
	waitNoExtraGoroutines(t, base)
}

// break из range-цикла отменяет контекст оставшихся задач и дожидается их
// завершения — главная гарантия структурной конкурентности.
func TestBreakCancelsRemaining(t *testing.T) {
	base := runtime.NumGoroutine()

	const n = 8
	var sawCancel atomic.Int32

	// Одна быстрая задача и n-1 задач, которые висят до отмены контекста:
	// break после первого результата обязан разбудить их все.
	fns := make([]func(context.Context) (string, error), n)
	fns[0] = func(ctx context.Context) (string, error) { return "fast", nil }
	for i := 1; i < n; i++ {
		fns[i] = func(ctx context.Context) (string, error) {
			<-ctx.Done() // висим до отмены
			sawCancel.Add(1)
			return "", ctx.Err()
		}
	}

	for _, r := range Stream(context.Background(), fns...) {
		if r.Err == nil && r.Value == "fast" {
			break // уходим сразу после самого первого результата
		}
	}

	// Сразу после range-цикла — без sleep, без опросов — каждый оставшийся
	// воркер обязан был увидеть отмену и завершиться. Это гарантия
	// структурной конкурентности в действии.
	if got := sawCancel.Load(); got != n-1 {
		t.Fatalf("want %d cancelled tasks, got %d", n-1, got)
	}
	waitNoExtraGoroutines(t, base)
}

// Отмена родительского контекста не ломает семантику allSettled: каждая
// задача всё равно отдаёт ровно один результат (с ошибкой отмены).
func TestParentContextCancel(t *testing.T) {
	base := runtime.NumGoroutine()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Все задачи честно ждут отмены контекста; ветка с 5-секундным таймером
	// существует только как страховка от зависания теста.
	const n = 5
	fns := make([]func(context.Context) (int, error), n)
	for i := range fns {
		fns[i] = func(ctx context.Context) (int, error) {
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-time.After(5 * time.Second):
				return 0, errors.New("should not happen")
			}
		}
	}

	// Отменяем родительский контекст извне, пока задачи ещё работают.
	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()

	count := 0
	for _, r := range Stream(ctx, fns...) {
		count++
		if !errors.Is(r.Err, context.Canceled) {
			t.Errorf("want context.Canceled, got %v", r.Err)
		}
	}
	// Семантика allSettled переживает отмену: по-прежнему один результат на
	// задачу.
	if count != n {
		t.Fatalf("want %d results, got %d", n, count)
	}
	waitNoExtraGoroutines(t, base)
}

// Контекст, отменённый ещё ДО вызова Stream, тоже не ломает allSettled:
// задачи всё равно запускаются, и каждая отдаёт ровно один результат.
// Тест закрепляет контракт от «оптимизации» вида «если ctx уже отменён —
// не запускать ничего и не отдать ни одного результата».
func TestAlreadyCancelledContext(t *testing.T) {
	base := runtime.NumGoroutine()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // отменяем заранее, ещё до запуска

	const n = 5
	fns := make([]func(context.Context) (int, error), n)
	for i := range fns {
		fns[i] = func(ctx context.Context) (int, error) {
			// Дочерний контекст уже отменён — сразу выходим с его ошибкой.
			return 0, ctx.Err()
		}
	}

	count := 0
	for _, r := range Stream(ctx, fns...) {
		count++
		if !errors.Is(r.Err, context.Canceled) {
			t.Errorf("want context.Canceled, got %v", r.Err)
		}
	}
	if count != n {
		t.Fatalf("want %d results, got %d", n, count)
	}
	waitNoExtraGoroutines(t, base)
}

// Паника внутри задачи не роняет процесс: она превращается в *PanicError со
// значением паники и снимком стека, а соседние задачи не страдают.
func TestPanicIsRecovered(t *testing.T) {
	base := runtime.NumGoroutine()

	fns := []func(context.Context) (int, error){
		func(ctx context.Context) (int, error) { return 1, nil },
		func(ctx context.Context) (int, error) { panic("boom") },
	}

	var panics, oks int
	for _, r := range Stream(context.Background(), fns...) {
		var pe *PanicError
		switch {
		case errors.As(r.Err, &pe):
			panics++
			// Проверяем, что в PanicError доехало исходное значение паники
			// и непустой стек — иначе от него мало толку при отладке.
			if pe.Value != "boom" {
				t.Errorf("want panic value %q, got %v", "boom", pe.Value)
			}
			if len(pe.Stack) == 0 {
				t.Error("want non-empty stack in PanicError")
			}
		case r.Err == nil:
			oks++
		default:
			t.Errorf("unexpected error: %v", r.Err)
		}
	}
	if panics != 1 || oks != 1 {
		t.Fatalf("want 1 panic + 1 ok, got %d + %d", panics, oks)
	}
	waitNoExtraGoroutines(t, base)
}

// panic(err) внутри задачи остаётся различимым для сентинелей: благодаря
// PanicError.Unwrap цепочка errors.Is проходит сквозь PanicError до исходной
// ошибки.
func TestPanicErrorUnwrap(t *testing.T) {
	sentinel := errors.New("db down")

	fns := []func(context.Context) (int, error){
		func(ctx context.Context) (int, error) {
			panic(fmt.Errorf("query: %w", sentinel))
		},
	}

	count := 0
	for _, r := range Stream(context.Background(), fns...) {
		count++
		var pe *PanicError
		if !errors.As(r.Err, &pe) {
			t.Fatalf("want *PanicError, got %v", r.Err)
		}
		if !errors.Is(r.Err, sentinel) {
			t.Fatalf("want errors.Is to reach sentinel through PanicError, got %v", r.Err)
		}
	}
	if count != 1 {
		t.Fatalf("want 1 result, got %d", count)
	}
}

// Под GODEBUG=panicnil=1 действует старая семантика panic(nil): recover()
// возвращает nil, хотя паника была. Флаг completed в run не даёт принять
// такую панику за успех — потребитель всё равно получает *PanicError.
func TestPanicNilUnderGodebug(t *testing.T) {
	t.Setenv("GODEBUG", "panicnil=1")

	fns := []func(context.Context) (int, error){
		func(ctx context.Context) (int, error) { panic(nil) },
	}

	count := 0
	for _, r := range Stream(context.Background(), fns...) {
		count++
		var pe *PanicError
		if !errors.As(r.Err, &pe) {
			t.Fatalf("want *PanicError for panic(nil), got Err=%v Value=%v", r.Err, r.Value)
		}
	}
	if count != 1 {
		t.Fatalf("want 1 result, got %d", count)
	}
}

// nil вместо функции — это обычная паника вызова nil-функции, а не крах
// процесса: она приходит как *PanicError, соседние задачи не страдают.
// Тест закрепляет текущее поведение (побочный эффект recover в run), чтобы
// будущий рефакторинг не поменял его молча.
func TestNilTaskFn(t *testing.T) {
	base := runtime.NumGoroutine()

	fns := []func(context.Context) (int, error){
		func(ctx context.Context) (int, error) { return 7, nil },
		nil,
	}

	var oks, panics int
	for i, r := range Stream(context.Background(), fns...) {
		var pe *PanicError
		switch {
		case r.Err == nil:
			oks++
			if i != 0 || r.Value != 7 {
				t.Errorf("want (0, 7), got (%d, %d)", i, r.Value)
			}
		case errors.As(r.Err, &pe):
			panics++
			if i != 1 {
				t.Errorf("want panic from task 1, got task %d", i)
			}
		default:
			t.Errorf("unexpected error: %v", r.Err)
		}
	}
	if oks != 1 || panics != 1 {
		t.Fatalf("want 1 ok + 1 panic, got %d + %d", oks, panics)
	}
	waitNoExtraGoroutines(t, base)
}

// runtime.Goexit внутри задачи (например, t.Fatal в чужом тесте) не
// подвешивает потребителя навсегда: такая задача отдаёт Result с ErrGoexit,
// а остальные завершаются как обычно.
func TestGoexitDeliversErrGoexit(t *testing.T) {
	base := runtime.NumGoroutine()

	fns := []func(context.Context) (int, error){
		func(ctx context.Context) (int, error) { return 1, nil },
		func(ctx context.Context) (int, error) { runtime.Goexit(); return 0, nil },
	}

	var goexits, oks int
	for i, r := range Stream(context.Background(), fns...) {
		switch {
		case errors.Is(r.Err, ErrGoexit):
			goexits++
			if i != 1 {
				t.Errorf("want ErrGoexit from task 1, got task %d", i)
			}
		case r.Err == nil:
			oks++
		default:
			t.Errorf("unexpected error: %v", r.Err)
		}
	}
	if goexits != 1 || oks != 1 {
		t.Fatalf("want 1 goexit + 1 ok, got %d + %d", goexits, oks)
	}
	waitNoExtraGoroutines(t, base)
}

// Паника в теле range-цикла — самый жёсткий путь выхода: она пролетает
// сквозь yield внутрь итератора, и уборка происходит только потому, что
// cancel и wg.Wait оформлены как defer. Проверяем, что зависшая задача при
// этом отменяется, а значение паники доходит до вызывающего.
func TestConsumerPanicStillCleansUp(t *testing.T) {
	base := runtime.NumGoroutine()

	var sawCancel atomic.Int32
	fns := []func(context.Context) (string, error){
		func(ctx context.Context) (string, error) { return "fast", nil },
		func(ctx context.Context) (string, error) {
			<-ctx.Done() // висим до отмены
			sawCancel.Add(1)
			return "", ctx.Err()
		},
	}

	// Паникуем на первом же результате; recover — снаружи range-цикла.
	recovered := func() (recovered any) {
		defer func() { recovered = recover() }()
		for range Stream(context.Background(), fns...) {
			panic("boom in body")
		}
		return nil
	}()

	if recovered != "boom in body" {
		t.Fatalf("want panic %q to propagate, got %v", "boom in body", recovered)
	}
	// Уборка произошла синхронно, во время раскрутки паники через defer'ы
	// итератора: зависшая задача обязана была увидеть отмену ещё до recover.
	if got := sawCancel.Load(); got != 1 {
		t.Fatalf("want 1 cancelled task, got %d", got)
	}
	waitNoExtraGoroutines(t, base)
}

// Пустой список задач — пустой итератор: тело цикла не должно выполниться ни
// разу.
func TestZeroTasks(t *testing.T) {
	for range Stream[int](context.Background()) {
		t.Fatal("iterator over zero tasks must not yield")
	}
}
