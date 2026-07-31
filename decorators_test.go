package settle

import (
	"context"
	"errors"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

func TestTimeoutAppliesToEachCall(t *testing.T) {
	fn := Timeout(20*time.Millisecond, func(ctx context.Context) (string, error) {
		<-ctx.Done()
		return "", ctx.Err()
	})

	start := time.Now()
	_, err := fn(context.Background())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want DeadlineExceeded, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("дедлайн не сработал: заняло %s", elapsed)
	}
}

func TestTimeoutDoesNotTouchParentContext(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()

	fn := Timeout(time.Millisecond, func(ctx context.Context) (string, error) {
		<-ctx.Done()
		return "", ctx.Err()
	})
	if _, err := fn(parent); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want DeadlineExceeded, got %v", err)
	}

	if err := parent.Err(); err != nil {
		t.Fatalf("родительский контекст пострадал: %v", err)
	}
}

// Порядок обёрток задаёт смысл: Retry снаружи — дедлайн у каждой попытки свой.
func TestTimeoutInsideRetryAppliesPerAttempt(t *testing.T) {
	var calls atomic.Int64

	fn := Retry(RetryPolicy{Backoff: Constant(time.Millisecond, 2)},
		Timeout(20*time.Millisecond, func(ctx context.Context) (string, error) {
			calls.Add(1)
			<-ctx.Done()
			return "", ctx.Err()
		}))

	if _, err := fn(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want DeadlineExceeded, got %v", err)
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("want 3 attempts, each with its own deadline, got %d", got)
	}
}

func TestObserveReportsDurationAndError(t *testing.T) {
	errBroken := errors.New("broken")
	var starts int
	var gotErr error
	var gotElapsed time.Duration

	fn := Observe(Hooks{
		Start: func() { starts++ },
		Done: func(elapsed time.Duration, err error) {
			gotElapsed, gotErr = elapsed, err
		},
	}, func(context.Context) (string, error) {
		time.Sleep(5 * time.Millisecond)
		return "", errBroken
	})

	if _, err := fn(context.Background()); !errors.Is(err, errBroken) {
		t.Fatalf("Observe подменил ошибку: %v", err)
	}
	if starts != 1 {
		t.Errorf("want a single Start, got %d", starts)
	}
	if !errors.Is(gotErr, errBroken) {
		t.Errorf("want the task error in Done, got %v", gotErr)
	}
	if gotElapsed < 5*time.Millisecond {
		t.Errorf("want elapsed >= 5ms, got %s", gotElapsed)
	}
}

func TestObservePassesValueThrough(t *testing.T) {
	fn := Observe(Hooks{}, func(context.Context) (string, error) {
		return "value", nil
	})
	v, err := fn(context.Background())
	if v != "value" || err != nil {
		t.Fatalf("want (value, nil), got (%q, %v)", v, err)
	}
}

// При панике значения ошибки не существует, поэтому Done не вызывается —
// иначе метрика записала бы успех. Расхождение Start и Done и есть сигнал.
func TestObserveSkipsDoneOnPanic(t *testing.T) {
	base := runtime.NumGoroutine()
	var starts, dones atomic.Int64

	fn := Observe(Hooks{
		Start: func() { starts.Add(1) },
		Done:  func(time.Duration, error) { dones.Add(1) },
	}, func(context.Context) (string, error) {
		panic("boom")
	})

	results := AllSettled(context.Background(), fn)
	var pe *PanicError
	if !errors.As(results[0].Err, &pe) {
		t.Fatalf("want PanicError, got %v", results[0].Err)
	}
	if starts.Load() != 1 {
		t.Errorf("want a single Start, got %d", starts.Load())
	}
	if dones.Load() != 0 {
		t.Errorf("want no Done on panic, got %d", dones.Load())
	}
	waitNoExtraGoroutines(t, base)
}

func TestObserveWithNilHooks(t *testing.T) {
	fn := Observe(Hooks{}, func(context.Context) (int, error) { return 42, nil })
	if v, err := fn(context.Background()); v != 42 || err != nil {
		t.Fatalf("want (42, nil), got (%d, %v)", v, err)
	}
}

// Декораторы композируются в любом сочетании и не мешают движку.
func TestDecoratorsComposeInsideMap(t *testing.T) {
	base := runtime.NumGoroutine()
	var observed atomic.Int64
	errTemporary := errors.New("temporary")
	attempts := make([]atomic.Int64, 3)

	seq := Map(context.Background(), seqOf(0, 1, 2), 2,
		func(ctx context.Context, v int) (int, error) {
			task := Observe(Hooks{Done: func(time.Duration, error) { observed.Add(1) }},
				Timeout(time.Second, func(context.Context) (int, error) {
					if attempts[v].Add(1) < 2 {
						return 0, errTemporary
					}
					return v * 10, nil
				}))
			return Retry(RetryPolicy{Backoff: Constant(time.Millisecond, 3)}, task)(ctx)
		})

	values, err := Values(Ordered(seq))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if want := []int{0, 10, 20}; len(values) != 3 || values[0] != want[0] || values[1] != want[1] || values[2] != want[2] {
		t.Fatalf("want %v, got %v", want, values)
	}
	// По две попытки на каждую из трёх задач — и все шесть наблюдались.
	if got := observed.Load(); got != 6 {
		t.Errorf("want 6 observed attempts, got %d", got)
	}
	waitNoExtraGoroutines(t, base)
}
