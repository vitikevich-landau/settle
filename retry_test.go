package settle

import (
	"context"
	"errors"
	"math"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestRetrySucceedsAfterTransientFailures(t *testing.T) {
	errTemporary := errors.New("temporary")
	var calls int

	fn := Retry(RetryPolicy{Backoff: Constant(time.Millisecond, 5)},
		func(context.Context) (string, error) {
			calls++
			if calls < 3 {
				return "", errTemporary
			}
			return "ok", nil
		})

	value, err := fn(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if value != "ok" {
		t.Errorf("want %q, got %q", "ok", value)
	}
	if calls != 3 {
		t.Errorf("want 3 calls, got %d", calls)
	}
}

func TestRetryStopsOnNonRetryableError(t *testing.T) {
	errFatal := errors.New("400 bad request")
	var calls int

	fn := Retry(RetryPolicy{
		Backoff:   Constant(time.Hour, 5), // сработай хоть одна пауза — тест не уложится
		Retryable: func(err error) bool { return !errors.Is(err, errFatal) },
	}, func(context.Context) (string, error) {
		calls++
		return "", errFatal
	})

	_, err := fn(context.Background())
	if !errors.Is(err, errFatal) {
		t.Fatalf("want the original error, got %v", err)
	}
	// Неповторяемая ошибка возвращается как есть — без обёртки счётчиком.
	if strings.Contains(err.Error(), "attempts") {
		t.Errorf("want unwrapped error, got %q", err)
	}
	if calls != 1 {
		t.Errorf("want a single call, got %d", calls)
	}
}

func TestRetryExhaustsAttempts(t *testing.T) {
	errBroken := errors.New("broken")
	var calls int

	fn := Retry(RetryPolicy{Backoff: Constant(time.Millisecond, 2)},
		func(context.Context) (string, error) {
			calls++
			return "", errBroken
		})

	_, err := fn(context.Background())
	if !errors.Is(err, errBroken) {
		t.Fatalf("want the last error wrapped, got %v", err)
	}
	// Две паузы — три вызова.
	if calls != 3 {
		t.Errorf("want 3 calls, got %d", calls)
	}
	if !strings.Contains(err.Error(), "3 attempts") {
		t.Errorf("want the attempt count in the error, got %q", err)
	}
}

func TestRetryWithoutBackoffMakesSingleAttempt(t *testing.T) {
	errBroken := errors.New("broken")
	var calls int

	fn := Retry(RetryPolicy{}, func(context.Context) (string, error) {
		calls++
		return "", errBroken
	})

	_, err := fn(context.Background())
	if !errors.Is(err, errBroken) {
		t.Fatalf("want broken, got %v", err)
	}
	if calls != 1 {
		t.Errorf("want a single call, got %d", calls)
	}
}

func TestRetryAbortsPauseOnCancellation(t *testing.T) {
	errTemporary := errors.New("temporary")
	ctx, cancel := context.WithCancel(context.Background())

	fn := Retry(RetryPolicy{Backoff: Constant(10*time.Second, 5)},
		func(context.Context) (string, error) {
			cancel() // отменяем ровно перед уходом в долгую паузу
			return "", errTemporary
		})

	start := time.Now()
	_, err := fn(ctx)
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("пауза не прервалась отменой: заняло %s", elapsed)
	}
	// Обе причины различимы: и предметная ошибка, и отмена.
	if !errors.Is(err, errTemporary) {
		t.Errorf("want the task error preserved, got %v", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("want context.Canceled preserved, got %v", err)
	}
}

// Паника — дефект кода, а не временный сбой: повторять её нельзя, она должна
// долететь до движка и стать *PanicError.
func TestRetryDoesNotSwallowPanic(t *testing.T) {
	base := runtime.NumGoroutine()
	var calls int

	fn := Retry(RetryPolicy{Backoff: Constant(time.Millisecond, 5)},
		func(context.Context) (string, error) {
			calls++
			panic("boom")
		})

	results := AllSettled(context.Background(), fn)
	var pe *PanicError
	if !errors.As(results[0].Err, &pe) || pe.Value != "boom" {
		t.Fatalf("want PanicError(boom), got %v", results[0].Err)
	}
	if calls != 1 {
		t.Errorf("want a single call, got %d", calls)
	}
	waitNoExtraGoroutines(t, base)
}

func TestExponentialGrowsGeometrically(t *testing.T) {
	got := slices.Collect(Exponential(10*time.Millisecond, 2, 4))
	want := []time.Duration{
		10 * time.Millisecond,
		20 * time.Millisecond,
		40 * time.Millisecond,
		80 * time.Millisecond,
	}
	if !slices.Equal(got, want) {
		t.Fatalf("want %v, got %v", want, got)
	}
}

func TestConstantRepeatsTheSameDelay(t *testing.T) {
	got := slices.Collect(Constant(5*time.Millisecond, 3))
	want := []time.Duration{5 * time.Millisecond, 5 * time.Millisecond, 5 * time.Millisecond}
	if !slices.Equal(got, want) {
		t.Fatalf("want %v, got %v", want, got)
	}
}

func TestCapLimitsDelays(t *testing.T) {
	got := slices.Collect(Cap(Exponential(10*time.Millisecond, 4, 4), 100*time.Millisecond))
	want := []time.Duration{
		10 * time.Millisecond,
		40 * time.Millisecond,
		100 * time.Millisecond,
		100 * time.Millisecond,
	}
	if !slices.Equal(got, want) {
		t.Fatalf("want %v, got %v", want, got)
	}
}

func TestJitterStaysWithinRange(t *testing.T) {
	const base = 100 * time.Millisecond
	for d := range Jitter(Constant(base, 200), 0.5) {
		if d < base/2 || d > base*3/2 {
			t.Fatalf("delay %s out of [50ms, 150ms]", d)
		}
	}

	// frac <= 0 оставляет последовательность нетронутой.
	got := slices.Collect(Jitter(Constant(base, 2), 0))
	if !slices.Equal(got, []time.Duration{base, base}) {
		t.Fatalf("want the sequence untouched, got %v", got)
	}
}

// Ретраи живут внутри задачи, поэтому движок по-прежнему видит ровно один
// результат на элемент, а индексы не съезжают.
func TestRetryInsideMapKeepsOneResultPerTask(t *testing.T) {
	base := runtime.NumGoroutine()
	errTemporary := errors.New("temporary")
	attempts := make([]int, 4)

	policy := RetryPolicy{Backoff: Constant(time.Millisecond, 3)}
	seq := Map(context.Background(), seqOf(0, 1, 2, 3), 2,
		func(ctx context.Context, v int) (int, error) {
			return Retry(policy, func(context.Context) (int, error) {
				attempts[v]++
				// Нечётные задачи со второго раза начинают отвечать.
				if v%2 == 1 && attempts[v] < 2 {
					return 0, errTemporary
				}
				return v, nil
			})(ctx)
		})

	seen := make(map[int]int)
	for i, r := range seq {
		if r.Err != nil {
			t.Fatalf("task %d: %v", i, r.Err)
		}
		if _, dup := seen[i]; dup {
			t.Fatalf("task %d returned twice", i)
		}
		seen[i] = r.Value
	}

	if len(seen) != 4 {
		t.Fatalf("want 4 results, got %v", seen)
	}
	for i, v := range seen {
		if i != v {
			t.Errorf("index %d carries value %d", i, v)
		}
	}
	waitNoExtraGoroutines(t, base)
}

// Пакет не зануляет Value при ошибке, и Retry не имеет права это менять:
// частичный результат задачи должен доезжать до потребителя во всех исходах.
func TestRetryPreservesPartialValue(t *testing.T) {
	errFatal := errors.New("fatal")
	errTemporary := errors.New("temporary")

	t.Run("неповторяемая ошибка", func(t *testing.T) {
		fn := Retry(RetryPolicy{
			Backoff:   Constant(time.Millisecond, 3),
			Retryable: func(err error) bool { return errors.Is(err, errTemporary) },
		}, func(context.Context) (string, error) {
			return "частичный результат", errFatal
		})

		v, err := fn(context.Background())
		if !errors.Is(err, errFatal) {
			t.Fatalf("want fatal, got %v", err)
		}
		if v != "частичный результат" {
			t.Errorf("значение потеряно: got %q", v)
		}
	})

	t.Run("повторы исчерпаны", func(t *testing.T) {
		fn := Retry(RetryPolicy{Backoff: Constant(time.Millisecond, 2)},
			func(context.Context) (string, error) {
				return "частичный результат", errTemporary
			})

		v, err := fn(context.Background())
		if !errors.Is(err, errTemporary) {
			t.Fatalf("want temporary, got %v", err)
		}
		if v != "частичный результат" {
			t.Errorf("значение последней попытки потеряно: got %q", v)
		}
	})

	t.Run("отмена во время паузы", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		fn := Retry(RetryPolicy{Backoff: Constant(10*time.Second, 3)},
			func(context.Context) (string, error) {
				cancel()
				return "частичный результат", errTemporary
			})

		v, err := fn(ctx)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("want cancellation, got %v", err)
		}
		if v != "частичный результат" {
			t.Errorf("значение потеряно при отмене: got %q", v)
		}
	})
}

// Одну политику разделяют все задачи пачки, поэтому её последовательность
// задержек обходится конкурентно. Нормализация frac внутри замыкания была бы
// записью в общую переменную — гонкой, которую ловит -race.
func TestJitterPolicyIsSafeForConcurrentUse(t *testing.T) {
	base := runtime.NumGoroutine()
	errTemporary := errors.New("temporary")

	// frac > 1 — именно тот случай, который требует нормализации.
	policy := RetryPolicy{Backoff: Jitter(Constant(time.Millisecond, 3), 5)}

	items := make([]int, 32)
	for i := range items {
		items[i] = i
	}

	for _, r := range Map(context.Background(), slices.Values(items), 8,
		func(ctx context.Context, v int) (int, error) {
			return Retry(policy, func(context.Context) (int, error) {
				return 0, errTemporary
			})(ctx)
		}) {
		if !errors.Is(r.Err, errTemporary) {
			t.Fatalf("want temporary, got %v", r.Err)
		}
	}
	waitNoExtraGoroutines(t, base)
}

// frac > 1 обрезается до 1, иначе задержка могла бы стать отрицательной.
func TestJitterClampsFraction(t *testing.T) {
	const base = 100 * time.Millisecond
	for d := range Jitter(Constant(base, 500), 42) {
		if d < 0 || d > 2*base {
			t.Fatalf("delay %s out of [0, 200ms]", d)
		}
	}
}

// Задержки у верхней границы диапазона обязаны насыщаться, а не переполняться.
// Отрицательная длительность для sleepCtx означает «не спать вовсе», то есть
// очень долгая пауза обернулась бы мгновенным повтором.
func TestBackoffSaturatesInsteadOfOverflowing(t *testing.T) {
	const maxDuration = time.Duration(math.MaxInt64)

	t.Run("Jitter поверх предельной задержки", func(t *testing.T) {
		var seen int
		for d := range Jitter(Constant(maxDuration, 200), 0.5) {
			if d <= 0 {
				t.Fatalf("переполнение: задержка %d", d)
			}
			seen++
		}
		if seen != 200 {
			t.Fatalf("want 200 delays, got %d", seen)
		}
	})

	t.Run("Exponential упирается в потолок", func(t *testing.T) {
		// Растёт быстро и заведомо выходит за int64 — и обязан остановиться
		// на максимуме, а не уйти в минус.
		var last time.Duration
		for d := range Exponential(maxDuration/4, 8, 10) {
			if d <= 0 {
				t.Fatalf("переполнение: задержка %d", d)
			}
			last = d
		}
		if last != maxDuration {
			t.Errorf("want the sequence to saturate at %d, got %d", maxDuration, last)
		}
	})

	t.Run("предельное значение переживает конвертацию", func(t *testing.T) {
		// float64(math.MaxInt64) округляется вверх до 2^63 — на единицу
		// больше, чем помещается в int64.
		if got := toDuration(float64(math.MaxInt64)); got != maxDuration {
			t.Errorf("want %d, got %d", maxDuration, got)
		}
		if got := toDuration(math.Inf(1)); got != maxDuration {
			t.Errorf("want %d for +Inf, got %d", maxDuration, got)
		}
		if got := toDuration(-1); got != 0 {
			t.Errorf("want 0 for a negative input, got %d", got)
		}
	})
}
