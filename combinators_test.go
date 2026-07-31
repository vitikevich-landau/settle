package settle

import (
	"context"
	"errors"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

func TestAllSettledPreservesInputOrderAndPartialValue(t *testing.T) {
	base := runtime.NumGoroutine()
	errPartial := errors.New("partial")
	releaseSecond := make(chan struct{})
	secondDone := make(chan struct{})

	tasks := []func(context.Context) (string, error){
		func(context.Context) (string, error) {
			<-secondDone
			return "first", nil
		},
		func(context.Context) (string, error) {
			<-releaseSecond
			close(secondDone)
			return "second", errPartial
		},
	}

	close(releaseSecond)
	got := AllSettled(context.Background(), tasks...)
	want := []Result[string]{
		{Value: "first"},
		{Value: "second", Err: errPartial},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("want %#v, got %#v", want, got)
	}
	waitNoExtraGoroutines(t, base)
}

func TestAllSettledWithNoTasks(t *testing.T) {
	got := AllSettled[string](context.Background())
	if got == nil || len(got) != 0 {
		t.Fatalf("want non-nil empty slice, got %#v", got)
	}
}

func TestAllPreservesInputOrder(t *testing.T) {
	base := runtime.NumGoroutine()
	release := make(chan struct{})
	secondDone := make(chan struct{})

	tasks := []func(context.Context) (string, error){
		func(context.Context) (string, error) {
			<-secondDone
			return "first", nil
		},
		func(context.Context) (string, error) {
			<-release
			close(secondDone)
			return "second", nil
		},
	}

	close(release)
	got, err := All(context.Background(), tasks...)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if want := []string{"first", "second"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("want %q, got %q", want, got)
	}
	waitNoExtraGoroutines(t, base)
}

func TestAllCancelsRemainingAndReturnsNoPartialValues(t *testing.T) {
	base := runtime.NumGoroutine()
	errRejected := errors.New("rejected")
	started := make(chan struct{})
	cancelled := make(chan struct{})

	tasks := []func(context.Context) (string, error){
		func(ctx context.Context) (string, error) {
			close(started)
			<-ctx.Done()
			close(cancelled)
			return "", ctx.Err()
		},
		func(context.Context) (string, error) {
			<-started
			return "partial", errRejected
		},
	}

	got, err := All(context.Background(), tasks...)
	if got != nil {
		t.Fatalf("want nil values, got %q", got)
	}
	if !errors.Is(err, errRejected) {
		t.Fatalf("want wrapped rejection, got %v", err)
	}
	if !strings.Contains(err.Error(), "task 1") {
		t.Fatalf("want task index in error, got %q", err)
	}
	select {
	case <-cancelled:
	default:
		t.Fatal("All returned before the remaining task observed cancellation")
	}
	waitNoExtraGoroutines(t, base)
}

func TestAllWithNoTasks(t *testing.T) {
	got, err := All[string](context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil || len(got) != 0 {
		t.Fatalf("want non-nil empty slice, got %#v", got)
	}
}

func TestAllPreservesPanicError(t *testing.T) {
	base := runtime.NumGoroutine()
	_, err := All(context.Background(), func(context.Context) (string, error) {
		panic("boom")
	})

	var panicErr *PanicError
	if !errors.As(err, &panicErr) || panicErr.Value != "boom" {
		t.Fatalf("want wrapped PanicError(boom), got %v", err)
	}
	waitNoExtraGoroutines(t, base)
}

func TestRaceReturnsFirstSettlementAndCancelsRemaining(t *testing.T) {
	base := runtime.NumGoroutine()
	errFast := errors.New("fast failure")
	started := make(chan struct{})
	cancelled := make(chan struct{})

	tasks := []func(context.Context) (string, error){
		func(ctx context.Context) (string, error) {
			close(started)
			<-ctx.Done()
			close(cancelled)
			return "", ctx.Err()
		},
		func(context.Context) (string, error) {
			<-started
			return "partial", errFast
		},
	}

	value, err := Race(context.Background(), tasks...)
	if value != "partial" || !errors.Is(err, errFast) {
		t.Fatalf("want (%q, errFast), got (%q, %v)", "partial", value, err)
	}
	select {
	case <-cancelled:
	default:
		t.Fatal("Race returned before the losing task observed cancellation")
	}
	waitNoExtraGoroutines(t, base)
}

func TestRaceWithNoTasks(t *testing.T) {
	value, err := Race[string](context.Background())
	if value != "" || !errors.Is(err, ErrNoTasks) {
		t.Fatalf("want zero value and ErrNoTasks, got (%q, %v)", value, err)
	}
}

func TestAnyReturnsFirstSuccessAndCancelsRemaining(t *testing.T) {
	base := runtime.NumGoroutine()
	errFast := errors.New("fast failure")
	started := make(chan struct{})
	failed := make(chan struct{})
	cancelled := make(chan struct{})

	tasks := []func(context.Context) (string, error){
		func(context.Context) (string, error) {
			<-started
			close(failed)
			return "", errFast
		},
		func(context.Context) (string, error) {
			<-failed
			return "winner", nil
		},
		func(ctx context.Context) (string, error) {
			close(started)
			<-ctx.Done()
			close(cancelled)
			return "", ctx.Err()
		},
	}

	value, err := Any(context.Background(), tasks...)
	if err != nil || value != "winner" {
		t.Fatalf("want (%q, nil), got (%q, %v)", "winner", value, err)
	}
	select {
	case <-cancelled:
	default:
		t.Fatal("Any returned before the remaining task observed cancellation")
	}
	waitNoExtraGoroutines(t, base)
}

func TestAnyJoinsAllErrorsInInputOrder(t *testing.T) {
	base := runtime.NumGoroutine()
	errs := []error{
		errors.New("zero"),
		errors.New("one"),
		errors.New("two"),
	}
	secondDone := make(chan struct{})
	thirdDone := make(chan struct{})

	tasks := []func(context.Context) (string, error){
		func(context.Context) (string, error) {
			<-secondDone
			return "", errs[0]
		},
		func(context.Context) (string, error) {
			<-thirdDone
			close(secondDone)
			return "", errs[1]
		},
		func(context.Context) (string, error) {
			close(thirdDone)
			return "", errs[2]
		},
	}

	value, err := Any(context.Background(), tasks...)
	if value != "" {
		t.Fatalf("want zero value, got %q", value)
	}
	for _, want := range errs {
		if !errors.Is(err, want) {
			t.Errorf("joined error does not contain %v: %v", want, err)
		}
	}
	lines := strings.Split(err.Error(), "\n")
	wantLines := []string{
		"settle: task 0: zero",
		"settle: task 1: one",
		"settle: task 2: two",
	}
	if !reflect.DeepEqual(lines, wantLines) {
		t.Fatalf("want errors in input order %q, got %q", wantLines, lines)
	}
	joined, ok := err.(interface{ Unwrap() []error })
	if !ok {
		t.Fatalf("want joined error with Unwrap() []error, got %T", err)
	}
	parts := joined.Unwrap()
	if len(parts) != len(errs) {
		t.Fatalf("want %d joined errors, got %d", len(errs), len(parts))
	}
	for i, part := range parts {
		if !errors.Is(part, errs[i]) {
			t.Errorf("joined part %d does not wrap %v: %v", i, errs[i], part)
		}
	}
	waitNoExtraGoroutines(t, base)
}

func TestAnyWithNoTasks(t *testing.T) {
	value, err := Any[string](context.Background())
	if value != "" || !errors.Is(err, ErrNoTasks) {
		t.Fatalf("want zero value and ErrNoTasks, got (%q, %v)", value, err)
	}
}

// waitForCtx — задача, честно уважающая контекст: висит до отмены и возвращает
// её причину. На уже отменённом контексте завершается сразу.
func waitForCtx(ctx context.Context) (string, error) {
	<-ctx.Done()
	return "", ctx.Err()
}

func TestAllSettledWithCancelledContext(t *testing.T) {
	base := runtime.NumGoroutine()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	got := AllSettled(ctx, waitForCtx, waitForCtx)
	if len(got) != 2 {
		t.Fatalf("want 2 results, got %#v", got)
	}
	for i, r := range got {
		if !errors.Is(r.Err, context.Canceled) {
			t.Errorf("result %d: want context.Canceled, got %v", i, r.Err)
		}
	}
	waitNoExtraGoroutines(t, base)
}

func TestAllWithCancelledContext(t *testing.T) {
	base := runtime.NumGoroutine()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	values, err := All(ctx, waitForCtx, waitForCtx)
	if values != nil {
		t.Fatalf("want nil values, got %q", values)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want wrapped context.Canceled, got %v", err)
	}
	waitNoExtraGoroutines(t, base)
}

func TestRaceWithCancelledContext(t *testing.T) {
	base := runtime.NumGoroutine()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	value, err := Race(ctx, waitForCtx)
	if value != "" || !errors.Is(err, context.Canceled) {
		t.Fatalf("want zero value and context.Canceled, got (%q, %v)", value, err)
	}
	waitNoExtraGoroutines(t, base)
}

func TestAnyWithCancelledContext(t *testing.T) {
	base := runtime.NumGoroutine()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	value, err := Any(ctx, waitForCtx, waitForCtx)
	if value != "" {
		t.Fatalf("want zero value, got %q", value)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want joined context.Canceled, got %v", err)
	}
	joined, ok := err.(interface{ Unwrap() []error })
	if !ok || len(joined.Unwrap()) != 2 {
		t.Fatalf("want 2 joined errors, got %v", err)
	}
	waitNoExtraGoroutines(t, base)
}

func TestAnyPreservesPanicError(t *testing.T) {
	base := runtime.NumGoroutine()
	errPlain := errors.New("plain failure")

	_, err := Any(context.Background(),
		func(context.Context) (string, error) { panic("boom") },
		func(context.Context) (string, error) { return "", errPlain },
	)

	var panicErr *PanicError
	if !errors.As(err, &panicErr) || panicErr.Value != "boom" {
		t.Fatalf("want wrapped PanicError(boom), got %v", err)
	}
	if !errors.Is(err, errPlain) {
		t.Fatalf("joined error does not contain plain failure: %v", err)
	}
	waitNoExtraGoroutines(t, base)
}
