package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/vitikevich-landau/settle"
)

// Классификация ошибок — половина смысла этого сценария, поэтому она
// закреплена тестом, а не только глазами.
func TestIsTransient(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"503 — временный сбой", &httpError{Status: http.StatusServiceUnavailable}, true},
		{"429 — просят притормозить", &httpError{Status: http.StatusTooManyRequests}, true},
		{"404 — окончательный отказ", &httpError{Status: http.StatusNotFound}, false},
		{"400 — окончательный отказ", &httpError{Status: http.StatusBadRequest}, false},
		{"обрыв соединения", errors.New("connection reset by peer"), true},
		{"отмена — мы уходим", fmt.Errorf("GET: %w", context.Canceled), false},
		// Главное: дедлайн ОТДЕЛЬНОЙ попытки — это ровно тот медленный ответ,
		// ради повтора которого Timeout внутри Retry и ставят.
		{"дедлайн попытки", fmt.Errorf("GET: %w", context.DeadlineExceeded), true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := isTransient(c.err); got != c.want {
				t.Errorf("isTransient(%v) = %v, want %v", c.err, got, c.want)
			}
		})
	}
}

// Связка «Retry снаружи, Timeout внутри» обязана повторять медленные ответы,
// а не сдаваться на первом же истёкшем дедлайне.
func TestPerAttemptTimeoutIsRetried(t *testing.T) {
	var attempts int
	policy := settle.RetryPolicy{
		Backoff:   settle.Constant(time.Millisecond, 2),
		Retryable: isTransient,
	}

	fn := settle.Retry(policy, settle.Timeout(10*time.Millisecond,
		func(ctx context.Context) (pageDoc, error) {
			attempts++
			<-ctx.Done()
			return pageDoc{}, fmt.Errorf("GET slow: %w", ctx.Err())
		}))

	if _, err := fn(context.Background()); err == nil {
		t.Fatal("want an error")
	}
	if attempts != 3 {
		t.Fatalf("want 3 attempts (медленный ответ повторяется), got %d", attempts)
	}
}

// А общий дедлайн выгрузки повторы останавливает — через паузу внутри Retry.
func TestGlobalDeadlineStopsRetries(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	var attempts int
	fn := settle.Retry(settle.RetryPolicy{
		Backoff:   settle.Constant(50*time.Millisecond, 100),
		Retryable: isTransient,
	}, func(ctx context.Context) (pageDoc, error) {
		attempts++
		<-ctx.Done()
		return pageDoc{}, fmt.Errorf("GET slow: %w", ctx.Err())
	})

	start := time.Now()
	if _, err := fn(ctx); err == nil {
		t.Fatal("want an error")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("повторы не остановились по общему дедлайну: заняло %s", elapsed)
	}
	if attempts > 3 {
		t.Errorf("слишком много попыток после общего дедлайна: %d", attempts)
	}
}
