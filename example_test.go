package settle_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/vitikevich-landau/settle"
)

// sleepTask собирает задачу, уважающую контекст: она «работает» в течение d,
// затем возвращает v/err. Если контекст отменили раньше — сразу выходит с
// ошибкой отмены, не досыпая.
func sleepTask(d time.Duration, v string, err error) func(context.Context) (string, error) {
	return func(ctx context.Context) (string, error) {
		select {
		case <-time.After(d):
			return v, err
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
}

// hangTask собирает задачу-«вечного работника»: она висит до отмены контекста
// и возвращает ошибку отмены. На таких задачах видно главное свойство Stream:
// break из цикла действительно отменяет проигравших — и заодно вывод примеров
// не зависит от таймингов.
func hangTask() func(context.Context) (string, error) {
	return func(ctx context.Context) (string, error) {
		<-ctx.Done()
		return "", ctx.Err()
	}
}

// Promise.allSettled: дождаться всех, собрать всё.
func ExampleStream_allSettled() {
	ctx := context.Background()
	tasks := []func(context.Context) (string, error){
		sleepTask(60*time.Millisecond, "first", nil),
		sleepTask(10*time.Millisecond, "", errors.New("broken")),
		sleepTask(30*time.Millisecond, "third", nil),
	}

	// Результаты приходят в порядке завершения; индекс расставляет их по
	// местам — в срез размером с исходный список задач.
	results := make([]settle.Result[string], len(tasks))
	for i, r := range settle.Stream(ctx, tasks...) {
		results[i] = r
	}

	for i, r := range results {
		if r.Err != nil {
			fmt.Printf("#%d failed: %v\n", i, r.Err)
		} else {
			fmt.Printf("#%d: %s\n", i, r.Value)
		}
	}
	// Output:
	// #0: first
	// #1 failed: broken
	// #2: third
}

// Promise.all: первая ошибка прерывает цикл, а break отменяет остальных.
func ExampleStream_all() {
	ctx := context.Background()
	tasks := []func(context.Context) (string, error){
		func(ctx context.Context) (string, error) { return "fine", nil },
		hangTask(), // работала бы вечно — но break её отменит
		func(ctx context.Context) (string, error) { return "", errors.New("broken") },
	}

	out := make([]string, len(tasks))
	var firstErr error
	for i, r := range settle.Stream(ctx, tasks...) {
		if r.Err != nil {
			firstErr = r.Err
			break // отменяет зависшую задачу
		}
		out[i] = r.Value
	}

	fmt.Println("error:", firstErr)
	// Output:
	// error: broken
}

// Promise.race: взять первую завершившуюся задачу, остальных отменить.
func ExampleStream_race() {
	ctx := context.Background()
	tasks := []func(context.Context) (string, error){
		hangTask(), // проигравший: сам не завершится, его отменит break
		func(ctx context.Context) (string, error) { return "quick", nil },
	}

	for _, r := range settle.Stream(ctx, tasks...) {
		fmt.Println("winner:", r.Value)
		break
	}
	// Output:
	// winner: quick
}

// Promise.any: побеждает первый успех, ошибки пропускаются.
func ExampleStream_any() {
	ctx := context.Background()
	tasks := []func(context.Context) (string, error){
		func(ctx context.Context) (string, error) { return "", errors.New("fast but broken") },
		func(ctx context.Context) (string, error) { return "alive", nil },
		hangTask(), // так и не финиширует сам — будет отменён при break
	}

	for _, r := range settle.Stream(ctx, tasks...) {
		if r.Err == nil {
			fmt.Println("first success:", r.Value)
			break
		}
	}
	// Output:
	// first success: alive
}
