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
// и возвращает ошибку отмены. На ней видно, что комбинаторы действительно
// отменяют проигравших и дожидаются их завершения.
func hangTask() func(context.Context) (string, error) {
	return func(ctx context.Context) (string, error) {
		<-ctx.Done()
		return "", ctx.Err()
	}
}

// AllSettled дожидается всех задач и возвращает каждый исход в порядке
// исходного списка.
func ExampleAllSettled() {
	ctx := context.Background()
	tasks := []func(context.Context) (string, error){
		sleepTask(60*time.Millisecond, "first", nil),
		sleepTask(10*time.Millisecond, "", errors.New("broken")),
		sleepTask(30*time.Millisecond, "third", nil),
	}

	for i, r := range settle.AllSettled(ctx, tasks...) {
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

// All возвращает значения, только если успешны все задачи. Первая ошибка
// отменяет остальных; при ошибке частичного среза нет.
func ExampleAll() {
	ctx := context.Background()
	tasks := []func(context.Context) (string, error){
		func(ctx context.Context) (string, error) { return "fine", nil },
		hangTask(), // работала бы вечно — но All её отменит
		func(ctx context.Context) (string, error) { return "", errors.New("broken") },
	}

	values, err := settle.All(ctx, tasks...)
	fmt.Println("values:", values)
	fmt.Println("error:", err)
	// Output:
	// values: []
	// error: settle: task 2: broken
}

// Race возвращает первый завершившийся исход — успех или ошибку — и отменяет
// проигравших.
func ExampleRace() {
	ctx := context.Background()
	tasks := []func(context.Context) (string, error){
		hangTask(),
		func(ctx context.Context) (string, error) { return "quick", nil },
	}

	value, err := settle.Race(ctx, tasks...)
	fmt.Println("winner:", value, err)
	// Output:
	// winner: quick <nil>
}

// Any пропускает ошибки и возвращает первый успех. Если успехов нет, в ошибке
// лежит errors.Join всех неудач.
func ExampleAny() {
	ctx := context.Background()
	tasks := []func(context.Context) (string, error){
		func(ctx context.Context) (string, error) { return "", errors.New("fast but broken") },
		func(ctx context.Context) (string, error) { return "alive", nil },
		hangTask(),
	}

	value, err := settle.Any(ctx, tasks...)
	fmt.Println("first success:", value, err)
	// Output:
	// first success: alive <nil>
}

// Stream нужен для своей политики: здесь ошибка необязательной задачи
// допустима, ошибка обязательной останавливает обработку, а индекс связывает
// результат с этой политикой.
func ExampleStream() {
	ctx := context.Background()
	afterOptional := make(chan struct{})
	afterProfile := make(chan struct{})
	names := []string{"profile", "recommendations", "orders", "audit"}
	required := []bool{true, false, true, false}
	tasks := []func(context.Context) (string, error){
		func(context.Context) (string, error) {
			<-afterOptional
			return "Ivan", nil
		},
		func(context.Context) (string, error) {
			return "", errors.New("unavailable")
		},
		func(context.Context) (string, error) {
			<-afterProfile
			return "", errors.New("forbidden")
		},
		hangTask(),
	}

loop:
	for i, result := range settle.Stream(ctx, tasks...) {
		switch {
		case result.Err == nil:
			fmt.Printf("%s: %s\n", names[i], result.Value)
		case required[i]:
			fmt.Printf("%s: fatal: %v\n", names[i], result.Err)
			break loop
		default:
			fmt.Printf("%s: skipped: %v\n", names[i], result.Err)
		}

		// Каналы делают порядок примера детерминированным без time.Sleep:
		// после необязательной ошибки разрешаем профиль, затем заказы.
		switch i {
		case 1:
			close(afterOptional)
		case 0:
			close(afterProfile)
		}
	}
	// Output:
	// recommendations: skipped: unavailable
	// profile: Ivan
	// orders: fatal: forbidden
}
