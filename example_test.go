package settle_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
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

// Map ограничивает число одновременно работающих задач и читает вход лениво,
// поэтому подходит для списков любой длины. Индекс по-прежнему указывает на
// позицию элемента во входной последовательности.
func ExampleMap() {
	ctx := context.Background()
	ids := slices.Values([]int{1, 2, 3, 4, 5})

	// Не больше двух задач одновременно, каким бы длинным ни был вход.
	results := make([]string, 5)
	for i, r := range settle.Map(ctx, ids, 2,
		func(_ context.Context, id int) (string, error) {
			return fmt.Sprintf("item-%d", id), nil
		}) {
		results[i] = r.Value
	}

	fmt.Println(results)
	// Output:
	// [item-1 item-2 item-3 item-4 item-5]
}

// Ordered возвращает результаты в порядке входа, оставаясь последовательностью:
// он не ждёт конца обхода, а отдаёт очередной результат, как только пришли все
// предыдущие.
func ExampleOrdered() {
	ctx := context.Background()

	// Чем больше число, тем быстрее задача — значит завершаются они в порядке,
	// обратном входному.
	delays := slices.Values([]int{40, 30, 20, 10})

	for i, r := range settle.Ordered(settle.Map(ctx, delays, 4,
		func(ctx context.Context, ms int) (string, error) {
			select {
			case <-time.After(time.Duration(ms) * time.Millisecond):
				return fmt.Sprintf("%dms", ms), nil
			case <-ctx.Done():
				return "", ctx.Err()
			}
		})) {
		fmt.Printf("#%d: %s\n", i, r.Value)
	}
	// Output:
	// #0: 40ms
	// #1: 30ms
	// #2: 20ms
	// #3: 10ms
}

// Values раскладывает исходы на успехи и объединённую ошибку. В отличие от
// All, частичный успех не теряется.
func ExampleValues() {
	ctx := context.Background()
	tasks := []func(context.Context) (string, error){
		func(context.Context) (string, error) { return "first", nil },
		func(context.Context) (string, error) { return "", errors.New("broken") },
		func(context.Context) (string, error) { return "third", nil },
	}

	values, err := settle.Values(settle.Stream(ctx, tasks...))
	fmt.Println("values:", values)
	fmt.Println("error:", err)
	// Output:
	// values: [first third]
	// error: settle: task 1: broken
}

// Errors собирает только неудачи — для задач, у которых важен факт выполнения,
// а не возвращаемое значение.
func ExampleErrors() {
	ctx := context.Background()
	tasks := []func(context.Context) (string, error){
		func(context.Context) (string, error) { return "sent", nil },
		func(context.Context) (string, error) { return "", errors.New("smtp refused") },
	}

	fmt.Println(settle.Errors(settle.Stream(ctx, tasks...)))
	// Output:
	// settle: task 1: smtp refused
}

// Batch нарезает поток результатов на пачки — единицы работы для следующей
// стадии конвейера, например батч-вставки в базу.
func ExampleBatch() {
	ctx := context.Background()
	ids := slices.Values([]int{1, 2, 3, 4, 5})

	results := settle.Map(ctx, ids, 3, func(_ context.Context, id int) (int, error) {
		return id * 10, nil
	})

	for chunk := range settle.Batch(settle.Ordered(results), 2) {
		fmt.Print("пачка:")
		for _, item := range chunk {
			fmt.Printf(" #%d=%d", item.Index, item.Value)
		}
		fmt.Println()
	}
	// Output:
	// пачка: #0=10 #1=20
	// пачка: #2=30 #3=40
	// пачка: #4=50
}

// Retry повторяет неудачную задачу, не трогая движок: снаружи это всё та же
// функция, поэтому «ровно один результат на задачу» сохраняется.
func ExampleRetry() {
	ctx := context.Background()
	errTemporary := errors.New("503 service unavailable")

	attempts := 0
	flaky := func(context.Context) (string, error) {
		attempts++
		if attempts < 3 {
			return "", errTemporary
		}
		return "ok", nil
	}

	policy := settle.RetryPolicy{
		// Три повтора с растущей паузой; Jitter расталкивает одновременно
		// стартовавшие задачи, Cap не даёт паузе улететь.
		Backoff: settle.Jitter(settle.Cap(
			settle.Exponential(time.Millisecond, 2, 3), 50*time.Millisecond), 0.3),
		// Повторяем только временные сбои: на 400 повтор бессмыслен.
		Retryable: func(err error) bool { return errors.Is(err, errTemporary) },
	}

	value, err := settle.Retry(policy, flaky)(ctx)
	fmt.Printf("%s за %d попытки, ошибка: %v\n", value, attempts, err)
	// Output:
	// ok за 3 попытки, ошибка: <nil>
}

// Timeout ограничивает одну попытку. Порядок обёрток задаёт смысл: Retry
// снаружи означает отдельный дедлайн для каждой попытки.
func ExampleTimeout() {
	ctx := context.Background()

	slow := func(ctx context.Context) (string, error) {
		<-ctx.Done()
		return "", ctx.Err()
	}

	_, err := settle.Retry(
		settle.RetryPolicy{Backoff: settle.Constant(time.Millisecond, 1)},
		settle.Timeout(10*time.Millisecond, slow),
	)(ctx)

	fmt.Println(errors.Is(err, context.DeadlineExceeded))
	// Output:
	// true
}
