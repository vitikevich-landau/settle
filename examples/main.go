// Демонстрационная программа библиотеки settle.
//
// Запуск из корня репозитория:
//
//	go run ./examples
//
// Каждая demo-функция ниже — отдельный сценарий использования, от простого к
// сложному: allSettled, all, race, any, таймаут через контекст и перехват
// паник. Читать лучше сверху вниз — сценарии ссылаются на идеи предыдущих.
package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/vitikevich-landau/settle"
)

func main() {
	demoAllSettled()
	demoAll()
	demoRace()
	demoAny()
	demoTimeout()
	demoPanic()
}

// section печатает заголовок сценария, чтобы вывод программы читался как
// оглавление этой демки.
func section(title string) {
	fmt.Printf("\n=== %s ===\n", title)
}

// job имитирует полезную работу (например, HTTP-запрос): «трудится» d, затем
// возвращает value/err. Задача уважает контекст: если её отменили раньше,
// она не досыпает, а сразу возвращает ошибку отмены. Именно так стоит писать
// реальные задачи для settle — иначе break не сможет быстро их остановить.
func job(name string, d time.Duration, value string, err error) func(context.Context) (string, error) {
	return func(ctx context.Context) (string, error) {
		select {
		case <-time.After(d): // поработали d — отдаём результат
			return value, err
		case <-ctx.Done(): // нас отменили — сворачиваемся немедленно
			fmt.Printf("  [%s] отменена: %v\n", name, ctx.Err())
			return "", ctx.Err()
		}
	}
}

// Сценарий 1. Promise.allSettled: дождаться ВСЕХ задач и собрать все исходы —
// и успехи, и ошибки. Самый частый способ использования settle.
func demoAllSettled() {
	section("allSettled: собрать все результаты")

	tasks := []func(context.Context) (string, error){
		job("медленный", 60*time.Millisecond, "данные из БД", nil),
		job("сломанный", 10*time.Millisecond, "", errors.New("сервис недоступен")),
		job("средний", 30*time.Millisecond, "данные из кеша", nil),
	}

	// Результаты приходят в порядке ЗАВЕРШЕНИЯ (сломанный, средний,
	// медленный), а не в порядке объявления. Индекс i говорит, чья это
	// работа, — кладём результат в срез на его законное место.
	results := make([]settle.Result[string], len(tasks))
	for i, r := range settle.Stream(context.Background(), tasks...) {
		fmt.Printf("  завершилась задача #%d\n", i)
		results[i] = r
	}

	// Теперь results упорядочен как исходный список задач.
	for i, r := range results {
		if r.Err != nil {
			fmt.Printf("  #%d: ошибка: %v\n", i, r.Err)
		} else {
			fmt.Printf("  #%d: %s\n", i, r.Value)
		}
	}
}

// Сценарий 2. Promise.all: нужны ВСЕ результаты, но первая же ошибка делает
// остальные бессмысленными. break отменяет всё, что ещё работает, — этого
// JS-овский Promise.all не умеет: там проигравшие продолжают крутиться.
func demoAll() {
	section("all: первая ошибка отменяет остальных")

	tasks := []func(context.Context) (string, error){
		job("быстрый", 10*time.Millisecond, "готово", nil),
		job("сломанный", 30*time.Millisecond, "", errors.New("нет прав")),
		job("вечный", 10*time.Second, "этого никто не увидит", nil),
	}

	out := make([]string, len(tasks))
	var firstErr error
	for i, r := range settle.Stream(context.Background(), tasks...) {
		if r.Err != nil {
			firstErr = r.Err
			// break отменяет контекст «вечной» задачи и ЖДЁТ её выхода:
			// строка «[вечный] отменена» печатается ещё до выхода из цикла.
			break
		}
		out[i] = r.Value
	}

	if firstErr != nil {
		fmt.Printf("  сбор прерван: %v (частичные результаты: %q)\n", firstErr, out)
	}
}

// Сценарий 3. Promise.race: несколько равнозначных источников, берём самый
// быстрый ответ — неважно, успех это или ошибка. Классика: запрос к
// нескольким зеркалам, кто первый — того и тапки.
func demoRace() {
	section("race: побеждает самый быстрый")

	tasks := []func(context.Context) (string, error){
		job("зеркало-EU", 80*time.Millisecond, "ответ из Европы", nil),
		job("зеркало-US", 30*time.Millisecond, "ответ из Америки", nil),
		job("зеркало-ASIA", 120*time.Millisecond, "ответ из Азии", nil),
	}

	// Берём первую же пару и сразу выходим: break отменит оба проигравших
	// зеркала — их «отменена»-строки появятся до того, как мы пойдём дальше.
	for i, r := range settle.Stream(context.Background(), tasks...) {
		fmt.Printf("  победило зеркало #%d: %s\n", i, r.Value)
		break
	}
}

// Сценарий 4. Promise.any: как race, но ошибки не считаются победой — ждём
// первый УСПЕХ, а неудачников просто пропускаем. Если бы успехов не
// нашлось вовсе, цикл дошёл бы до конца и firstOK остался бы пустым.
func demoAny() {
	section("any: первый успех, ошибки пропускаем")

	tasks := []func(context.Context) (string, error){
		job("резерв-1", 10*time.Millisecond, "", errors.New("быстро упал")),
		job("резерв-2", 40*time.Millisecond, "живой ответ", nil),
		job("резерв-3", 10*time.Second, "этого никто не увидит", nil),
	}

	var firstOK string
	for _, r := range settle.Stream(context.Background(), tasks...) {
		if r.Err != nil {
			fmt.Printf("  пропускаем неудачника: %v\n", r.Err)
			continue // ошибка — не повод останавливаться
		}
		firstOK = r.Value
		break // успех найден, остальных (включая «резерв-3») отменит break
	}
	fmt.Printf("  итог: %s\n", firstOK)
}

// Сценарий 5. Общий дедлайн через родительский контекст: Stream не имеет
// собственных опций таймаута — и не нуждается в них, потому что таймаут
// это просто свойство ctx, который вы передаёте.
func demoTimeout() {
	section("таймаут: дедлайн всей пачки через контекст")

	// 50 мс на всё про всё. Кто не успел — получит context.DeadlineExceeded.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	tasks := []func(context.Context) (string, error){
		job("шустрый", 20*time.Millisecond, "успел", nil),
		job("копуша", 200*time.Millisecond, "не успел бы", nil),
	}

	// Семантика allSettled сохраняется и при таймауте: ровно один результат
	// на задачу, просто у опоздавших в Err лежит ошибка дедлайна.
	for i, r := range settle.Stream(ctx, tasks...) {
		switch {
		case errors.Is(r.Err, context.DeadlineExceeded):
			fmt.Printf("  #%d: не уложилась в дедлайн\n", i)
		case r.Err != nil:
			fmt.Printf("  #%d: ошибка: %v\n", i, r.Err)
		default:
			fmt.Printf("  #%d: %s\n", i, r.Value)
		}
	}
}

// Сценарий 6. Паники: задача может запаниковать, но процесс не упадёт —
// паника приедет в Result.Err как *settle.PanicError. А если паниковали
// ошибкой (идиома panic(err)), то через Unwrap до неё доберётся errors.Is.
func demoPanic() {
	section("паники: перехват вместо падения процесса")

	// Сентинель, который «внезапно» вылетит паникой из глубины задачи.
	errNoAccess := errors.New("доступ запрещён")

	tasks := []func(context.Context) (string, error){
		// Обычная задача — работает как ни в чём не бывало.
		job("сосед", 10*time.Millisecond, "жив-здоров", nil),
		// Паника строкой — типичный баг вроде выхода за границы среза.
		func(ctx context.Context) (string, error) {
			panic("что-то пошло совсем не так")
		},
		// Паника ошибкой — идиома must-хелперов: panic(fmt.Errorf(...)).
		func(ctx context.Context) (string, error) {
			panic(fmt.Errorf("проверка прав: %w", errNoAccess))
		},
	}

	for i, r := range settle.Stream(context.Background(), tasks...) {
		var pe *settle.PanicError
		switch {
		case errors.As(r.Err, &pe):
			// pe.Value — значение из panic, pe.Stack — стек горутины на
			// момент паники (здесь не печатаем, он длинный, но для логов
			// он бесценен).
			fmt.Printf("  #%d: паника: %v (стек: %d байт)\n", i, pe.Value, len(pe.Stack))
			// Благодаря PanicError.Unwrap сентинель различим даже сквозь
			// панику — цепочка errors.Is проходит до errNoAccess.
			if errors.Is(r.Err, errNoAccess) {
				fmt.Printf("  #%d: ...и это конкретно %v\n", i, errNoAccess)
			}
		case r.Err != nil:
			fmt.Printf("  #%d: ошибка: %v\n", i, r.Err)
		default:
			fmt.Printf("  #%d: %s\n", i, r.Value)
		}
	}
}
