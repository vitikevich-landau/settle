// Демонстрационная программа библиотеки settle.
//
// Запуск из корня репозитория:
//
//	go run ./examples
//
// Каждая demo-функция ниже — отдельный сценарий использования: готовые
// комбинаторы AllSettled, All, Race и Any, затем низкоуровневый Stream,
// общий дедлайн и перехват паник.
//
// Задачи здесь намеренно синтетические: они просто спят, чтобы был виден сам
// механизм. Те же приёмы на работе, похожей на боевую — HTTP-запросы к
// микросервисам, чтение файлов с диска, ручка здоровья, — лежат в
// ./examples/realworld.
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
	demoStream()
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

// Сценарий 1. AllSettled: дождаться ВСЕХ задач и собрать все исходы — и
// успехи, и ошибки. Самый частый способ использования settle.
func demoAllSettled() {
	section("AllSettled: собрать все результаты")

	tasks := []func(context.Context) (string, error){
		job("медленный", 60*time.Millisecond, "данные из БД", nil),
		job("сломанный", 10*time.Millisecond, "", errors.New("сервис недоступен")),
		job("средний", 30*time.Millisecond, "данные из кеша", nil),
	}

	// Внутри результаты приезжают по готовности, но AllSettled сам
	// раскладывает их по индексам. Возвращённый срез всегда совпадает по
	// порядку и длине с tasks.
	results := settle.AllSettled(context.Background(), tasks...)
	for i, r := range results {
		if r.Err != nil {
			fmt.Printf("  #%d: ошибка: %v\n", i, r.Err)
		} else {
			fmt.Printf("  #%d: %s\n", i, r.Value)
		}
	}
}

// Сценарий 2. All: нужны ВСЕ результаты, но первая же ошибка делает остальные
// бессмысленными. Комбинатор отменяет всё, что ещё работает, — этого
// JS-овский Promise.all сам по себе не умеет.
func demoAll() {
	section("All: первая ошибка отменяет остальных")

	tasks := []func(context.Context) (string, error){
		job("быстрый", 10*time.Millisecond, "готово", nil),
		job("сломанный", 30*time.Millisecond, "", errors.New("нет прав")),
		job("вечный", 10*time.Second, "этого никто не увидит", nil),
	}

	// При ошибке All возвращает nil вместо случайного набора успевших
	// значений. Перед возвратом он отменяет «вечную» задачу и ждёт её выхода:
	// строка «[вечный] отменена» печатается раньше строки ниже.
	values, err := settle.All(context.Background(), tasks...)
	if err != nil {
		fmt.Printf("  сбор прерван: %v (результаты: %v)\n", err, values)
		return
	}
	fmt.Printf("  всё готово: %q\n", values)
}

// Сценарий 3. Race: несколько равнозначных источников, берём самый быстрый
// ответ — неважно, успех это или ошибка.
func demoRace() {
	section("Race: побеждает самый быстрый")

	tasks := []func(context.Context) (string, error){
		job("зеркало-EU", 80*time.Millisecond, "ответ из Европы", nil),
		job("зеркало-US", 30*time.Millisecond, "ответ из Америки", nil),
		job("зеркало-ASIA", 120*time.Millisecond, "ответ из Азии", nil),
	}

	// Здесь важно само значение, а не индекс зеркала, поэтому готовая
	// обёртка проще Stream. Проигравших Race отменит и дождётся сам.
	value, err := settle.Race(context.Background(), tasks...)
	if err != nil {
		fmt.Printf("  первой пришла ошибка: %v\n", err)
		return
	}
	fmt.Printf("  самый быстрый ответ: %s\n", value)
}

// Сценарий 4. Any: как Race, но ошибки не считаются победой — ждём первый
// УСПЕХ. Если упадут все, Any вернёт errors.Join их ошибок.
func demoAny() {
	section("Any: первый успех, ошибки пропускаем")

	tasks := []func(context.Context) (string, error){
		job("резерв-1", 10*time.Millisecond, "", errors.New("быстро упал")),
		job("резерв-2", 40*time.Millisecond, "живой ответ", nil),
		job("резерв-3", 10*time.Second, "этого никто не увидит", nil),
	}

	value, err := settle.Any(context.Background(), tasks...)
	if err != nil {
		fmt.Printf("  все источники упали:\n%v\n", err)
		return
	}
	fmt.Printf("  итог: %s\n", value)
}

// Сценарий 5. Stream нужен, когда готового правила недостаточно: результат
// надо обработать сразу, его индекс влияет на решение или условие остановки
// специфично для предметной области.
func demoStream() {
	section("Stream: собственная политика обработки")

	names := []string{"профиль", "рекомендации", "заказы", "аудит"}
	required := []bool{true, false, true, false}
	tasks := []func(context.Context) (string, error){
		job("профиль", 20*time.Millisecond, "Иван", nil),
		job("рекомендации", 10*time.Millisecond, "", errors.New("сервис недоступен")),
		job("заказы", 30*time.Millisecond, "", errors.New("нет прав")),
		job("аудит", 10*time.Second, "записан", nil),
	}

	// Ни один готовый комбинатор не знает, что ошибка рекомендаций допустима,
	// а ошибка заказов фатальна. Stream отдаёт индекс и каждый результат сразу,
	// поэтому политику можно выразить непосредственно в цикле.
loop:
	for i, result := range settle.Stream(context.Background(), tasks...) {
		switch {
		case result.Err == nil:
			fmt.Printf("  ✓ %s: %s\n", names[i], result.Value)
		case required[i]:
			fmt.Printf("  ✗ обязательный этап %s: %v\n", names[i], result.Err)
			break loop
		default:
			fmt.Printf("  ~ %s пропущены: %v\n", names[i], result.Err)
		}
	}
}

// Сценарий 6. Общий дедлайн через родительский контекст: комбинаторы не имеют
// собственных опций таймаута — и не нуждаются в них, потому что таймаут
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
	for i, r := range settle.AllSettled(ctx, tasks...) {
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

// Сценарий 7. Паники: задача может запаниковать, но процесс не упадёт —
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

	for i, r := range settle.AllSettled(context.Background(), tasks...) {
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
