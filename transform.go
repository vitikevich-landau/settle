package settle

import (
	"errors"
	"fmt"
	"iter"
	"maps"
	"slices"
)

// Indexed связывает результат с индексом его задачи. Нужен там, где пары
// (индекс, Result) приходится складывать в обычную последовательность —
// например в [Batch].
type Indexed[T any] struct {
	Index int
	Result[T]
}

// Ordered переупорядочивает последовательность результатов по возрастанию
// индекса, оставляя её последовательностью: результат с индексом i отдаётся
// сразу, как только пришли все результаты с меньшими индексами. Ждать всю
// пачку, как это делает [AllSettled], не нужно.
//
// Трансформер ничего не знает про способ запуска задач, поэтому одинаково
// работает поверх [Stream] и поверх [Map]:
//
//	for i, r := range settle.Ordered(settle.Map(ctx, urls, 20, fetch)) {
//		// i идёт 0, 1, 2, … — строго в порядке входа
//	}
//
// Цена — буфер: результаты, «обогнавшие» ещё не пришедший меньший индекс,
// копятся в памяти. Если нулевая задача висит до последнего, к концу обхода в
// буфере окажутся все остальные. Обратная сторона этого же свойства —
// бэкпрешер: пока Ordered не отдал накопленное, он не читает источник, а
// значит [Map] не берёт из входа новые элементы.
//
// Прерывание обхода через break пробрасывается в исходную последовательность,
// то есть отменяет задачи ровно так же, как break по [Stream] или [Map].
func Ordered[T any](seq iter.Seq2[int, Result[T]]) iter.Seq2[int, Result[T]] {
	return func(yield func(int, Result[T]) bool) {
		pending := make(map[int]Result[T])
		next := 0

		for i, r := range seq {
			pending[i] = r
			// Отдаём всё, что успело выстроиться в непрерывную цепочку от
			// next: один пришедший результат может «разблокировать» сразу
			// длинный хвост накопленных.
			for {
				r, ok := pending[next]
				if !ok {
					break
				}
				delete(pending, next)
				if !yield(next, r) {
					return
				}
				next++
			}
		}

		// Хвост на случай разрывов в нумерации: [Stream] и [Map] дают
		// сплошные индексы 0..N-1, но Ordered не обязан этого требовать от
		// произвольного источника — иначе часть результатов молча пропала бы.
		for _, i := range slices.Sorted(maps.Keys(pending)) {
			if !yield(i, pending[i]) {
				return
			}
		}
	}
}

// Values обходит последовательность до конца и раскладывает её на две части:
// значения успешных задач в порядке возрастания индекса и объединённую ошибку
// всех неуспешных.
//
// Ошибка каждой неудачи оборачивается индексом задачи и складывается через
// [errors.Join] в порядке индексов, а не в порядке завершения — как это
// делает [Any]. Если неудач не было, ошибка равна nil.
//
// В отличие от [All], срез значений не обнуляется при ошибке: частичный успех
// остаётся доступен. Это делает Values удобным для «прогнать всё и разобрать
// потом»: успехи идут дальше по конвейеру, ошибки — в лог или в отдельную
// таблицу.
//
// Длина среза равна числу успешных задач, поэтому позиция значения в нём в
// общем случае не совпадает с индексом задачи. Когда важна именно позиция,
// используйте [AllSettled] или обход [Ordered].
func Values[T any](seq iter.Seq2[int, Result[T]]) ([]T, error) {
	type failure struct {
		idx int
		err error
	}

	var (
		ok       []Indexed[T]
		failures []failure
	)
	for i, r := range seq {
		if r.Err != nil {
			failures = append(failures, failure{idx: i, err: r.Err})
			continue
		}
		ok = append(ok, Indexed[T]{Index: i, Result: r})
	}

	slices.SortFunc(ok, func(a, b Indexed[T]) int { return a.Index - b.Index })
	values := make([]T, len(ok))
	for i, r := range ok {
		values[i] = r.Value
	}

	if len(failures) == 0 {
		return values, nil
	}

	slices.SortFunc(failures, func(a, b failure) int { return a.idx - b.idx })
	errs := make([]error, len(failures))
	for i, f := range failures {
		errs[i] = fmt.Errorf("settle: task %d: %w", f.idx, f.err)
	}
	return values, errors.Join(errs...)
}

// Errors обходит последовательность до конца и собирает только ошибки —
// значения отбрасываются. Каждая ошибка обёрнута индексом своей задачи,
// объединение идёт через [errors.Join] в порядке индексов. Если все задачи
// успешны, Errors возвращает nil.
//
// Это форма для задач-эффектов: разослать уведомления, инвалидировать кеши,
// прогреть реплики — там, где возвращаемое значение не несёт смысла, а важен
// только список того, что не получилось.
func Errors[T any](seq iter.Seq2[int, Result[T]]) error {
	_, err := Values(seq)
	return err
}

// Batch собирает последовательность результатов в пачки по n штук, сохраняя
// за каждым результатом индекс его задачи. Последняя пачка может быть
// короче n; пустых пачек Batch не отдаёт.
//
// Пачка — это единица работы для следующей стадии конвейера: батч-вставка в
// базу, отправка в очередь, запись файла. В сочетании с [Ordered] пачки идут
// в порядке входа, поэтому позиция элемента сохраняется на всём пути:
//
//	for chunk := range settle.Batch(settle.Ordered(results), 500) {
//		writeToDatabase(chunk)
//	}
//
// Batch отдаёт срез, которым дальше владеет потребитель: следующая пачка
// собирается в новый срез, поэтому её можно передавать в другую горутину без
// копирования.
//
// Batch паникует, если n <= 0: это ошибка вызывающего.
func Batch[T any](seq iter.Seq2[int, Result[T]], n int) iter.Seq[[]Indexed[T]] {
	if n <= 0 {
		panic("settle: Batch requires n > 0")
	}

	return func(yield func([]Indexed[T]) bool) {
		batch := make([]Indexed[T], 0, n)
		for i, r := range seq {
			batch = append(batch, Indexed[T]{Index: i, Result: r})
			if len(batch) < n {
				continue
			}
			if !yield(batch) {
				return
			}
			// Новый срез, а не batch[:0]: отданную пачку потребитель может
			// держать сколь угодно долго, и переиспользование массива тихо
			// переписало бы её содержимое.
			batch = make([]Indexed[T], 0, n)
		}
		if len(batch) > 0 {
			yield(batch)
		}
	}
}
