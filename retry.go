package settle

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"math"
	"math/rand/v2"
	"time"
)

// RetryPolicy описывает, сколько раз и через какие паузы повторять неудачную
// задачу.
//
// Backoff задаёт паузы между попытками, и его длина — это число ПОВТОРОВ, а
// не общее число вызовов: пустая последовательность означает «одна попытка,
// без повторов», три задержки — до четырёх вызовов задачи. Паузы берутся
// лениво, поэтому генератор может быть и бесконечным — тогда ограничителем
// становится отмена контекста.
//
// Retryable решает, имеет ли смысл повторять конкретную ошибку. Значение nil
// означает «повторять любую» — годится для черновика, но в бою почти всегда
// нужна классификация: 5xx и таймауты повторять стоит, 400 и «файл не
// найден» — нет.
type RetryPolicy struct {
	Backoff   iter.Seq[time.Duration]
	Retryable func(error) bool
}

// Retry превращает задачу в задачу с повторами. Это декоратор: он ничего не
// знает про [Stream] и [Map], а те ничего не знают про него — снаружи
// остаётся обычная функция задачи, поэтому контракт «ровно один Result на
// задачу» сохраняется, а индексы и порядок не съезжают.
//
// Именно поэтому повторять нужно здесь, а не на уровне движка: перезапуск
// целой пачки ради одной неудачной ссылки стоил бы всех остальных запросов.
//
// Исходы:
//
//   - успех — значение задачи и nil;
//   - ошибка, которую Retryable отверг как неповторяемую, — возвращается как
//     есть, без обёрток и без пауз;
//   - повторы исчерпаны — последняя ошибка, обёрнутая числом попыток;
//   - контекст отменён во время паузы — [errors.Join] последней ошибки и
//     причины отмены, так что [errors.Is] находит и предметную ошибку, и
//     context.Canceled / context.DeadlineExceeded.
//
// Во всех неуспешных исходах возвращается значение ПОСЛЕДНЕЙ попытки, а не
// нулевое: пакет принципиально не зануляет Value при ошибке (см. [Result]), и
// декоратор не имеет права менять это свойство задачи. Задача в духе
// io.Reader, отдающая частичный результат вместе с ошибкой, остаётся такой же
// и под Retry.
//
// Паника внутри задачи не повторяется: паника — это дефект кода, а не
// временный сбой, поэтому она летит дальше и превращается в *[PanicError] уже
// в движке.
//
// Таймаут на попытку ставится композицией, и порядок обёрток имеет значение:
//
//	settle.Retry(policy, settle.Timeout(2*time.Second, fetch)) // таймаут на КАЖДУЮ попытку
//	settle.Timeout(2*time.Second, settle.Retry(policy, fetch)) // таймаут на ВСЕ попытки разом
func Retry[T any](p RetryPolicy, fn func(context.Context) (T, error)) func(context.Context) (T, error) {
	retryable := p.Retryable
	if retryable == nil {
		retryable = func(error) bool { return true }
	}

	return func(ctx context.Context) (T, error) {
		var (
			zero     T
			lastVal  T
			lastErr  error
			attempts int
		)

		// Значение последней попытки не выбрасывается ни в одной ветке: пакет
		// принципиально не зануляет Value при ошибке (см. [Result]), и
		// декоратор не имеет права менять это поведение задачи.
		attempt := func() (T, error, bool) {
			attempts++
			v, err := fn(ctx)
			lastVal, lastErr = v, err
			switch {
			case err == nil:
				return v, nil, true
			case !retryable(err):
				return v, err, true
			}
			return zero, nil, false
		}

		if v, err, done := attempt(); done {
			return v, err
		}

		if p.Backoff != nil {
			for delay := range p.Backoff {
				if err := sleepCtx(ctx, delay); err != nil {
					// Пауза прервана: повторять уже некуда, но и терять
					// предметную причину неудачи не стоит — отдаём обе.
					return lastVal, errors.Join(lastErr, err)
				}
				if v, err, done := attempt(); done {
					return v, err
				}
			}
		}

		return lastVal, fmt.Errorf("settle: %d attempts: %w", attempts, lastErr)
	}
}

// sleepCtx — пауза, уважающая отмену: возвращает ошибку контекста, если тот
// отменился раньше, чем истекла задержка. Без неё повторы продолжали бы спать
// уже после того, как потребитель ушёл, и движок ждал бы их возврата.
func sleepCtx(ctx context.Context, d time.Duration) error {
	// Отмена важнее истёкшего таймера, поэтому проверяем её дважды: до
	// ожидания и в ветке таймера. При короткой паузе обе ветви select готовы
	// одновременно, а выбор между готовыми случаен — без этих проверок
	// повторы продолжались бы уже после отмены.
	if err := ctx.Err(); err != nil {
		return err
	}
	if d <= 0 {
		return nil
	}

	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return ctx.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Exponential возвращает retries задержек, растущих геометрически:
// base, base*factor, base*factor², и так далее.
//
// Классический выбор — base порядка сотен миллисекунд и factor = 2. Почти
// всегда его стоит дополнить [Jitter], иначе пачка задач, стартовавшая
// одновременно, будет и повторять одновременно, накрывая уже перегруженный
// сервис синхронными волнами.
func Exponential(base time.Duration, factor float64, retries int) iter.Seq[time.Duration] {
	return func(yield func(time.Duration) bool) {
		d := float64(base)
		for range retries {
			if !yield(toDuration(d)) {
				return
			}
			d *= factor
		}
	}
}

// toDuration переводит секунды-в-плавающей-точке в time.Duration, насыщая
// результат вместо переполнения.
//
// Обычное преобразование здесь — ловушка: при выходе за диапазон int64 оно
// даёт неопределённый результат, на практике отрицательный. Отрицательная
// задержка для sleepCtx означает «не спать вовсе», то есть очень долгая пауза
// превратилась бы в мгновенный повтор — поведение, противоположное
// задуманному.
//
// Сравнение идёт с math.MaxInt64 в плавающей точке, где эта константа
// округляется вверх до 2⁶³ — на единицу больше, чем помещается в int64.
// Поэтому граница нестрогая: значения, равные ей, тоже насыщаются.
func toDuration(f float64) time.Duration {
	switch {
	case f >= math.MaxInt64:
		return time.Duration(math.MaxInt64)
	case f <= 0:
		return 0
	default:
		return time.Duration(f)
	}
}

// Constant возвращает retries одинаковых задержек. Подходит там, где темп
// диктует не перегрузка, а внешнее ограничение — например известное окно
// квоты.
func Constant(d time.Duration, retries int) iter.Seq[time.Duration] {
	return Exponential(d, 1, retries)
}

// Cap ограничивает каждую задержку сверху. Экспонента без потолка за десяток
// повторов уходит в часы, поэтому связка Cap(Exponential(...), 30*time.Second)
// — обычная практика.
func Cap(seq iter.Seq[time.Duration], max time.Duration) iter.Seq[time.Duration] {
	return func(yield func(time.Duration) bool) {
		for d := range seq {
			if d > max {
				d = max
			}
			if !yield(d) {
				return
			}
		}
	}
}

// Jitter размывает каждую задержку случайным множителем из диапазона
// [1-frac, 1+frac]: при frac = 0.5 задержка в секунду превращается в
// случайную от 0.5 до 1.5 секунды. Это расталкивает повторы одновременно
// стартовавших задач и снимает синхронные волны нагрузки на чужой сервис.
//
// frac ≤ 0 оставляет последовательность нетронутой, frac ≥ 1 обрезается до 1,
// чтобы задержка не стала отрицательной.
func Jitter(seq iter.Seq[time.Duration], frac float64) iter.Seq[time.Duration] {
	// Нормализуем ДО создания замыкания. Внутри это была бы запись в
	// захваченную переменную, а последовательность рассчитана на то, что её
	// обходят конкурентно: одну политику разделяют все задачи пачки. Тогда
	// параллельные обходы писали бы и читали один и тот же frac — гонка,
	// пусть даже записывается всегда одно и то же значение.
	if frac > 1 {
		frac = 1
	}

	return func(yield func(time.Duration) bool) {
		if frac <= 0 {
			for d := range seq {
				if !yield(d) {
					return
				}
			}
			return
		}
		for d := range seq {
			// rand.Float64 из math/rand/v2 безопасен для конкурентного
			// использования, поэтому одну и ту же политику можно смело
			// раздать всем задачам пачки.
			k := 1 + frac*(2*rand.Float64()-1)
			// Насыщение обязательно: источник вполне может отдать задержку у
			// верхней границы диапазона — [Exponential] именно ею и
			// заканчивается, — а множитель больше единицы вывел бы
			// произведение за пределы int64.
			if !yield(toDuration(float64(d) * k)) {
				return
			}
		}
	}
}
