package settle

import (
	"context"
	"errors"
	"fmt"
)

// ErrNoTasks возвращают [Race] и [Any], когда им не передали ни одной задачи:
// без участников нельзя выбрать ни первый завершившийся результат, ни первый
// успех.
var ErrNoTasks = errors.New("settle: no tasks")

// AllSettled запускает все fns конкурентно, дожидается каждой и возвращает
// результаты в порядке исходных задач. Ошибка одной задачи не отменяет
// остальные: каждый исход остаётся в соответствующем Result.
//
// При пустом списке задач AllSettled возвращает пустой срез. Если ctx
// отменится, уже запущенные задачи получат его отмену, но функция всё равно
// дождётся их возврата — как и [Stream] при полном обходе.
func AllSettled[T any](
	ctx context.Context,
	fns ...func(context.Context) (T, error),
) []Result[T] {
	results := make([]Result[T], len(fns))
	for i, result := range Stream(ctx, fns...) {
		results[i] = result
	}
	return results
}

// All запускает все fns конкурентно и возвращает их значения в порядке
// исходных задач, только если завершились успешно все.
//
// Первая полученная ошибка отменяет ещё работающие задачи. Перед возвратом
// All дожидается, пока они отреагируют на отмену; поэтому задачи обязаны
// уважать ctx. При ошибке срез значений равен nil, а сама ошибка обёрнута
// индексом задачи и остаётся доступна через [errors.Is] / [errors.As].
//
// При пустом списке задач All возвращает пустой срез и nil.
func All[T any](
	ctx context.Context,
	fns ...func(context.Context) (T, error),
) ([]T, error) {
	values := make([]T, len(fns))
	var firstErr error

	for i, result := range Stream(ctx, fns...) {
		if result.Err != nil {
			firstErr = fmt.Errorf("settle: task %d: %w", i, result.Err)
			break
		}
		values[i] = result.Value
	}

	if firstErr != nil {
		return nil, firstErr
	}
	return values, nil
}

// Race запускает все fns конкурентно и возвращает первый завершившийся
// результат — успешный или ошибочный. Значение при ошибке сохраняется ровно
// таким, каким его вернула задача; см. контракт [Result].
//
// После первого результата Race отменяет проигравших и дожидается их
// завершения. При пустом списке задач возвращается [ErrNoTasks].
func Race[T any](
	ctx context.Context,
	fns ...func(context.Context) (T, error),
) (T, error) {
	if len(fns) == 0 {
		var zero T
		return zero, ErrNoTasks
	}

	var winner Result[T]
	for _, result := range Stream(ctx, fns...) {
		winner = result
		break
	}
	return winner.Value, winner.Err
}

// Any запускает все fns конкурентно и возвращает первый успешный результат.
// Ошибки отдельных задач не останавливают ожидание, пока остаётся шанс на
// успех.
//
// После первого успеха Any отменяет оставшиеся задачи и дожидается их
// завершения. Если ошибкой завершились все, Any возвращает [errors.Join] из
// ошибок, обёрнутых индексами задач и упорядоченных как исходный список fns,
// а не по времени завершения. При пустом списке возвращается [ErrNoTasks].
func Any[T any](
	ctx context.Context,
	fns ...func(context.Context) (T, error),
) (T, error) {
	if len(fns) == 0 {
		var zero T
		return zero, ErrNoTasks
	}

	errs := make([]error, len(fns))
	var winner T
	found := false

	for i, result := range Stream(ctx, fns...) {
		if result.Err != nil {
			errs[i] = fmt.Errorf("settle: task %d: %w", i, result.Err)
			continue
		}
		winner = result.Value
		found = true
		break
	}

	if found {
		return winner, nil
	}

	var zero T
	return zero, errors.Join(errs...)
}
