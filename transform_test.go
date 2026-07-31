package settle

import (
	"context"
	"errors"
	"iter"
	"reflect"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// pairs — синтетический источник пар (индекс, Result): трансформеры не должны
// зависеть от того, кто их произвёл, поэтому тестируем их без движка.
func pairs[T any](items ...Indexed[T]) iter.Seq2[int, Result[T]] {
	return func(yield func(int, Result[T]) bool) {
		for _, it := range items {
			if !yield(it.Index, it.Result) {
				return
			}
		}
	}
}

func ok[T any](i int, v T) Indexed[T] {
	return Indexed[T]{Index: i, Result: Result[T]{Value: v}}
}

func failed[T any](i int, err error) Indexed[T] {
	return Indexed[T]{Index: i, Result: Result[T]{Err: err}}
}

func TestOrderedRestoresInputOrder(t *testing.T) {
	src := pairs(ok(2, "c"), ok(0, "a"), ok(3, "d"), ok(1, "b"))

	var gotIdx []int
	var gotVal []string
	for i, r := range Ordered(src) {
		gotIdx = append(gotIdx, i)
		gotVal = append(gotVal, r.Value)
	}

	if want := []int{0, 1, 2, 3}; !reflect.DeepEqual(gotIdx, want) {
		t.Errorf("want indices %v, got %v", want, gotIdx)
	}
	if want := []string{"a", "b", "c", "d"}; !reflect.DeepEqual(gotVal, want) {
		t.Errorf("want values %v, got %v", want, gotVal)
	}
}

// Ordered остаётся последовательностью: результат отдаётся сразу, как только
// пришли все меньшие индексы, а не после конца источника.
func TestOrderedYieldsWithoutDrainingSource(t *testing.T) {
	consumed := make(chan struct{})
	src := func(yield func(int, Result[string]) bool) {
		if !yield(0, Result[string]{Value: "first"}) {
			return
		}
		// Источник продолжит отдавать, только когда потребитель уже получил
		// нулевой результат: если бы Ordered копил всё до конца, здесь был бы
		// дедлок.
		<-consumed
		yield(1, Result[string]{Value: "second"})
	}

	var got []string
	for _, r := range Ordered(iter.Seq2[int, Result[string]](src)) {
		got = append(got, r.Value)
		if len(got) == 1 {
			close(consumed)
		}
	}

	if want := []string{"first", "second"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("want %v, got %v", want, got)
	}
}

func TestOrderedBreakStopsSource(t *testing.T) {
	var produced int
	src := func(yield func(int, Result[int]) bool) {
		for i := range 10 {
			produced++
			if !yield(i, Result[int]{Value: i}) {
				return
			}
		}
	}

	for i := range Ordered(iter.Seq2[int, Result[int]](src)) {
		if i == 2 {
			break
		}
	}

	if produced > 4 {
		t.Fatalf("break не остановил источник: произведено %d элементов", produced)
	}
}

// Разрывы в нумерации возможны, если между движком и Ordered кто-то отфильтровал
// часть результатов. Терять их нельзя.
func TestOrderedHandlesGaps(t *testing.T) {
	src := pairs(ok(5, "f"), ok(0, "a"), ok(2, "c"))

	var gotIdx []int
	for i := range Ordered(src) {
		gotIdx = append(gotIdx, i)
	}

	if want := []int{0, 2, 5}; !reflect.DeepEqual(gotIdx, want) {
		t.Fatalf("want %v, got %v", want, gotIdx)
	}
}

// Интеграция с движком: задачи завершаются в обратном порядке, на выходе —
// исходный.
func TestOrderedOverMap(t *testing.T) {
	base := runtime.NumGoroutine()
	gates := make([]chan struct{}, 4)
	for i := range gates {
		gates[i] = make(chan struct{})
	}
	close(gates[3])

	var got []int
	for i, r := range Ordered(Map(context.Background(), seqOf(0, 1, 2, 3), 4,
		func(_ context.Context, v int) (int, error) {
			<-gates[v]
			if v > 0 {
				close(gates[v-1])
			}
			return v, nil
		})) {
		if r.Err != nil {
			t.Fatalf("task %d: %v", i, r.Err)
		}
		got = append(got, i)
	}

	if want := []int{0, 1, 2, 3}; !reflect.DeepEqual(got, want) {
		t.Fatalf("want %v, got %v", want, got)
	}
	waitNoExtraGoroutines(t, base)
}

func TestValuesSplitsSuccessesAndFailures(t *testing.T) {
	errA := errors.New("a failed")
	errB := errors.New("b failed")
	src := pairs(ok(3, "d"), failed[string](1, errA), ok(0, "a"), failed[string](2, errB))

	values, err := Values(src)

	if want := []string{"a", "d"}; !reflect.DeepEqual(values, want) {
		t.Errorf("want values %v, got %v", want, values)
	}
	if !errors.Is(err, errA) || !errors.Is(err, errB) {
		t.Fatalf("want both failures in the joined error, got %v", err)
	}
	lines := strings.Split(err.Error(), "\n")
	want := []string{"settle: task 1: a failed", "settle: task 2: b failed"}
	if !reflect.DeepEqual(lines, want) {
		t.Errorf("want errors ordered by index %q, got %q", want, lines)
	}
}

func TestValuesReturnsNilErrorOnFullSuccess(t *testing.T) {
	values, err := Values(pairs(ok(1, "b"), ok(0, "a")))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if want := []string{"a", "b"}; !reflect.DeepEqual(values, want) {
		t.Fatalf("want %v, got %v", want, values)
	}
}

func TestErrorsCollectsOnlyFailures(t *testing.T) {
	errBoom := errors.New("boom")
	if err := Errors(pairs(ok(0, "a"), failed[string](1, errBoom))); !errors.Is(err, errBoom) {
		t.Fatalf("want boom, got %v", err)
	}
	if err := Errors(pairs(ok(0, "a"), ok(1, "b"))); err != nil {
		t.Fatalf("want nil for a fully successful sequence, got %v", err)
	}
}

func TestBatchSplitsIntoChunks(t *testing.T) {
	var items []Indexed[int]
	for i := range 7 {
		items = append(items, ok(i, i))
	}

	var sizes []int
	var seen []int
	for chunk := range Batch(pairs(items...), 3) {
		sizes = append(sizes, len(chunk))
		for _, it := range chunk {
			seen = append(seen, it.Index)
		}
	}

	if want := []int{3, 3, 1}; !reflect.DeepEqual(sizes, want) {
		t.Errorf("want chunk sizes %v, got %v", want, sizes)
	}
	if want := []int{0, 1, 2, 3, 4, 5, 6}; !reflect.DeepEqual(seen, want) {
		t.Errorf("want indices %v, got %v", want, seen)
	}
}

func TestBatchEmitsNothingForEmptySequence(t *testing.T) {
	n := 0
	for range Batch(pairs[int](), 3) {
		n++
	}
	if n != 0 {
		t.Fatalf("want no chunks, got %d", n)
	}
}

// Отданная пачка принадлежит потребителю: следующая не должна переписать её
// содержимое.
func TestBatchChunksAreIndependent(t *testing.T) {
	var items []Indexed[int]
	for i := range 4 {
		items = append(items, ok(i, i))
	}

	var kept [][]Indexed[int]
	for chunk := range Batch(pairs(items...), 2) {
		kept = append(kept, chunk)
	}

	if len(kept) != 2 {
		t.Fatalf("want 2 chunks, got %d", len(kept))
	}
	if kept[0][0].Index != 0 || kept[0][1].Index != 1 {
		t.Fatalf("первая пачка перезаписана: %v", kept[0])
	}
}

func TestBatchBreakStopsSource(t *testing.T) {
	var produced int
	src := func(yield func(int, Result[int]) bool) {
		for i := range 100 {
			produced++
			if !yield(i, Result[int]{Value: i}) {
				return
			}
		}
	}

	for range Batch(iter.Seq2[int, Result[int]](src), 2) {
		break
	}

	if produced > 3 {
		t.Fatalf("break не остановил источник: произведено %d элементов", produced)
	}
}

func TestBatchPanicsOnNonPositiveSize(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("want panic for n <= 0")
		}
	}()
	Batch(pairs(ok(0, 1)), 0)
}

// Errors не должен удерживать значения успешных задач: на успех-ориентированном
// потоке это была бы память, которую функция тут же выбрасывает. Проверяем
// поведенчески — значение освобождается сразу после обработки.
func TestErrorsDoesNotRetainValues(t *testing.T) {
	errBroken := errors.New("broken")

	var live atomic.Int64
	type payload struct{ data []byte }

	src := func(yield func(int, Result[*payload]) bool) {
		for i := range 200 {
			p := &payload{data: make([]byte, 1024)}
			live.Add(1)
			runtime.SetFinalizer(p, func(*payload) { live.Add(-1) })

			r := Result[*payload]{Value: p}
			if i%50 == 49 {
				r = Result[*payload]{Err: errBroken}
			}
			if !yield(i, r) {
				return
			}
		}
	}

	err := Errors(iter.Seq2[int, Result[*payload]](src))
	if !errors.Is(err, errBroken) {
		t.Fatalf("want broken, got %v", err)
	}

	// Ни одна ссылка не должна пережить обход: значения нигде не накапливались.
	for range 3 {
		runtime.GC()
		time.Sleep(10 * time.Millisecond)
	}
	if got := live.Load(); got > 20 {
		t.Errorf("Errors удерживает значения: живых объектов %d из 200", got)
	}
}
