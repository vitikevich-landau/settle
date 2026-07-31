package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/vitikevich-landau/settle"
)

// Сценарий 4. Контрольные суммы файлов: настоящий ввод-вывод, а не сон.
//
// Здесь два вопроса, которые в синтетических примерах не возникают.
//
// Первый: сколько задач запускать разом. settle сознательно поднимает по
// горутине на задачу, без пула воркеров. Для десятка HTTP-запросов это ровно
// то, что нужно. Для десяти тысяч файлов — нет: столько же одновременных
// open(2) упрутся в лимит дескрипторов, а диск получит случайный доступ
// вместо последовательного и станет медленнее, а не быстрее. Ограничитель
// пишется снаружи одной строкой: slices.Chunk режет список на пачки, каждая
// пачка — отдельный Stream. Кстати, slices.Chunk сам возвращает iter.Seq —
// то есть здесь один нативный итератор крутится поверх другого.
//
// Второй: файловый ввод-вывод в Go не принимает контекст. os.File.Read нельзя
// прервать снаружи, поэтому «отменяемость» приходится строить руками —
// проверять ctx между порциями данных. Без этого break из цикла честно
// дождётся, пока задача дочитает свой гигабайт до конца.

// sum — контрольная сумма одного файла.
type sum struct {
	Name string
	Hash string
	Size int64
}

func demoChecksums() {
	section("Контрольные суммы файлов",
		"Настоящее чтение с диска: конкурентность ограничена пачками, а\n"+
			"неотменяемый файловый ввод-вывод сделан отменяемым вручную.")

	dir, paths := makeSampleFiles()
	defer os.RemoveAll(dir)

	// Файл, которого нет: в реальном обходе директории всегда найдётся то,
	// что удалили между os.ReadDir и открытием, или то, на что нет прав.
	paths = append(paths, filepath.Join(dir, "потеряшка.bin"))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Семантика allSettled: одна нечитаемая картинка не повод бросать обход,
	// поэтому результаты складываем целиком и разбираем после.
	results := make([]settle.Result[sum], 0, len(paths))

	const batch = 4
	start := time.Now()
	for group := range slices.Chunk(paths, batch) {
		// Отмену проверяем на границе пачек: продолжать обход после того, как
		// вызывающий передумал, незачем.
		if ctx.Err() != nil {
			break
		}

		tasks := make([]func(context.Context) (sum, error), len(group))
		for i, p := range group {
			tasks[i] = hashFile(p)
		}

		// Порядок внутри пачки восстанавливаем по индексу — итоговый список
		// совпадёт с исходным списком путей.
		batchResults := make([]settle.Result[sum], len(group))
		for i, r := range settle.Stream(ctx, tasks...) {
			batchResults[i] = r
		}
		results = append(results, batchResults...)
	}

	var (
		total  int64
		hashed int
	)
	for i, r := range results {
		if r.Err != nil {
			fmt.Printf("  ✗ %-14s %v\n", filepath.Base(paths[i]), shortErr(r.Err))
			continue
		}
		hashed++
		total += r.Value.Size
		fmt.Printf("  ✓ %-14s %s  %d байт\n", r.Value.Name, r.Value.Hash, r.Value.Size)
	}
	// Знаменатель — len(paths), а не len(results): если дедлайн истёк между
	// пачками, до части путей обход не добрался, и в results их просто нет.
	// Считать «сколько успели» от «сколько успели» — верный способ отчитаться
	// «4 из 4» там, где на входе было десять файлов.
	fmt.Printf("  → посчитано %d файлов из %d (%d байт) пачками по %d за %s\n",
		hashed, len(paths), total, batch, since(start))
	if skipped := len(paths) - len(results); skipped > 0 {
		fmt.Printf("     до %d файлов обход не добрался: истёк дедлайн\n", skipped)
	}
}

// hashFile собирает задачу, считающую SHA-256 одного файла.
func hashFile(path string) func(context.Context) (sum, error) {
	return func(ctx context.Context) (sum, error) {
		// Дешёвая проверка на входе: если пачка уже отменена, не открываем
		// файл вообще.
		if err := ctx.Err(); err != nil {
			return sum{}, err
		}

		f, err := os.Open(path)
		if err != nil {
			return sum{}, err
		}
		defer f.Close()

		h := sha256.New()
		n, err := copyContext(ctx, h, f)
		if err != nil {
			return sum{}, fmt.Errorf("%s: %w", filepath.Base(path), err)
		}
		return sum{
			Name: filepath.Base(path),
			// Первых 12 символов достаточно, чтобы вывод оставался читаемым.
			Hash: hex.EncodeToString(h.Sum(nil))[:12],
			Size: n,
		}, nil
	}
}

// copyContext — это io.Copy, который проверяет отмену между порциями данных.
// Ровно так делают отменяемым ввод-вывод, который отмены не поддерживает:
// прервать текущий Read нельзя, но можно не начинать следующий. Размер буфера
// задаёт гранулярность реакции — 32 КиБ с диска читаются за микросекунды,
// так что задержка отмены незаметна.
func copyContext(ctx context.Context, dst io.Writer, src io.Reader) (int64, error) {
	buf := make([]byte, 32<<10)
	var total int64
	for {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		n, err := src.Read(buf)
		if n > 0 {
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return total, werr
			}
			total += int64(n)
		}
		switch {
		case err == io.EOF:
			return total, nil
		case err != nil:
			return total, err
		}
	}
}

// makeSampleFiles готовит временную директорию с файлами разного размера,
// чтобы задачи заканчивались вразнобой, как это бывает на реальных данных.
func makeSampleFiles() (dir string, paths []string) {
	dir, err := os.MkdirTemp("", "settle-checksums-")
	if err != nil {
		panic(err)
	}
	for i := range 9 {
		name := fmt.Sprintf("data-%02d.bin", i)
		path := filepath.Join(dir, name)
		// Размер растёт с номером: от 4 КиБ до почти мегабайта.
		content := bytes.Repeat([]byte{byte('a' + i)}, 4<<10*(i*i+1))
		if err := os.WriteFile(path, content, 0o600); err != nil {
			panic(err)
		}
		paths = append(paths, path)
	}
	return dir, paths
}
