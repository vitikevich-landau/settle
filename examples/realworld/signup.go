package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/vitikevich-landau/settle"
)

// Сценарий 5. Регистрация пользователя: пачка независимых проверок, где
// первый отказ отменяет остальные.
//
// Проверки не зависят друг от друга, поэтому гонять их последовательно —
// значит складывать задержки на ровном месте. Но и доводить до конца все
// проверки, когда одна уже отказала, незачем: решение принято, а обращение
// к антифроду стоит денег за вызов и жжёт квоту у внешнего провайдера.
// break здесь — это не оптимизация задержки, а прямая экономия бюджета.
//
// Второй сюжет сценария — разница между двумя видами неудач:
//
//   - бизнес-отказ: проверка отработала и сказала «нет». Это 400, повторять
//     запрос бессмысленно, пользователю нужно объяснение.
//   - инфраструктурная ошибка: проверка не смогла отработать. Это 503,
//     регистрация валидна, но решение отложено — клиенту стоит повторить.
//
// Различить их снаружи можно только сентинелем, поэтому бизнес-отказы
// заворачиваются в errRejected и опознаются через errors.Is.

// errRejected — сентинель бизнес-отказа. Всё, что им не обёрнуто, считается
// поломкой инфраструктуры.
var errRejected = errors.New("проверка не пройдена")

// antifraudBilled считает оплаченные обращения к внешнему антифроду:
// счётчик растёт, только если вызов реально дошёл до конца.
var antifraudBilled atomic.Int32

// signupForm — заявка на регистрацию.
type signupForm struct {
	Nickname string
	Email    string
	IP       string
}

func demoSignup() {
	section("Проверки при регистрации",
		"Три независимые проверки. Первый отказ отменяет остальные, включая\n"+
			"платный вызов антифрода, — и отделяется от поломки инфраструктуры.")

	form := signupForm{Nickname: "ivan", Email: "ivan@10minutemail.com", IP: "203.0.113.7"}

	fmt.Println("\n-- прогон 1: одноразовый почтовый домен (бизнес-отказ)")
	signup(form, false)

	fmt.Println("\n-- прогон 2: сервис никнеймов недоступен (поломка)")
	signup(signupForm{Nickname: "ivan", Email: "ivan@example.com", IP: form.IP}, true)

	fmt.Println("\n-- прогон 3: всё в порядке")
	signup(signupForm{Nickname: "ivan", Email: "ivan@example.com", IP: form.IP}, false)
}

// signup прогоняет проверки и печатает решение вместе с ценой вопроса.
func signup(form signupForm, nicknameServiceDown bool) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	antifraudBilled.Store(0)

	// Задачи возвращают имя проверки: сами данные никому не нужны, важен
	// только вердикт. Result.Value идёт в лог и в текст ошибки для клиента.
	tasks := []func(context.Context) (string, error){
		checkNickname(form.Nickname, nicknameServiceDown),
		checkEmailDomain(form.Email),
		checkIPReputation(form.IP),
	}

	start := time.Now()
	var (
		rejection error // бизнес-отказ: 400
		failure   error // поломка: 503
	)

loop:
	for _, r := range settle.Stream(ctx, tasks...) {
		switch {
		case r.Err == nil:
			fmt.Printf("  ✓ %s — пройдена за %s\n", r.Value, since(start))

		case errors.Is(r.Err, errRejected):
			rejection = r.Err
			// Решение принято, остальные проверки уже ни на что не влияют.
			// break отменяет их контекст — антифрод не успеет выставить счёт.
			break loop

		default:
			failure = r.Err
			// Поломка тоже прерывает: без полного набора вердиктов регистрировать
			// пользователя нельзя, а держать остальные проверки — только жечь
			// бюджет.
			break loop
		}
	}

	switch {
	case rejection != nil:
		fmt.Printf("  → 400: %v\n", rejection)
	case failure != nil:
		fmt.Printf("  → 503: %v (клиенту стоит повторить)\n", failure)
	default:
		fmt.Printf("  → 201: пользователь %q зарегистрирован\n", form.Nickname)
	}
	fmt.Printf("     заняло %s, платных вызовов антифрода: %d\n",
		since(start), antifraudBilled.Load())
}

// checkNickname — обращение к своему же сервису пользователей: быстро и
// дёшево, поэтому идёт в общей пачке без всяких приоритетов.
func checkNickname(nick string, serviceDown bool) func(context.Context) (string, error) {
	return func(ctx context.Context) (string, error) {
		if err := sleepCtx(ctx, 25*time.Millisecond); err != nil {
			return "", err
		}
		if serviceDown {
			// Не обёрнуто в errRejected — значит поломка, а не отказ.
			return "", errors.New("сервис никнеймов: connection refused")
		}
		if nick == "admin" {
			return "", fmt.Errorf("ник %q занят: %w", nick, errRejected)
		}
		return "ник свободен", nil
	}
}

// checkEmailDomain — проверка по списку одноразовых почтовых доменов.
func checkEmailDomain(email string) func(context.Context) (string, error) {
	disposable := []string{"10minutemail.com", "mailinator.com"}
	return func(ctx context.Context) (string, error) {
		if err := sleepCtx(ctx, 40*time.Millisecond); err != nil {
			return "", err
		}
		for _, d := range disposable {
			if strings.HasSuffix(email, "@"+d) {
				return "", fmt.Errorf("домен %s одноразовый: %w", d, errRejected)
			}
		}
		return "почта пригодна", nil
	}
}

// checkIPReputation — внешний платный антифрод: медленный и с ценником за
// каждый вызов. Именно ради него в этом сценарии и нужен break.
func checkIPReputation(ip string) func(context.Context) (string, error) {
	return func(ctx context.Context) (string, error) {
		if err := sleepCtx(ctx, 200*time.Millisecond); err != nil {
			// Отменили до ответа — счёт не выставлен.
			return "", err
		}
		antifraudBilled.Add(1)
		if ip == "198.51.100.1" {
			return "", fmt.Errorf("IP %s в чёрном списке: %w", ip, errRejected)
		}
		return "репутация IP чистая", nil
	}
}

// sleepCtx имитирует сетевой вызов, уважающий отмену: настоящий клиент делал
// бы то же самое, просто внутри http.Client или драйвера БД.
func sleepCtx(ctx context.Context, d time.Duration) error {
	select {
	case <-time.After(d):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
