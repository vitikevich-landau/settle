package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/vitikevich-landau/settle"
)

// Сценарий 1. BFF / API-gateway: собрать одну страницу из нескольких
// микросервисов.
//
// Постановка задачи, знакомая любому, кто писал бэкенд для мобильного
// приложения: чтобы отрисовать экран «Мой профиль», нужно сходить в три
// разных сервиса. Ходить последовательно нельзя — сумма задержек не влезет в
// бюджет ответа. Ходить параллельно и падать от любой ошибки тоже нельзя:
// баннеры с рекомендациями — не повод не показать пользователю его заказы.
//
// Отсюда деление фрагментов на обязательные и необязательные:
//   - профиль и заказы обязательны: без них страницы не существует, первая
//     же ошибка должна оборвать сбор и отменить оставшиеся запросы;
//   - рекомендации необязательны: не ответили — рисуем страницу без них.

// Модели ответов трёх сервисов. Разные типы — это здесь принципиально, ниже
// объясняется, что с этим делать.
type profile struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

type order struct {
	ID    string `json:"id"`
	Total int    `json:"total"`
}

type banner struct {
	SKU   string `json:"sku"`
	Title string `json:"title"`
}

// Индексы фрагментов. Индекс в settle — это позиция задачи в списке
// аргументов, и он же единственный способ понять, чей результат приехал.
// Держать его именованной константой намного надёжнее, чем считать
// «ну, заказы вроде вторые».
const (
	fragProfile = iota
	fragOrders
	fragBanners
)

// Обязательность фрагмента по его индексу. Длина среза равна числу задач —
// иначе required[i] однажды выйдет за границы.
var required = []bool{fragProfile: true, fragOrders: true, fragBanners: false}

func demoAggregate() {
	section("Сбор страницы из микросервисов (BFF)",
		"Профиль и заказы обязательны, рекомендации — нет. Смотрим два прогона:\n"+
			"в первом падает необязательный фрагмент, во втором — обязательный.")

	mux := http.NewServeMux()
	mux.Handle("/profile", respondAfter("профиль", 120*time.Millisecond, http.StatusOK,
		profile{ID: 42, Name: "Иван Петров"}))
	mux.Handle("/recommendations", respondAfter("рекомендации", 20*time.Millisecond,
		http.StatusInternalServerError, nil))
	// Сервис заказов умеет ломаться по запросу — так один и тот же стенд
	// отыгрывает оба прогона.
	mux.HandleFunc("/orders", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("fail") != "" {
			respondAfter("заказы", 20*time.Millisecond, http.StatusBadGateway, nil)(w, r)
			return
		}
		respondAfter("заказы", 70*time.Millisecond, http.StatusOK, []order{
			{ID: "A-1043", Total: 1990},
			{ID: "A-1044", Total: 450},
		})(w, r)
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()

	fmt.Println("\n-- прогон 1: лежит сервис рекомендаций (некритично)")
	renderPage(srv.URL, srv.URL+"/orders")

	fmt.Println("\n-- прогон 2: лёг сервис заказов (критично)")
	renderPage(srv.URL, srv.URL+"/orders?fail=1")
}

// renderPage собирает страницу и печатает, что получилось.
func renderPage(base, ordersURL string) {
	// Бюджет ответа на весь экран. Дедлайн задаётся снаружи, у Stream нет и
	// не нужно собственных опций таймаута: таймаут — свойство контекста.
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()

	// Фрагменты страницы разного типа, а Stream[T] параметризован одним T.
	// Поэтому задачи не возвращают данные наружу, а раскладывают их по полям
	// page, отдавая через settle только человекочитаемое имя фрагмента — для
	// логов. Это тот же приём, что и с errgroup.Go(func() error {...}).
	//
	// Гонки здесь нет по двум причинам: каждая задача пишет в собственное
	// поле, и к моменту выхода из цикла все горутины Stream уже завершились —
	// это гарантия библиотеки, а не удачное стечение обстоятельств.
	//
	// Если бы все три фрагмента были одного типа (как в сценарии с репликами
	// ниже), значения возвращались бы обычным путём, через Result.Value.
	var page struct {
		Profile profile
		Orders  []order
		Banners []banner
	}

	tasks := []func(context.Context) (string, error){
		fragProfile: func(ctx context.Context) (string, error) {
			v, err := fetchJSON[profile](ctx, base+"/profile")
			page.Profile = v
			return "профиль", err
		},
		fragOrders: func(ctx context.Context) (string, error) {
			v, err := fetchJSON[[]order](ctx, ordersURL)
			page.Orders = v
			return "заказы", err
		},
		fragBanners: func(ctx context.Context) (string, error) {
			v, err := fetchJSON[[]banner](ctx, base+"/recommendations")
			page.Banners = v
			return "рекомендации", err
		},
	}

	start := time.Now()
	var fatal error

	// Метка нужна из-за старой ловушки Go: break внутри switch выходит из
	// switch, а не из цикла. С range-over-func это особенно обидно — тихо
	// потерялась бы вся отмена. break loop работает как обычно: yield
	// вернёт false, Stream отменит контекст и дождётся оставшихся задач.
loop:
	for i, r := range settle.Stream(ctx, tasks...) {
		switch {
		case r.Err == nil:
			fmt.Printf("  ✓ %s — получен за %s\n", r.Value, since(start))

		case required[i]:
			fatal = fmt.Errorf("фрагмент #%d: %w", i, r.Err)
			fmt.Printf("  ✗ обязательный фрагмент #%d упал: %v\n", i, r.Err)
			break loop

		default:
			// Необязательный фрагмент: пишем в лог, живём дальше. Именно так
			// выглядит graceful degradation — пользователь получает страницу,
			// пусть и без блока рекомендаций.
			fmt.Printf("  ~ необязательный фрагмент #%d недоступен: %v\n", i, r.Err)
		}
	}

	if fatal != nil {
		// Сюда мы попали уже после того, как Stream отменил и дождался
		// оставшиеся запросы, — строка «клиент отменил запрос» выше в выводе
		// напечаталась раньше этой.
		fmt.Printf("  → 502: страницу собрать не удалось (%v), потрачено %s\n", fatal, since(start))
		return
	}

	fmt.Printf("  → 200: %s, заказов: %d, баннеров: %d, собрано за %s\n",
		page.Profile.Name, len(page.Orders), len(page.Banners), since(start))
}
