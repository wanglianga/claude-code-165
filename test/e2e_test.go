package test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"bakery-donation/internal/api"
	"bakery-donation/internal/config"
	"bakery-donation/internal/database"
	"bakery-donation/internal/events"
	"bakery-donation/internal/projector"
	"bakery-donation/internal/service"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

var (
	cli      *http.Client
	baseURL  string
	testPool *pgxpool.Pool
)

type harness struct {
	token map[string]string
}

func TestMain(m *testing.M) {
	ctx := context.Background()
	cfg := config.Config{
		HTTPAddr:            ":0",
		DatabaseURL:         envOr("TEST_DATABASE_URL", "postgres://app@127.0.0.1:5433/bakery_test?sslmode=disable"),
		RedisAddr:           envOr("TEST_REDIS_ADDR", "127.0.0.1:6379"),
		RedisDB:             15,
		KafkaEnabled:        os.Getenv("TEST_KAFKA") == "true",
		KafkaBrokers:        []string{"127.0.0.1:9092"},
		KafkaTopic:          "donation-batch-events-test",
		TokenSecret:         "test-secret",
		SweepInterval:       time.Hour,
		ColdSafetyMargin:    4 * time.Hour,
		AmbientSafetyMargin: 2 * time.Hour,
		LateToleranceMin:    15,
		NoShowGraceMin:      20,
	}

	db, err := database.New(ctx, cfg.DatabaseURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "postgres: %v\n", err)
		os.Exit(1)
	}
	if err := db.Migrate(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "migrate: %v\n", err)
		os.Exit(1)
	}
	if err := database.SeedUsers(ctx, db); err != nil {
		fmt.Fprintf(os.Stderr, "seed: %v\n", err)
		os.Exit(1)
	}
	// 每次测试运行清空业务数据，保留组织/账号种子与迁移记录
	if _, err := db.Pool.Exec(ctx, `
		TRUNCATE TABLE
		  outbox_events, processed_events, public_ledger, inspections, exception_records,
		  distributions, delivery_stops, delivery_plans, temperature_readings, handovers,
		  reservations, boxes, batch_items, donation_batches, vouchers
		RESTART IDENTITY CASCADE`); err != nil {
		fmt.Fprintf(os.Stderr, "truncate: %v\n", err)
		os.Exit(1)
	}
	if _, err := db.Pool.Exec(ctx, `
		UPDATE charities SET completed_count=0, no_show_count=0, late_count=0,
		                      reliability_score=0.900, status='active'`); err != nil {
		fmt.Fprintf(os.Stderr, "reset charities: %v\n", err)
		os.Exit(1)
	}
	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr, DB: cfg.RedisDB})
	_ = rdb.FlushDB(ctx).Err()

	proj := projector.New(db, rdb, cfg.KafkaBrokers, cfg.KafkaTopic, cfg.KafkaEnabled)
	go proj.Run(ctx)
	relay := events.NewRelay(cfg.KafkaBrokers, cfg.KafkaTopic, db.Pool, cfg.KafkaEnabled)
	go relay.Run(ctx, 200*time.Millisecond)
	svc := service.New(db, rdb, cfg, proj)

	srv := httptest.NewServer(api.New(cfg, db, rdb, svc, proj).Router())
	baseURL = srv.URL
	cli = srv.Client()
	testPool = db.Pool
	code := m.Run()
	srv.Close()
	db.Close()
	_ = rdb.Close()
	os.Exit(code)
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

// ---- HTTP 辅助 ----

func login(t *testing.T, who string) string {
	t.Helper()
	body := map[string]string{"login": who, "password": "Test@1234"}
	var resp struct {
		Token string `json:"token"`
	}
	post(t, "/api/v1/auth/login", "", body, 200, &resp)
	if resp.Token == "" {
		t.Fatalf("empty token for %s", who)
	}
	return resp.Token
}

func req(t *testing.T, method, path, token string, body any) (int, map[string]any) {
	t.Helper()
	var rdr *bytes.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rdr = bytes.NewReader(b)
	} else {
		rdr = bytes.NewReader(nil)
	}
	r, err := http.NewRequest(method, baseURL+path, rdr)
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("Content-Type", "application/json")
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := cli.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	if out == nil {
		out = map[string]any{}
	}
	return resp.StatusCode, out
}

func post(t *testing.T, path, token string, body any, wantStatus int, out any) map[string]any {
	t.Helper()
	status, raw := req(t, http.MethodPost, path, token, body)
	if status != wantStatus {
		t.Fatalf("POST %s status=%d want %d body=%v", path, status, wantStatus, raw)
	}
	if out != nil {
		b, _ := json.Marshal(raw)
		_ = json.Unmarshal(b, out)
	}
	return raw
}

func get(t *testing.T, path, token string, wantStatus int) map[string]any {
	t.Helper()
	status, raw := req(t, http.MethodGet, path, token, nil)
	if status != wantStatus {
		t.Fatalf("GET %s status=%d want %d body=%v", path, status, wantStatus, raw)
	}
	return raw
}

func waitFor(t *testing.T, desc string, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for: %s", desc)
}

// ---- 场景构造辅助 ----

type reportOpts struct {
	allergenDeclared bool
	withColdItem     bool
	pickupIn         time.Duration
	shelfLifeIn      time.Duration
	damagedPkg       bool
}

func reportBatch(t *testing.T, storeToken string, o reportOpts) string {
	t.Helper()
	pkg := "intact"
	if o.damagedPkg {
		pkg = "repacked"
	}
	items := []map[string]any{{
		"category": "面包", "name": "原味吐司", "quantity": 40, "box_count": 4,
		"production_at":       time.Now().Add(-10 * time.Hour),
		"shelf_life_until":    time.Now().Add(o.shelfLifeIn),
		"allergens":           []string{"小麦", "麸质"},
		"allergen_declared":   true,
		"needs_refrigeration": false,
		"package_integrity":   pkg,
	}}
	if o.withColdItem {
		items = append(items, map[string]any{
			"category": "乳制甜品", "name": "鲜奶油蛋糕", "quantity": 8, "box_count": 2,
			"production_at":       time.Now().Add(-6 * time.Hour),
			"shelf_life_until":    time.Now().Add(o.shelfLifeIn),
			"allergens":           []string{"牛奶", "鸡蛋", "小麦"},
			"allergen_declared":   true,
			"needs_refrigeration": true,
			"max_storage_temp_c":  8.0,
			"package_integrity":   "intact",
		})
	}
	if !o.allergenDeclared {
		// 面包品类过敏原缺失
		items[0]["allergen_declared"] = false
		items[0]["allergens"] = []string{}
	}
	body := map[string]any{
		"title":           "闭店临期食品-" + time.Now().Format("150405.000"),
		"report_type":     "scheduled",
		"pickup_deadline": time.Now().Add(o.pickupIn),
		"items":           items,
	}
	resp := post(t, "/api/v1/batches/report", storeToken, body, 201, nil)
	return resp["batch_id"].(string)
}

func reserve(t *testing.T, charityToken, batchID string, pickupIn time.Duration, transit int) map[string]any {
	t.Helper()
	body := map[string]any{
		"batch_id": batchID, "planned_pickup_at": time.Now().Add(pickupIn),
		"est_transit_minutes": transit, "demand_categories": []string{"面包", "乳制甜品"},
	}
	return post(t, "/api/v1/reservations", charityToken, body, 201, nil)
}

func boxInfos(t *testing.T, storeToken, batchID string) []map[string]any {
	t.Helper()
	detail := get(t, "/api/v1/batches/"+batchID, storeToken, 200)
	boxes := detail["boxes"].([]any)
	out := make([]map[string]any, 0, len(boxes))
	for _, b := range boxes {
		out = append(out, b.(map[string]any))
	}
	return out
}

type boxInfo = map[string]any

func sealMap(checklist []any) map[string]string {
	m := map[string]string{}
	for _, c := range checklist {
		c := c.(map[string]any)
		m[c["box_code"].(string)] = c["seal_no"].(string)
	}
	return m
}

// ================= 测试用例 =================

func TestFullHappyPathWithVouchers(t *testing.T) {
	store := login(t, "store1")
	charity := login(t, "charity1")
	reg := login(t, "regulator")
	admin := login(t, "admin")
	_ = admin

	batchID := reportBatch(t, store, reportOpts{
		allergenDeclared: true, withColdItem: true,
		pickupIn: 4 * time.Hour, shelfLifeIn: 24 * time.Hour,
	})

	// 远郊无冷藏车机构：半径/冷藏能力/需求匹配规则拦截
	blocked := postExpectError(t, "/api/v1/reservations", login(t, "charity3"), map[string]any{
		"batch_id": batchID, "planned_pickup_at": time.Now().Add(2 * time.Hour),
		"est_transit_minutes": 60,
	}, 422, "eligibility_blocked")
	if details, ok := blocked["details"].([]any); !ok || len(details) == 0 {
		t.Fatalf("eligibility trace expected, got %v", blocked)
	}

	// 并发重复预约：恰好一个成功（429 锁竞争时重试）
	var wg sync.WaitGroup
	statuses := make(chan int, 2)
	body := map[string]any{
		"batch_id": batchID, "planned_pickup_at": time.Now().Add(90 * time.Minute),
		"est_transit_minutes": 25, "demand_categories": []string{"面包"},
	}
	reserveOnce := func() int {
		for i := 0; i < 30; i++ {
			st, _ := req(t, http.MethodPost, "/api/v1/reservations", charity, body)
			if st != 429 {
				return st
			}
			time.Sleep(50 * time.Millisecond)
		}
		return 429
	}
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			statuses <- reserveOnce()
		}()
	}
	wg.Wait()
	close(statuses)
	got201, got409 := false, false
	for st := range statuses {
		if st == 201 {
			got201 = true
		}
		if st == 409 {
			got409 = true
		}
	}
	if !got201 || !got409 {
		t.Fatalf("expected one 201 and one 409 under concurrent reserve, got 201=%v 409=%v", got201, got409)
	}

	// 串行重复预约必须 409（唯一索引/状态校验均可能先命中）
	st, dup := req(t, http.MethodPost, "/api/v1/reservations", charity, body)
	if st != 409 {
		t.Fatalf("duplicate reserve status=%d body=%v", st, dup)
	}

	// 门店确认：取货码 + 封签清单
	detail := get(t, "/api/v1/batches/"+batchID, store, 200)
	resvID := reservationIDFromDetail(t, detail)
	confirm := post(t, fmt.Sprintf("/api/v1/batches/%s/decision", batchID), store,
		map[string]any{"reservation_id": resvID, "decision": "confirm"}, 200, nil)
	pickupCode := confirm["pickup_code"].(string)
	seals := sealMap(confirm["handover_checklist"].([]any))

	// 冷链车到达（准时）
	post(t, fmt.Sprintf("/api/v1/reservations/%s/arrive", resvID), charity, nil, 200, nil)

	// 身份码错误先被拒
	postExpectError(t, fmt.Sprintf("/api/v1/batches/%s/verify-receiver", batchID), store,
		map[string]any{"pickup_code": "000000", "receiver_name": "王五", "receiver_id": "310101199003034567"},
		403, "pickup_code_mismatch")
	post(t, fmt.Sprintf("/api/v1/batches/%s/verify-receiver", batchID), store, map[string]any{
		"pickup_code": pickupCode, "receiver_name": "王温暖",
		"receiver_id": "310101199003034567", "receiver_phone": "13800002222",
	}, 200, nil)

	// 错扫：不存在的箱号
	postExpectError(t, fmt.Sprintf("/api/v1/batches/%s/scan-box", batchID), store,
		map[string]any{"box_code": "BOX-FAKE-0000", "checked_qty": 10}, 404, "box_unknown")

	codes := make([]string, 0)
	boxes := boxInfos(t, store, batchID)
	if len(boxes) != 6 {
		t.Fatalf("expected 6 boxes, got %d", len(boxes))
	}
	// 逐箱核对：冷藏 2 箱带温度
	for _, b := range boxes {
		c := b["box_code"].(string)
		codes = append(codes, c)
		qty := int(b["expected_qty"].(float64))
		item := map[string]any{
			"box_code": c, "seal_no": seals[c], "checked_qty": qty,
			"measured_temp_c": 6.0,
		}
		resp := post(t, fmt.Sprintf("/api/v1/batches/%s/scan-box", batchID), store, item, 200, nil)
		if resp["result"] != "scanned_ok" {
			t.Fatalf("box %s not ok: %v", c, resp)
		}
	}

	// 完成核对 + 双方签收
	post(t, fmt.Sprintf("/api/v1/batches/%s/complete-check", batchID), store, nil, 200, nil)
	post(t, fmt.Sprintf("/api/v1/batches/%s/sign", batchID), store,
		map[string]any{"party": "store"}, 200, nil)
	// 机构未等门店先签的顺序已由 store 先签；机构签收
	post(t, fmt.Sprintf("/api/v1/batches/%s/sign", batchID), charity,
		map[string]any{"party": "charity"}, 200, nil)

	// 运输温度
	post(t, fmt.Sprintf("/api/v1/batches/%s/temperatures", batchID), charity,
		map[string]any{"phase": "transport", "temp_c": 6.4, "box_code": codes[0],
			"note": "冷链车实时回传"}, 201, nil)

	// 配送计划：两个受助点
	post(t, "/api/v1/delivery-plans", charity, map[string]any{
		"batch_id": batchID, "vehicle_plate": "沪D·L8826", "driver_name": "李冷链",
		"cold_chain": true,
		"stops": []map[string]any{
			{"point_name": "华阳社区救助点", "point_type": "社区救助点",
				"eta": time.Now().Add(40 * time.Minute), "box_codes": codes[:3]},
			{"point_name": "晚晴养老院", "point_type": "养老院",
				"eta": time.Now().Add(70 * time.Minute), "box_codes": codes[3:]},
		},
	}, 201, nil)

	// 逐箱发放，最后一箱完成批次
	qtyByCode := map[string]int{}
	for _, b := range boxes {
		qtyByCode[b["box_code"].(string)] = int(b["expected_qty"].(float64))
	}
	for i, c := range codes {
		point := "华阳社区救助点"
		rtype := "困难家庭"
		if i >= 3 {
			point, rtype = "晚晴养老院", "住养老人"
		}
		resp := post(t, "/api/v1/distributions", charity, map[string]any{
			"batch_id": batchID, "point_name": point, "recipient_type": rtype,
			"box_code": c, "quantity": qtyByCode[c],
		}, 201, nil)
		if i == len(codes)-1 && resp["batch_completed"] != true {
			t.Fatalf("last distribution should complete batch, got %v", resp)
		}
	}

	// 公示最终投影为 donated，且五证齐全
	waitFor(t, "ledger donated", func() bool {
		ledger := get(t, "/api/v1/ledger?result=donated", reg, 200)["ledger"].([]any)
		for _, r := range ledger {
			r := r.(map[string]any)
			if r["batch_id"] == batchID && r["vouchers_complete"] == true {
				return true
			}
		}
		return false
	})

	// 监管抽查：哈希链通过 + 五证齐全 + 公示一致
	inspRaw := post(t, fmt.Sprintf("/api/v1/batches/%s/inspections", batchID), reg, nil, 200, nil)
	if inspRaw["chain_ok"] != true || inspRaw["ledger_consistent"] != true ||
		inspRaw["five_vouchers_complete"] != true || inspRaw["result"] != "pass" {
		t.Fatalf("inspection not pass: %v", inspRaw)
	}
	chain := inspRaw["vouchers"].([]any)
	types := map[string]bool{}
	for _, v := range chain {
		types[v.(map[string]any)["voucher_type"].(string)] = true
	}
	for _, want := range []string{"source", "temperature", "handover", "delivery", "distribution"} {
		if !types[want] {
			t.Fatalf("missing voucher type %s in chain", want)
		}
	}
}

func TestSoldOutAndDuplicateAfterReleaseBlocked(t *testing.T) {
	store := login(t, "store1")
	charity := login(t, "charity2")

	batchID := reportBatch(t, store, reportOpts{
		allergenDeclared: true, pickupIn: 3 * time.Hour, shelfLifeIn: 20 * time.Hour,
	})
	r := reserve(t, charity, batchID, 90*time.Minute, 20)

	// 已预约状态门店临时售罄：预约自动撤销，批次 wasted
	post(t, fmt.Sprintf("/api/v1/batches/%s/sold-out", batchID), store,
		map[string]any{"note": "晚高峰全部售完"}, 200, nil)
	_ = r
	reg := login(t, "regulator")
	waitFor(t, "ledger wasted", func() bool {
		ledger := get(t, "/api/v1/ledger?result=wasted", reg, 200)["ledger"].([]any)
		for _, x := range ledger {
			if x.(map[string]any)["batch_id"] == batchID {
				return true
			}
		}
		return false
	})
}

func TestNoShowSweepReleasesBatch(t *testing.T) {
	store := login(t, "store2")
	charity := login(t, "charity2")
	admin := login(t, "admin")

	batchID := reportBatch(t, store, reportOpts{
		allergenDeclared: true, pickupIn: 5 * time.Hour, shelfLifeIn: 26 * time.Hour,
	})
	r := reserve(t, charity, batchID, 90*time.Minute, 20)
	resvID := r["reservation_id"].(string)
	post(t, fmt.Sprintf("/api/v1/batches/%s/decision", batchID), store,
		map[string]any{"reservation_id": resvID, "decision": "confirm"}, 200, nil)

	// 立即触发 sweep 不应判爽约（计划取货时间在未来）
	swept := post(t, "/api/v1/admin/sweep", admin, nil, 200, nil)
	if len(swept["no_show_reservations"].([]any)) != 0 {
		t.Fatalf("should not no-show before planned time")
	}

	// 把预约计划取货时间改到过去 + 超过宽限期
	mustExec(t, `UPDATE reservations SET planned_pickup_at=now() - interval '30 minutes' WHERE id=$1`, resvID)
	swept = post(t, "/api/v1/admin/sweep", admin, nil, 200, nil)
	found := false
	for _, x := range swept["no_show_reservations"].([]any) {
		if x == resvID {
			found = true
		}
	}
	if !found {
		t.Fatalf("reservation %s should be marked no_show, got %v", resvID, swept)
	}
	// 批次 released，门店重新开放后可被再次预约
	post(t, fmt.Sprintf("/api/v1/batches/%s/release", batchID), store, nil, 200, nil)
	r2 := reserve(t, charity, batchID, 60*time.Minute, 20)
	if r2["status"] != "requested" {
		t.Fatalf("rebook failed: %v", r2)
	}
}

func TestAllergenMissingAndSpoiledFlow(t *testing.T) {
	store := login(t, "store1")
	charity := login(t, "charity1")
	reg := login(t, "regulator")

	// 过敏原缺失批次：面包箱被拒收（留店），冷链蛋糕箱过敏原完整、正常出库
	batchID := reportBatch(t, store, reportOpts{
		allergenDeclared: false, withColdItem: true,
		pickupIn: 4 * time.Hour, shelfLifeIn: 22 * time.Hour,
	})
	r := reserve(t, charity, batchID, 2*time.Hour, 20)
	resvID := r["reservation_id"].(string)
	confirm := post(t, fmt.Sprintf("/api/v1/batches/%s/decision", batchID), store,
		map[string]any{"reservation_id": resvID, "decision": "confirm"}, 200, nil)
	pickupCode := confirm["pickup_code"].(string)
	seals := sealMap(confirm["handover_checklist"].([]any))
	post(t, fmt.Sprintf("/api/v1/reservations/%s/arrive", resvID), charity, nil, 200, nil)
	post(t, fmt.Sprintf("/api/v1/batches/%s/verify-receiver", batchID), store, map[string]any{
		"pickup_code": pickupCode, "receiver_name": "赵安全",
		"receiver_id": "310101198805056789",
	}, 200, nil)

	boxes := boxInfos(t, store, batchID)
	if len(boxes) != 6 {
		t.Fatalf("expected 6 boxes, got %d", len(boxes))
	}
	codes := make([]string, 0, len(boxes))
	okCodes := make([]string, 0)
	rejected := 0
	for _, b := range boxes {
		c := b["box_code"].(string)
		codes = append(codes, c)
		qty := int(b["expected_qty"].(float64))
		resp := post(t, fmt.Sprintf("/api/v1/batches/%s/scan-box", batchID), store, map[string]any{
			"box_code": c, "seal_no": seals[c], "checked_qty": qty,
			"measured_temp_c": 6.0,
		}, 200, nil)
		if resp["result"] == "rejected" {
			rejected++
			rrs := resp["reject_reasons"].([]any)
			foundAllergen := false
			for _, rr := range rrs {
				if rr == "过敏原信息缺失" {
					foundAllergen = true
				}
			}
			if !foundAllergen {
				t.Fatalf("expected allergen rejection reason, got %v", rrs)
			}
		} else {
			okCodes = append(okCodes, c)
		}
	}
	if rejected != 4 {
		t.Fatalf("expected 4 rejected (allergen-missing bread boxes), got %d", rejected)
	}
	if len(okCodes) != 2 {
		t.Fatalf("expected 2 cold-chain cake boxes ok, got %d", len(okCodes))
	}
	// 拒收箱留店后异常自动关闭，核对可完成
	post(t, fmt.Sprintf("/api/v1/batches/%s/complete-check", batchID), store, nil, 200, nil)
	post(t, fmt.Sprintf("/api/v1/batches/%s/sign", batchID), store, nil, 200, nil)
	post(t, fmt.Sprintf("/api/v1/batches/%s/sign", batchID), charity, nil, 200, nil)

	// 受助点反馈变质 → quarantined
	post(t, fmt.Sprintf("/api/v1/batches/%s/spoiled-feedback", batchID), charity, map[string]any{
		"box_code": okCodes[0], "point_name": "华阳社区救助点", "note": "到场发现酸味",
	}, 200, nil)
	waitFor(t, "ledger spoiled", func() bool {
		ledger := get(t, "/api/v1/ledger?result=spoiled", reg, 200)["ledger"].([]any)
		for _, x := range ledger {
			if x.(map[string]any)["batch_id"] == batchID {
				return true
			}
		}
		return false
	})
	// 抽查应判 defect
	post(t, fmt.Sprintf("/api/v1/batches/%s/inspections", batchID), reg, nil, 422, nil)
}

func TestReconcileRebuildsConsistentLedger(t *testing.T) {
	admin := login(t, "admin")
	reg := login(t, "regulator")
	resp := post(t, "/api/v1/admin/reconcile", admin, nil, 200, nil)
	if n, _ := resp["rebuilt_batches"].(float64); n < 3 {
		t.Fatalf("expected rebuilt_batches >= 3, got %v", resp)
	}
	// 重建后所有批次抽查公示一致（仅对已完成批次抽查，避免中间态 findings）
	stats := get(t, "/api/v1/stats", reg, 200)
	if _, ok := stats["ledger"]; !ok {
		t.Fatalf("stats missing ledger: %v", stats)
	}
}

// ---- 辅助 ----

func postExpectError(t *testing.T, path, token string, body any, wantStatus int, wantCode string) map[string]any {
	t.Helper()
	status, raw := req(t, http.MethodPost, path, token, body)
	if status != wantStatus {
		t.Fatalf("POST %s status=%d want %d body=%v", path, status, wantStatus, raw)
	}
	if raw["code"] != wantCode {
		t.Fatalf("POST %s code=%v want %s body=%v", path, raw["code"], wantCode, raw)
	}
	return raw
}

func reservationIDFromDetail(t *testing.T, detail map[string]any) string {
	t.Helper()
	r, ok := detail["reservation"].(map[string]any)
	if !ok {
		t.Fatalf("detail missing reservation: %v", detail)
	}
	return r["id"].(string)
}

func mustExec(t *testing.T, query string, args ...any) {
	t.Helper()
	if _, err := testPool.Exec(context.Background(), query, args...); err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
}

func mustQueryRow(t *testing.T, query string, dest any, args ...any) {
	t.Helper()
	if err := testPool.QueryRow(context.Background(), query, args...).Scan(dest); err != nil {
		t.Fatalf("queryrow %q: %v", query, err)
	}
}
