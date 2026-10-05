package service

import (
	"context"
	"strings"
	"time"

	"bakery-donation/internal/apperror"
	"bakery-donation/internal/events"
	"bakery-donation/internal/geo"
	"bakery-donation/internal/rules"

	"github.com/jackc/pgx/v5"
)

// OpenBatchView 可预约批次视图
type OpenBatchView struct {
	BatchID            string    `json:"batch_id"`
	BatchCode          string    `json:"batch_code"`
	StoreName          string    `json:"store_name"`
	StoreAddress       string    `json:"store_address"`
	DistanceKm         float64   `json:"distance_km"`
	Title              string    `json:"title"`
	ReportType         string    `json:"report_type"`
	Categories         []string  `json:"categories"`
	TotalBoxes         int       `json:"total_boxes"`
	TotalQuantity      int       `json:"total_quantity"`
	NeedsRefrigeration bool      `json:"needs_refrigeration"`
	AllergenComplete   bool      `json:"allergen_complete"`
	PickupDeadline     time.Time `json:"pickup_deadline"`
	DonationDeadline   time.Time `json:"donation_deadline"`
	// 针对查询机构的预检（不代表预约成功）
	EligibilityPreview []rules.Trace `json:"eligibility_preview"`
	Blocked            bool          `json:"blocked"`
}

// ListOpenBatches 公益机构视角列出可预约批次，并按规则给出预检。
func (s *Service) ListOpenBatches(ctx context.Context, charityID string, radiusOnly bool) ([]OpenBatchView, error) {
	cid, err := parseUUID(charityID)
	if err != nil {
		return nil, apperror.New(400, "invalid_charity", "公益机构无效")
	}
	rows, err := s.db.Pool.Query(ctx, `
		SELECT b.id, b.code, s.name, s.address, s.longitude, s.latitude,
		       b.title, b.report_type, b.total_boxes, b.total_quantity,
		       b.needs_refrigeration, b.allergen_complete,
		       b.pickup_deadline, b.donation_deadline,
		       (SELECT COALESCE(array_agg(DISTINCT i.category),'{}')
		          FROM batch_items i WHERE i.batch_id=b.id) AS categories
		FROM donation_batches b JOIN stores s ON s.id=b.store_id
		WHERE b.status='open' AND b.donation_deadline > now()
		ORDER BY b.donation_deadline`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ch struct {
		lng, lat, radius        float64
		capacity                int
		cold                    bool
		rel                     float64
		completed, noShow, late int
		needs                   []string
	}
	err = s.db.Pool.QueryRow(ctx, `
		SELECT longitude, latitude, service_radius_km, daily_capacity_boxes,
		       has_cold_chain_vehicle, reliability_score,
		       completed_count, no_show_count, late_count
		FROM charities WHERE id=$1 AND status='active'`, cid).
		Scan(&ch.lng, &ch.lat, &ch.radius, &ch.capacity, &ch.cold, &ch.rel,
			&ch.completed, &ch.noShow, &ch.late)
	if err == pgx.ErrNoRows {
		return nil, apperror.New(404, "charity_not_found", "公益机构不存在或已停用")
	}
	if err != nil {
		return nil, err
	}
	nr, err := s.db.Pool.Query(ctx,
		`SELECT category FROM charity_needs WHERE charity_id=$1`, cid)
	if err != nil {
		return nil, err
	}
	for nr.Next() {
		var c string
		if err := nr.Scan(&c); err != nil {
			nr.Close()
			return nil, err
		}
		ch.needs = append(ch.needs, c)
	}
	nr.Close()

	var bookedToday int
	err = s.db.Pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(
		    (SELECT total_boxes FROM donation_batches b WHERE b.id=r.batch_id)),0)
		FROM reservations r
		WHERE r.charity_id=$1 AND r.status IN ('requested','confirmed')
		  AND r.requested_at::date = CURRENT_DATE`, cid).Scan(&bookedToday)
	if err != nil {
		return nil, err
	}

	out := []OpenBatchView{}
	now := time.Now()
	for rows.Next() {
		var v OpenBatchView
		var slng, slat float64
		if err := rows.Scan(&v.BatchID, &v.BatchCode, &v.StoreName, &v.StoreAddress,
			&slng, &slat, &v.Title, &v.ReportType, &v.TotalBoxes, &v.TotalQuantity,
			&v.NeedsRefrigeration, &v.AllergenComplete,
			&v.PickupDeadline, &v.DonationDeadline, &v.Categories); err != nil {
			return nil, err
		}
		v.DistanceKm = float64(int(geo.DistanceKm(ch.lng, ch.lat, slng, slat)*100)) / 100
		if radiusOnly && v.DistanceKm > ch.radius {
			continue
		}
		match := 0
		needSet := map[string]bool{}
		for _, n := range ch.needs {
			needSet[n] = true
		}
		seen := map[string]bool{}
		for _, c := range v.Categories {
			if needSet[c] && !seen[c] {
				match++
				seen[c] = true
			}
		}
		blocked, traces := rules.Evaluate(rules.EligibilityInput{
			DistanceKm: v.DistanceKm, RadiusKm: ch.radius,
			BoxesRequested: v.TotalBoxes, BoxesBookedToday: bookedToday,
			DailyCapacity: ch.capacity, BatchNeedsCold: v.NeedsRefrigeration,
			HasColdVehicle: ch.cold, MatchingCategories: match,
			ReliabilityScore: ch.rel, CompletedCount: ch.completed,
			NoShowCount: ch.noShow, LateCount: ch.late,
			PlannedPickupAt:   v.PickupDeadline.Add(-30 * time.Minute),
			EstTransitMinutes: int(v.DistanceKm / 30.0 * 60),
			DonationDeadline:  v.DonationDeadline, Now: now,
		})
		v.EligibilityPreview = traces
		v.Blocked = blocked
		if !radiusOnly || !blocked {
			out = append(out, v)
		}
	}
	return out, nil
}

type ReserveInput struct {
	BatchID           string    `json:"batch_id"`
	CharityID         string    `json:"charity_id"`
	PlannedPickupAt   time.Time `json:"planned_pickup_at"`
	EstTransitMinutes int       `json:"est_transit_minutes"`
	DemandCategories  []string  `json:"demand_categories"`
}

// Reserve 公益机构预约批次：规则评估 + Redis 热点锁 + DB 唯一约束三重防重。
func (s *Service) Reserve(ctx context.Context, in ReserveInput, requestedBy string) (map[string]any, error) {
	bid, err := parseUUID(in.BatchID)
	if err != nil {
		return nil, apperror.New(400, "invalid_batch", "批次无效")
	}
	cid, err := parseUUID(in.CharityID)
	if err != nil {
		return nil, apperror.New(400, "invalid_charity", "公益机构无效")
	}
	if in.PlannedPickupAt.IsZero() || !in.PlannedPickupAt.After(time.Now()) {
		return nil, apperror.New(400, "invalid_pickup_time", "计划取货时间必须晚于当前时间")
	}
	if in.EstTransitMinutes <= 0 {
		in.EstTransitMinutes = 30
	}
	if in.DemandCategories == nil {
		in.DemandCategories = []string{}
	}

	release, err := s.locker.Acquire(ctx, "bakery:lock:batch:"+in.BatchID, 10*time.Second)
	if err != nil {
		return nil, apperror.New(429, "reservation_in_progress", "该批次正在被预约，请稍后重试")
	}
	defer release()

	tx, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	var (
		status, title    string
		storeID          string
		slng, slat       float64
		totalBoxes       int
		needsCold        bool
		donationDeadline time.Time
	)
	err = tx.QueryRow(ctx, `
		SELECT b.status, b.title, b.store_id::text, s.longitude, s.latitude,
		       b.total_boxes, b.needs_refrigeration, b.donation_deadline
		FROM donation_batches b JOIN stores s ON s.id=b.store_id
		WHERE b.id=$1 FOR UPDATE OF b`, bid).
		Scan(&status, &title, &storeID, &slng, &slat, &totalBoxes, &needsCold, &donationDeadline)
	if err == pgx.ErrNoRows {
		return nil, apperror.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if status != "open" {
		return nil, apperror.New(409, "batch_not_open",
			"批次当前不可预约（状态: "+status+"），同一批次同时仅允许一个有效预约")
	}
	if !donationDeadline.After(time.Now()) {
		return nil, apperror.New(422, "past_donation_window", "已超过该批次可捐赠时限")
	}

	var ch struct {
		lng, lat, radius, rel   float64
		capacity                int
		cold                    bool
		completed, noShow, late int
	}
	err = tx.QueryRow(ctx, `
		SELECT longitude, latitude, service_radius_km, daily_capacity_boxes,
		       has_cold_chain_vehicle, reliability_score,
		       completed_count, no_show_count, late_count
		FROM charities WHERE id=$1 AND status='active' FOR UPDATE`,
		cid).Scan(&ch.lng, &ch.lat, &ch.radius, &ch.capacity, &ch.cold,
		&ch.rel, &ch.completed, &ch.noShow, &ch.late)
	if err == pgx.ErrNoRows {
		return nil, apperror.New(404, "charity_not_found", "公益机构不存在或已停用")
	}
	if err != nil {
		return nil, err
	}

	// 当日运力占用
	var bookedToday int
	err = tx.QueryRow(ctx, `
		SELECT COALESCE(SUM(
		    (SELECT total_boxes FROM donation_batches b WHERE b.id=r.batch_id)),0)
		FROM reservations r
		WHERE r.charity_id=$1 AND r.status IN ('requested','confirmed')
		  AND r.requested_at::date = CURRENT_DATE`, cid).Scan(&bookedToday)
	if err != nil {
		return nil, err
	}

	// 品类需求匹配（以机构在档需求为准，提交的 demand_categories 仅作声明核对）
	catRows, err := tx.Query(ctx, `
		SELECT DISTINCT i.category FROM batch_items i WHERE i.batch_id=$1`, bid)
	if err != nil {
		return nil, err
	}
	batchCats := map[string]bool{}
	for catRows.Next() {
		var c string
		_ = catRows.Scan(&c)
		batchCats[c] = true
	}
	catRows.Close()
	needRows, err := tx.Query(ctx,
		`SELECT category FROM charity_needs WHERE charity_id=$1`, cid)
	if err != nil {
		return nil, err
	}
	match := 0
	for needRows.Next() {
		var c string
		_ = needRows.Scan(&c)
		if batchCats[c] {
			match++
		}
	}
	needRows.Close()

	distance := geo.DistanceKm(ch.lng, ch.lat, slng, slat)
	blocked, traces := rules.Evaluate(rules.EligibilityInput{
		DistanceKm: distance, RadiusKm: ch.radius,
		BoxesRequested: totalBoxes, BoxesBookedToday: bookedToday,
		DailyCapacity: ch.capacity, BatchNeedsCold: needsCold,
		HasColdVehicle: ch.cold, MatchingCategories: match,
		ReliabilityScore: ch.rel, CompletedCount: ch.completed,
		NoShowCount: ch.noShow, LateCount: ch.late,
		PlannedPickupAt: in.PlannedPickupAt, EstTransitMinutes: in.EstTransitMinutes,
		DonationDeadline: donationDeadline, Now: time.Now(),
	})
	if blocked {
		return nil, apperror.New(422, "eligibility_blocked",
			"不满足预约条件（公益效率/食品安全规则未通过）").WithDetails(traces)
	}

	snapshot := map[string]any{
		"distance_km":       float64(int(distance*100)) / 100,
		"service_radius_km": ch.radius, "daily_capacity_boxes": ch.capacity,
		"boxes_booked_today": bookedToday, "has_cold_vehicle": ch.cold,
		"matching_categories": match, "reliability_score": ch.rel,
	}
	resvCode := codePrefix("RSV")
	var resvID string
	err = tx.QueryRow(ctx, `
		INSERT INTO reservations
		    (code, batch_id, charity_id, planned_pickup_at, est_transit_minutes,
		     demand_categories, capability_snapshot, eligibility_trace,
		     status, requested_by)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'requested',$9) RETURNING id`,
		resvCode, bid, cid, in.PlannedPickupAt, in.EstTransitMinutes,
		in.DemandCategories, snapshot, traces, requestedBy).Scan(&resvID)
	if err != nil {
		if isUniqueViolation(err) {
			return nil, apperror.New(409, "already_reserved",
				"该批次已被预约，同一批次禁止重复预约")
		}
		return nil, err
	}
	if _, err := tx.Exec(ctx,
		`UPDATE donation_batches SET status='reserved' WHERE id=$1`, bid); err != nil {
		return nil, err
	}
	if err := emit(ctx, tx, bid, events.TypeReservationMade, map[string]any{
		"reservation_id": resvID, "reservation_code": resvCode,
		"charity_id": in.CharityID, "traces": traces,
	}); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		if isUniqueViolation(err) {
			return nil, apperror.New(409, "already_reserved", "该批次已被预约，同一批次禁止重复预约")
		}
		return nil, err
	}
	return map[string]any{
		"reservation_id": resvID, "reservation_code": resvCode,
		"batch_id": in.BatchID, "status": "requested",
		"eligibility_trace": traces,
	}, nil
}

// DecideReservation 门店确认或拒绝预约。确认后生成取货码、逐箱封签与待交接清单。
func (s *Service) DecideReservation(ctx context.Context, reservationID, decision, reason, operatorID string) (map[string]any, error) {
	rid, err := parseUUID(reservationID)
	if err != nil {
		return nil, apperror.New(400, "invalid_reservation", "预约无效")
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	var (
		bid, cid, reqBy string
		status          string
		batchStatus     string
		storeID         string
	)
	err = tx.QueryRow(ctx, `
		SELECT r.batch_id::text, r.charity_id::text, r.status,
		       r.requested_by::text, b.status, b.store_id::text
		FROM reservations r JOIN donation_batches b ON b.id=r.batch_id
		WHERE r.id=$1 FOR UPDATE OF r`, rid).
		Scan(&bid, &cid, &status, &reqBy, &batchStatus, &storeID)
	if err == pgx.ErrNoRows {
		return nil, apperror.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if status != "requested" {
		return nil, apperror.New(409, "invalid_status", "预约已处理，状态: "+status)
	}

	if decision == "reject" {
		if _, err := tx.Exec(ctx, `
			UPDATE reservations SET status='rejected', decided_at=now(),
			       decided_by=$2, reject_reason=$3 WHERE id=$1`,
			rid, operatorID, reason); err != nil {
			return nil, err
		}
		if _, err := tx.Exec(ctx,
			`UPDATE donation_batches SET status='open' WHERE id=$1 AND status='reserved'`, bid); err != nil {
			return nil, err
		}
		if err := emit(ctx, tx, mustUUID(bid), events.TypeReservationRejected,
			map[string]any{"reservation_id": reservationID, "reason": reason}); err != nil {
			return nil, err
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		return map[string]any{"status": "rejected", "batch_id": bid}, nil
	}
	if decision != "confirm" {
		return nil, apperror.New(400, "invalid_decision", "decision 仅支持 confirm/reject")
	}

	// 确认时再次校验可捐赠时限
	var deadline time.Time
	if err := tx.QueryRow(ctx,
		`SELECT donation_deadline FROM donation_batches WHERE id=$1`, bid).Scan(&deadline); err != nil {
		return nil, err
	}
	if !deadline.After(time.Now()) {
		return nil, apperror.New(422, "past_donation_window", "批次已超过可捐赠时限，不能确认；请拒绝预约")
	}

	pickupCode := digits(1000000)
	if _, err := tx.Exec(ctx, `
		UPDATE reservations SET status='confirmed', decided_at=now(), decided_by=$2,
		       pickup_code=$3 WHERE id=$1`, rid, operatorID, pickupCode); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE donation_batches SET status='confirmed', confirmed_at=now() WHERE id=$1`, bid); err != nil {
		return nil, err
	}

	// 逐箱启用封签
	seals := []map[string]any{}
	rows, err := tx.Query(ctx,
		`SELECT id, box_code FROM boxes WHERE batch_id=$1 ORDER BY item_id, seq_in_item`, bid)
	if err != nil {
		return nil, err
	}
	type box struct{ id, code string }
	var boxes []box
	for rows.Next() {
		var b box
		_ = rows.Scan(&b.id, &b.code)
		boxes = append(boxes, b)
	}
	rows.Close()
	for _, b := range boxes {
		sealNo := "SEAL-" + strings.ToUpper(codePrefixRaw(4))
		if _, err := tx.Exec(ctx, `UPDATE boxes SET seal_no=$2 WHERE id=$1`, b.id, sealNo); err != nil {
			return nil, err
		}
		seals = append(seals, map[string]any{"box_code": b.code, "seal_no": sealNo})
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO handovers(batch_id, reservation_id, status)
		VALUES ($1,$2,'pending')`, bid, rid); err != nil {
		return nil, err
	}

	if err := emit(ctx, tx, mustUUID(bid), events.TypeReservationConfirmed, map[string]any{
		"reservation_id": reservationID, "pickup_code": pickupCode,
		"box_count": len(boxes),
	}); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return map[string]any{
		"status": "confirmed", "batch_id": bid,
		"pickup_code": pickupCode, "handover_checklist": seals,
	}, nil
}

// CancelReservation 机构撤销预约，批次重新开放（可能被其他机构承接）。
func (s *Service) CancelReservation(ctx context.Context, reservationID, reason string) error {
	rid, err := parseUUID(reservationID)
	if err != nil {
		return apperror.New(400, "invalid_reservation", "预约无效")
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var bid, status string
	if err := tx.QueryRow(ctx, `SELECT batch_id::text, status FROM reservations WHERE id=$1 FOR UPDATE`,
		rid).Scan(&bid, &status); err != nil {
		if err == pgx.ErrNoRows {
			return apperror.ErrNotFound
		}
		return err
	}
	if status != "requested" && status != "confirmed" {
		return apperror.New(409, "invalid_status", "预约已终结，无法撤销: "+status)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE reservations SET status='cancelled', decided_at=now(), reject_reason=$2
		WHERE id=$1`, rid, reason); err != nil {
		return err
	}
	if status == "confirmed" {
		if err := resetForRebook(ctx, tx, mustUUID(bid)); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(ctx, `
		UPDATE donation_batches SET status='released', close_reason='机构撤销，库存重新开放'
		WHERE id=$1`, bid); err != nil {
		return err
	}
	if err := emit(ctx, tx, mustUUID(bid), events.TypeReservationCancelled,
		map[string]any{"reservation_id": reservationID, "reason": reason}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// ReleaseBatch 门店把 released 批次重新置为 open（可被再次预约）。
func (s *Service) ReleaseBatch(ctx context.Context, batchID string) error {
	id, err := parseUUID(batchID)
	if err != nil {
		return apperror.ErrNotFound
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var deadline time.Time
	var status string
	if err := tx.QueryRow(ctx, `SELECT status, donation_deadline FROM donation_batches WHERE id=$1`,
		id).Scan(&status, &deadline); err != nil {
		if err == pgx.ErrNoRows {
			return apperror.ErrNotFound
		}
		return err
	}
	if status != "released" {
		return apperror.New(409, "invalid_status", "仅 released 批次可重新开放: "+status)
	}
	if !deadline.After(time.Now()) {
		if _, err := tx.Exec(ctx,
			`UPDATE donation_batches SET status='expired', close_reason='重新开放时已过可捐赠时限' WHERE id=$1`, id); err != nil {
			return err
		}
		return apperror.New(422, "past_donation_window", "批次已过可捐赠时限，系统自动转 expired")
	}
	if _, err := tx.Exec(ctx,
		`UPDATE donation_batches SET status='open' WHERE id=$1`, id); err != nil {
		return err
	}
	if err := emit(ctx, tx, id, events.TypeBatchReleased,
		map[string]any{"batch_id": batchID}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
