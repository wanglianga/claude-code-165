package service

import (
	"context"
	"time"

	"bakery-donation/internal/apperror"
	"bakery-donation/internal/events"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type StopInput struct {
	PointName string    `json:"point_name"`
	PointType string    `json:"point_type"`
	Longitude *float64  `json:"longitude"`
	Latitude  *float64  `json:"latitude"`
	ETA       time.Time `json:"eta"`
	BoxCodes  []string  `json:"box_codes"`
}

type DeliveryPlanInput struct {
	BatchID      string      `json:"batch_id"`
	CharityID    string      `json:"charity_id"`
	VehiclePlate string      `json:"vehicle_plate"`
	DriverName   string      `json:"driver_name"`
	ColdChain    bool        `json:"cold_chain"`
	Stops        []StopInput `json:"stops"`
}

// UploadDeliveryPlan 公益机构签收后上传配送计划（车辆/司机/受助点/箱体分配）。
func (s *Service) UploadDeliveryPlan(ctx context.Context, in DeliveryPlanInput, operatorID string) (map[string]any, error) {
	id, err := parseUUID(in.BatchID)
	if err != nil {
		return nil, apperror.New(400, "invalid_batch", "批次无效")
	}
	if in.VehiclePlate == "" || in.DriverName == "" || len(in.Stops) == 0 {
		return nil, apperror.New(400, "invalid_plan", "车牌、司机与至少一个受助点必填")
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	var status string
	var ownerCharity string
	err = tx.QueryRow(ctx, `
		SELECT b.status, r.charity_id::text
		FROM donation_batches b
		JOIN reservations r ON r.batch_id=b.id AND r.status='confirmed'
		WHERE b.id=$1`, id).Scan(&status, &ownerCharity)
	if err == pgx.ErrNoRows {
		return nil, apperror.New(404, "not_in_transit", "批次不存在或尚未完成签收")
	}
	if err != nil {
		return nil, err
	}
	if ownerCharity != in.CharityID {
		return nil, apperror.ErrForbidden
	}
	if status != "in_transit" && status != "delivered" {
		return nil, apperror.New(409, "invalid_status", "仅运输中批次可上传/更新配送计划，当前: "+status)
	}
	// 冷链批次必须冷藏车
	var needsCold bool
	if err := tx.QueryRow(ctx, `SELECT needs_refrigeration FROM donation_batches WHERE id=$1`, id).
		Scan(&needsCold); err != nil {
		return nil, err
	}
	if needsCold && !in.ColdChain {
		return nil, apperror.New(422, "cold_chain_required", "本批次必须使用冷藏车辆配送")
	}

	// 箱号必须属于本批、已签收且分配不重复
	assigned := map[string]bool{}
	for i, st := range in.Stops {
		if st.PointName == "" || st.PointType == "" {
			return nil, apperror.New(400, "invalid_stop", "受助点名称与类型必填")
		}
		if !st.ETA.IsZero() {
			in.Stops[i].ETA = st.ETA.Truncate(time.Microsecond)
		}
		st = in.Stops[i]
		for _, bc := range st.BoxCodes {
			var owner string
			var scan string
			err := tx.QueryRow(ctx,
				`SELECT batch_id::text, scan_status FROM boxes WHERE box_code=$1`, bc).
				Scan(&owner, &scan)
			if err == pgx.ErrNoRows {
				return nil, apperror.New(404, "box_unknown", "箱号不存在: "+bc)
			}
			if err != nil {
				return nil, err
			}
			if owner != in.BatchID {
				return nil, apperror.New(422, "box_wrong_batch", "箱号不属于本批次: "+bc)
			}
			if scan != "scanned_ok" && scan != "delivered" && scan != "distributed" {
				return nil, apperror.New(422, "box_not_handed_over", "该箱未通过交接核对，不能配送: "+bc)
			}
			if assigned[bc] {
				return nil, apperror.New(422, "box_double_assigned", "同一箱被分配到多个受助点: "+bc)
			}
			assigned[bc] = true
		}
		in.Stops[i] = st
	}

	var planID string
	err = tx.QueryRow(ctx, `
		INSERT INTO delivery_plans(batch_id, charity_id, vehicle_plate, driver_name, cold_chain, uploaded_by)
		VALUES ($1,$2,$3,$4,$5,$6)
		ON CONFLICT (batch_id) DO UPDATE
		  SET vehicle_plate=EXCLUDED.vehicle_plate, driver_name=EXCLUDED.driver_name,
		      cold_chain=EXCLUDED.cold_chain, uploaded_by=EXCLUDED.uploaded_by, uploaded_at=now()
		RETURNING id`,
		id, in.CharityID, in.VehiclePlate, in.DriverName, in.ColdChain, operatorID).Scan(&planID)
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM delivery_stops WHERE plan_id=$1`, planID); err != nil {
		return nil, err
	}
	for i, st := range in.Stops {
		var lng, lat any
		if st.Longitude != nil {
			lng = *st.Longitude
		}
		if st.Latitude != nil {
			lat = *st.Latitude
		}
		var eta any
		if !st.ETA.IsZero() {
			eta = st.ETA
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO delivery_stops(plan_id, seq, point_name, point_type, longitude, latitude, eta, box_codes)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
			planID, i+1, st.PointName, st.PointType, lng, lat, eta, st.BoxCodes); err != nil {
			return nil, err
		}
	}

	payload := map[string]any{
		"vehicle_plate": in.VehiclePlate, "driver_name": in.DriverName,
		"cold_chain": in.ColdChain, "stops": in.Stops,
		"uploaded_at": time.Now().Format(time.RFC3339),
	}
	var batchCode string
	_ = tx.QueryRow(ctx, `SELECT code FROM donation_batches WHERE id=$1`, id).Scan(&batchCode)
	payload["batch_code"] = batchCode
	if err := s.appendVoucher(ctx, tx, id, "delivery", "配送凭证：车辆/司机/受助点与箱体分配", payload); err != nil {
		return nil, err
	}
	if err := emit(ctx, tx, id, events.TypeDeliveryPlan,
		map[string]any{"stops": len(in.Stops), "cold_chain": in.ColdChain}); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return map[string]any{"plan_id": planID, "stop_count": len(in.Stops),
		"assigned_boxes": len(assigned)}, nil
}

type DistributionInput struct {
	BatchID       string    `json:"batch_id"`
	CharityID     string    `json:"charity_id"`
	StopSeq       int       `json:"stop_seq"`
	PointName     string    `json:"point_name"`
	RecipientType string    `json:"recipient_type"`
	BoxCode       string    `json:"box_code"`
	Quantity      int       `json:"quantity"`
	DistributedAt time.Time `json:"distributed_at"`
}

// RecordDistribution 登记最终发放对象类型，逐箱闭环。
func (s *Service) RecordDistribution(ctx context.Context, in DistributionInput, operatorID string) (map[string]any, error) {
	id, err := parseUUID(in.BatchID)
	if err != nil {
		return nil, apperror.New(400, "invalid_batch", "批次无效")
	}
	if in.RecipientType == "" || in.PointName == "" || in.BoxCode == "" || in.Quantity <= 0 {
		return nil, apperror.New(400, "invalid_distribution",
			"受助点、发放对象类型、箱号与数量必填")
	}
	if in.DistributedAt.IsZero() {
		in.DistributedAt = time.Now()
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	var status, ownerCharity, ownerBatch, scanStatus string
	var stopID *string
	err = tx.QueryRow(ctx, `
		SELECT b.status, r.charity_id::text, bx.batch_id::text,
		       CASE WHEN bx.delivered_at IS NOT NULL THEN 'delivered' ELSE bx.scan_status END,
		       ds.id::text
		FROM boxes bx
		JOIN donation_batches b ON b.id=bx.batch_id
		JOIN reservations r ON r.batch_id=b.id AND r.status='confirmed'
		LEFT JOIN delivery_plans dp ON dp.batch_id=b.id
		LEFT JOIN delivery_stops ds ON ds.plan_id=dp.id AND $1 = ANY(ds.box_codes)
		WHERE bx.box_code=$1`, in.BoxCode).
		Scan(&status, &ownerCharity, &ownerBatch, &scanStatus, &stopID)
	if err == pgx.ErrNoRows {
		return nil, apperror.New(404, "box_unknown", "箱号不存在或无有效交接")
	}
	if err != nil {
		return nil, err
	}
	if ownerCharity != in.CharityID {
		return nil, apperror.ErrForbidden
	}
	if ownerBatch != in.BatchID {
		return nil, apperror.New(422, "box_wrong_batch", "箱号不属于本批次")
	}
	if scanStatus != "scanned_ok" && scanStatus != "delivered" {
		return nil, apperror.New(409, "box_not_handed_over", "该箱未通过交接核对，不能登记发放")
	}
	if stopID == nil {
		return nil, apperror.New(422, "stop_not_found",
			"该箱未出现在配送计划的任何受助点，请先更新配送计划")
	}

	var distID string
	if err := tx.QueryRow(ctx, `
		INSERT INTO distributions(batch_id, stop_id, recipient_type, point_name,
		       box_code, quantity, distributed_at, recorded_by)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING id`,
		id, stopID, in.RecipientType, in.PointName, in.BoxCode, in.Quantity,
		in.DistributedAt, operatorID).Scan(&distID); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `UPDATE boxes SET scan_status='distributed', distributed_at=$2 WHERE box_code=$1`,
		in.BoxCode, in.DistributedAt); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx,
		`UPDATE donation_batches SET status='delivered', delivered_at=COALESCE(delivered_at, now())
		 WHERE id=$1 AND status='in_transit'`, id); err != nil {
		return nil, err
	}

	// 全部已交接箱体均发放完成 -> distributed
	var remaining int
	if err := tx.QueryRow(ctx, `
		SELECT COUNT(*) FROM boxes
		WHERE batch_id=$1 AND scan_status IN ('scanned_ok','delivered')`, id).Scan(&remaining); err != nil {
		return nil, err
	}
	completed := remaining == 0
	if completed {
		if _, err := tx.Exec(ctx, `
			UPDATE donation_batches SET status='distributed', distributed_at=now() WHERE id=$1`, id); err != nil {
			return nil, err
		}
	}

	if err := s.writeDistributionVoucher(ctx, tx, id); err != nil {
		return nil, err
	}
	if err := emit(ctx, tx, id, events.TypeDistributed, map[string]any{
		"box_code": in.BoxCode, "recipient_type": in.RecipientType,
		"point_name": in.PointName, "batch_completed": completed,
	}); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return map[string]any{"distribution_id": distID, "batch_completed": completed,
		"recipient_type": in.RecipientType}, nil
}

// writeDistributionVoucher 用当前全部发放记录重写最新发放凭证（追加一节，便于逐次留痕）。
func (s *Service) writeDistributionVoucher(ctx context.Context, tx pgx.Tx, bid uuid.UUID) error {
	var batchCode string
	if err := tx.QueryRow(ctx, `SELECT code FROM donation_batches WHERE id=$1`, bid).Scan(&batchCode); err != nil {
		return err
	}
	rows, err := tx.Query(ctx, `
		SELECT recipient_type, point_name, box_code, quantity, distributed_at, feedback_status
		FROM distributions WHERE batch_id=$1 ORDER BY distributed_at`, bid)
	if err != nil {
		return err
	}
	type line struct {
		RecipientType string    `json:"recipient_type"`
		PointName     string    `json:"point_name"`
		BoxCode       string    `json:"box_code"`
		Quantity      int       `json:"quantity"`
		DistributedAt time.Time `json:"distributed_at"`
		Feedback      string    `json:"feedback_status"`
	}
	lines := []line{}
	for rows.Next() {
		var l line
		if err := rows.Scan(&l.RecipientType, &l.PointName, &l.BoxCode,
			&l.Quantity, &l.DistributedAt, &l.Feedback); err != nil {
			rows.Close()
			return err
		}
		lines = append(lines, l)
	}
	rows.Close()
	return s.appendVoucher(ctx, tx, bid, "distribution", "发放凭证：最终发放对象类型与受助点",
		map[string]any{
			"batch_code": batchCode, "records": lines,
			"generated_at": time.Now().Format(time.RFC3339),
		})
}

// SpoiledFeedback 受助点反馈变质：批次封存（quarantined），登记阻断异常，品牌风险留痕。
func (s *Service) SpoiledFeedback(ctx context.Context, batchID, boxCode, pointName, note, operatorID string) (map[string]any, error) {
	id, err := parseUUID(batchID)
	if err != nil {
		return nil, apperror.New(400, "invalid_batch", "批次无效")
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	var status string
	if err := tx.QueryRow(ctx, `SELECT status FROM donation_batches WHERE id=$1`, id).Scan(&status); err != nil {
		if err == pgx.ErrNoRows {
			return nil, apperror.ErrNotFound
		}
		return nil, err
	}
	if status == "quarantined" {
		return nil, apperror.New(409, "already_quarantined", "批次已封存")
	}
	if boxCode != "" {
		if _, err := tx.Exec(ctx, `
			UPDATE boxes SET scan_status='quarantined' WHERE box_code=$1 AND batch_id=$2`,
			boxCode, id); err != nil {
			return nil, err
		}
	}
	if _, err := tx.Exec(ctx, `
		UPDATE donation_batches SET status='quarantined', closed_at=now(),
		       close_reason='受助点反馈变质，封存待查'
		WHERE id=$1`, id); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO distributions(batch_id, recipient_type, point_name, box_code, quantity,
		      recorded_by, feedback_status, feedback_note)
		VALUES ($1,'(变质反馈)',$3,COALESCE(NULLIF($2,''),'(整批)'),0,$4,'spoiled',$5)`,
		id, boxCode, pointName, operatorID, note); err != nil {
		return nil, err
	}
	if err := s.raiseException(ctx, tx, &id, "recipient_spoiled",
		map[string]any{"box_code": boxCode, "point_name": pointName, "note": note}, true); err != nil {
		return nil, err
	}
	if err := s.appendVoucher(ctx, tx, id, "exception", "异常凭证：受助点变质反馈与批次封存",
		map[string]any{
			"box_code": boxCode, "point_name": pointName, "note": note,
			"at": time.Now().Format(time.RFC3339),
		}); err != nil {
		return nil, err
	}
	if err := emit(ctx, tx, id, events.TypeSpoiled,
		map[string]any{"box_code": boxCode, "point_name": pointName}); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return map[string]any{"status": "quarantined", "box_code": boxCode}, nil
}
