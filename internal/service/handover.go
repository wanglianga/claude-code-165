package service

import (
	"context"
	"time"

	"bakery-donation/internal/apperror"
	"bakery-donation/internal/events"
	"bakery-donation/internal/rules"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// ArrivePickup 冷链车到达：登记到达时间，判定迟到并累计机构迟到记录。
func (s *Service) ArrivePickup(ctx context.Context, reservationID string, arrivedAt *time.Time) (map[string]any, error) {
	rid, err := parseUUID(reservationID)
	if err != nil {
		return nil, apperror.New(400, "invalid_reservation", "预约无效")
	}
	arrival := time.Now()
	if arrivedAt != nil {
		arrival = *arrivedAt
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	var bid, cid, status string
	var planned time.Time
	var arrived *time.Time
	err = tx.QueryRow(ctx, `
		SELECT r.batch_id::text, r.charity_id::text, r.status, r.planned_pickup_at, r.arrived_at
		FROM reservations r WHERE r.id=$1 FOR UPDATE`, rid).
		Scan(&bid, &cid, &status, &planned, &arrived)
	if err == pgx.ErrNoRows {
		return nil, apperror.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if status != "confirmed" {
		return nil, apperror.New(409, "invalid_status", "仅 confirmed 预约可登记到达，当前: "+status)
	}
	if arrived != nil {
		return nil, apperror.New(409, "already_arrived", "到达已登记，请勿重复")
	}

	lateMin := int(arrival.Sub(planned).Minutes())
	late := false
	if lateMin < 0 {
		lateMin = 0 // 提前到达不计迟到
	}
	if lateMin > s.cfg.LateToleranceMin {
		late = true
		if _, err := tx.Exec(ctx, `
			UPDATE reservations SET arrived_at=$2, late_minutes=$3 WHERE id=$1`,
			rid, arrival, lateMin); err != nil {
			return nil, err
		}
		if err := s.raiseException(ctx, tx, mustUUIDP(bid), "cold_truck_late",
			map[string]any{
				"planned_at": planned, "arrived_at": arrival,
				"late_minutes": lateMin, "tolerance_minutes": s.cfg.LateToleranceMin,
			}, false); err != nil {
			return nil, err
		}
		if _, err := tx.Exec(ctx,
			`UPDATE charities SET late_count = late_count + 1 WHERE id=$1`, cid); err != nil {
			return nil, err
		}
	} else {
		if _, err := tx.Exec(ctx, `
			UPDATE reservations SET arrived_at=$2, late_minutes=GREATEST($3,0) WHERE id=$1`,
			rid, arrival, lateMin); err != nil {
			return nil, err
		}
	}
	if err := s.recalcReliability(ctx, tx, cid); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx,
		`UPDATE donation_batches SET status='in_handover' WHERE id=$1 AND status='confirmed'`, bid); err != nil {
		return nil, err
	}
	if err := emit(ctx, tx, mustUUID(bid), events.TypeTruckLate,
		map[string]any{"late": late, "late_minutes": lateMin}); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return map[string]any{"arrived_at": arrival, "late": late, "late_minutes": lateMin}, nil
}

func (s *Service) recalcReliability(ctx context.Context, tx pgx.Tx, charityID string) error {
	var completed, noShow, late int
	if err := tx.QueryRow(ctx, `
		SELECT completed_count, no_show_count, late_count FROM charities WHERE id=$1`,
		charityID).Scan(&completed, &noShow, &late); err != nil {
		return err
	}
	score := rules.Reliability(completed, noShow, late)
	_, err := tx.Exec(ctx, `UPDATE charities SET reliability_score=$2 WHERE id=$1`, charityID, score)
	return err
}

// ReceiverVerifyInput 接收人身份核对
type ReceiverVerifyInput struct {
	PickupCode    string `json:"pickup_code"`
	ReceiverName  string `json:"receiver_name"`
	ReceiverID    string `json:"receiver_id"`
	ReceiverPhone string `json:"receiver_phone"`
}

// VerifyReceiver 门店核对接收人身份与取货码。
func (s *Service) VerifyReceiver(ctx context.Context, batchID string, in ReceiverVerifyInput) (map[string]any, error) {
	id, err := parseUUID(batchID)
	if err != nil {
		return nil, apperror.New(400, "invalid_batch", "批次无效")
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	var code, status string
	err = tx.QueryRow(ctx, `
		SELECT r.pickup_code, b.status
		FROM handovers h
		JOIN reservations r ON r.id = h.reservation_id
		JOIN donation_batches b ON b.id = h.batch_id
		WHERE h.batch_id=$1 FOR UPDATE`, id).Scan(&code, &status)
	if err == pgx.ErrNoRows {
		return nil, apperror.New(404, "handover_not_found", "待交接清单不存在（预约可能未确认）")
	}
	if err != nil {
		return nil, err
	}
	if code == "" || in.PickupCode != code {
		return nil, apperror.New(403, "pickup_code_mismatch", "取货身份码不匹配，拒绝交接")
	}
	if in.ReceiverName == "" || in.ReceiverID == "" {
		return nil, apperror.New(400, "receiver_required", "接收人姓名与证件必填")
	}
	_, err = tx.Exec(ctx, `
		UPDATE handovers SET receiver_name=$2, receiver_id_masked=$3, receiver_phone_mask=$4
		WHERE batch_id=$1`,
		id, in.ReceiverName, maskID(in.ReceiverID), maskPhone(in.ReceiverPhone))
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return map[string]any{"identity_verified": true,
		"receiver": in.ReceiverName, "receiver_id_masked": maskID(in.ReceiverID)}, nil
}

type ScanBoxInput struct {
	BoxCode       string   `json:"box_code"`
	MeasuredTempC *float64 `json:"measured_temp_c"`
	SealNo        string   `json:"seal_no"`
	CheckedQty    int      `json:"checked_qty"`
}

// ScanBox 门店员工取货时逐箱扫码核对：箱号归属、温度、封签、数量。
func (s *Service) ScanBox(ctx context.Context, batchID, operatorID string, in ScanBoxInput) (map[string]any, error) {
	id, err := parseUUID(batchID)
	if err != nil {
		return nil, apperror.New(400, "invalid_batch", "批次无效")
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	var bStatus string
	if err := tx.QueryRow(ctx, `SELECT status FROM donation_batches WHERE id=$1`, id).Scan(&bStatus); err != nil {
		if err == pgx.ErrNoRows {
			return nil, apperror.ErrNotFound
		}
		return nil, err
	}
	if bStatus != "in_handover" && bStatus != "confirmed" {
		return nil, apperror.New(409, "invalid_status", "批次不在交接环节: "+bStatus)
	}

	// 箱号归属校验（错扫）
	var (
		boxID, actualBatch string
		scanStatus, sealNo *string
		expectedQty        int
		itemID             string
	)
	err = tx.QueryRow(ctx, `
		SELECT b.id::text, b.batch_id::text, b.scan_status, b.seal_no, b.expected_qty, b.item_id::text
		FROM boxes b WHERE b.box_code=$1 FOR UPDATE`, in.BoxCode).
		Scan(&boxID, &actualBatch, &scanStatus, &sealNo, &expectedQty, &itemID)
	if err == pgx.ErrNoRows {
		// 错扫未知箱：操作告警并留痕，但不形成对本批交接的永久阻断
		if err := s.raiseException(ctx, tx, &id, "wrong_scan",
			map[string]any{"scanned_code": in.BoxCode, "reason": "系统中不存在该箱号"}, false); err != nil {
			return nil, err
		}
		return nil, apperror.New(404, "box_unknown", "箱号不存在，请核对后重扫")
	}
	if err != nil {
		return nil, err
	}
	if actualBatch != batchID {
		if err := s.raiseException(ctx, tx, &id, "wrong_scan",
			map[string]any{"scanned_code": in.BoxCode, "belongs_to_batch": actualBatch,
				"reason": "箱号不属于本批次"}, false); err != nil {
			return nil, err
		}
		return nil, apperror.New(422, "box_wrong_batch", "箱号不属于本批次，禁止装车")
	}
	if *scanStatus != "pending" {
		return nil, apperror.New(409, "box_already_scanned", "该箱已核对，状态: "+*scanStatus)
	}

	rejectReasons := []string{}

	// 数量
	if in.CheckedQty <= 0 || in.CheckedQty != expectedQty {
		rejectReasons = append(rejectReasons, "数量不符")
	}
	// 封签
	if sealNo == nil || *sealNo == "" || in.SealNo != *sealNo {
		rejectReasons = append(rejectReasons, "封签号不符或封签缺失")
	}
	// 温度（冷藏品）
	var needsCold bool
	var limitTemp *float64
	var allergenDeclared bool
	err = tx.QueryRow(ctx, `
		SELECT i.needs_refrigeration, i.max_storage_temp_c, i.allergen_declared
		FROM batch_items i WHERE i.id=$1`, itemID).
		Scan(&needsCold, &limitTemp, &allergenDeclared)
	if err != nil {
		return nil, err
	}
	tempCompliant := true
	if needsCold {
		if in.MeasuredTempC == nil {
			tempCompliant = false
			rejectReasons = append(rejectReasons, "冷藏箱缺少温度记录")
		} else if limitTemp != nil && *in.MeasuredTempC > *limitTemp {
			tempCompliant = false
			rejectReasons = append(rejectReasons, "温度超过冷藏上限")
		}
	}

	if in.MeasuredTempC != nil {
		var lim any
		if limitTemp != nil {
			lim = *limitTemp
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO temperature_readings
			    (batch_id, box_id, phase, temp_c, limit_temp_c, compliant, recorded_by, note)
			VALUES ($1,$2,'handover',$3,$4,$5,$6,$7)`,
			id, boxID, *in.MeasuredTempC, lim, tempCompliant, operatorID,
			map[bool]string{true: "", false: "交接温度异常"}[tempCompliant]); err != nil {
			return nil, err
		}
	}
	if !tempCompliant {
		if err := s.raiseException(ctx, tx, &id, "temp_breach",
			map[string]any{"box_code": in.BoxCode, "measured_temp_c": in.MeasuredTempC,
				"limit_temp_c": limitTemp}, true); err != nil {
			return nil, err
		}
	}
	if !allergenDeclared {
		if err := s.raiseException(ctx, tx, &id, "allergen_missing",
			map[string]any{"box_code": in.BoxCode, "reason": "该箱品类过敏原信息缺失，禁止出库"}, true); err != nil {
			return nil, err
		}
		rejectReasons = append(rejectReasons, "过敏原信息缺失")
	}

	newStatus := "scanned_ok"
	rejectNote := ""
	if len(rejectReasons) > 0 {
		newStatus = "rejected"
		rejectNote = joinReasons(rejectReasons)
		// 拒收箱留店处理、不进入公益流：异常留痕但立即关闭，不再阻断交接
		if err := resolveExceptions(ctx, tx, id,
			[]string{"temp_breach", "allergen_missing"},
			"该箱拒收并留店处理，不随批出库"); err != nil {
			return nil, err
		}
	}

	var tempVal any
	if in.MeasuredTempC != nil {
		tempVal = *in.MeasuredTempC
	}
	_, err = tx.Exec(ctx, `
		UPDATE boxes SET scan_status=$2, scanned_at=now(), measured_temp_c=$3,
		       seal_ok=$4, checked_qty=$5, reject_reason=$6
		WHERE id=$1`,
		boxID, newStatus, tempVal, len(rejectReasons) == 0, in.CheckedQty, rejectNote)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return map[string]any{
		"box_code": in.BoxCode, "result": newStatus,
		"expected_qty": expectedQty, "reject_reasons": rejectReasons,
	}, nil
}

func contains(xs []string, v string) bool {
	for _, x := range xs {
		if x == v {
			return true
		}
	}
	return false
}

func joinReasons(rs []string) string {
	out := ""
	for i, r := range rs {
		if i > 0 {
			out += "；"
		}
		out += r
	}
	return out
}

// RecordTemp 登记运输/受助点环节温度（冷链凭证）。
func (s *Service) RecordTemp(ctx context.Context, batchID string, phase string, tempC float64, boxCode, note, operatorID string) error {
	id, err := parseUUID(batchID)
	if err != nil {
		return apperror.New(400, "invalid_batch", "批次无效")
	}
	if phase != "transport" && phase != "recipient" && phase != "handover" {
		return apperror.New(400, "invalid_phase", "phase 仅支持 handover/transport/recipient")
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var boxID any
	var limit *float64
	if boxCode != "" {
		var bid string
		var lim *float64
		err := tx.QueryRow(ctx, `
			SELECT b.id::text, i.max_storage_temp_c
			FROM boxes b JOIN batch_items i ON i.id=b.item_id
			WHERE b.box_code=$1 AND b.batch_id=$2`, boxCode, id).Scan(&bid, &lim)
		if err == pgx.ErrNoRows {
			return apperror.New(404, "box_not_found", "箱号不存在或不属于本批次")
		}
		if err != nil {
			return err
		}
		boxID, limit = bid, lim
	}
	compliant := true
	if limit != nil && tempC > *limit {
		compliant = false
		if err := s.raiseException(ctx, tx, &id, "temp_breach",
			map[string]any{"phase": phase, "box_code": boxCode, "temp_c": tempC, "limit": limit},
			phase == "recipient"); err != nil {
			return err
		}
	}
	var limVal any
	if limit != nil {
		limVal = *limit
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO temperature_readings(batch_id, box_id, phase, temp_c, limit_temp_c, compliant, recorded_by, note)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
		id, boxID, phase, tempC, limVal, compliant, operatorID, note); err != nil {
		return err
	}
	// 运输/受助点温度作为温控凭证链的追加节
	if phase != "handover" {
		var bStatus string
		if err := tx.QueryRow(ctx, `SELECT status FROM donation_batches WHERE id=$1`, id).Scan(&bStatus); err != nil {
			return err
		}
		if bStatus == "in_transit" || bStatus == "delivered" {
			if err := s.appendVoucher(ctx, tx, id, "temperature",
				"温控凭证："+map[string]string{"transport": "运输途中测温", "recipient": "受助点到货测温"}[phase],
				map[string]any{
					"phase": phase, "box_code": boxCode, "temp_c": tempC,
					"limit_temp_c": limit, "compliant": compliant, "note": note,
					"recorded_at": time.Now().Format(time.RFC3339),
				}); err != nil {
				return err
			}
		}
	}
	return tx.Commit(ctx)
}

// CompleteHandoverCheck 门店完成交接核对：要求身份已核验、无阻断性异常、箱体全部有结论。
func (s *Service) CompleteHandoverCheck(ctx context.Context, batchID, operatorID string) (map[string]any, error) {
	id, err := parseUUID(batchID)
	if err != nil {
		return nil, apperror.New(400, "invalid_batch", "批次无效")
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	var hID, hStatus, receiver string
	err = tx.QueryRow(ctx, `SELECT id::text, status, COALESCE(receiver_name,'')
		FROM handovers WHERE batch_id=$1 FOR UPDATE`, id).Scan(&hID, &hStatus, &receiver)
	if err == pgx.ErrNoRows {
		return nil, apperror.New(404, "handover_not_found", "待交接清单不存在")
	}
	if err != nil {
		return nil, err
	}
	if hStatus != "pending" {
		return nil, apperror.New(409, "invalid_status", "交接核对已完成")
	}
	if receiver == "" {
		return nil, apperror.New(422, "receiver_unverified", "尚未核对接收人身份，不能完成核对")
	}
	var pending, rejected, ok int
	if err := tx.QueryRow(ctx, `
		SELECT COUNT(*) FILTER (WHERE scan_status='pending'),
		       COUNT(*) FILTER (WHERE scan_status='rejected'),
		       COUNT(*) FILTER (WHERE scan_status='scanned_ok')
		FROM boxes WHERE batch_id=$1`, id).Scan(&pending, &rejected, &ok); err != nil {
		return nil, err
	}
	if pending > 0 {
		return nil, apperror.New(422, "boxes_pending", "仍有箱未完成扫码核对").
			WithDetails(map[string]any{"pending_boxes": pending})
	}
	var blocking int
	if err := tx.QueryRow(ctx, `
		SELECT COUNT(*) FROM exception_records
		WHERE batch_id=$1 AND blocking AND NOT resolved`, id).Scan(&blocking); err != nil {
		return nil, err
	}
	if blocking > 0 {
		return nil, apperror.New(422, "blocking_exceptions",
			"存在未解决的阻断性异常（温控/封签/错扫/过敏原），禁止交接").
			WithDetails(map[string]any{"blocking": blocking, "rejected_boxes": rejected})
	}
	if ok == 0 {
		return nil, apperror.New(422, "no_valid_boxes", "没有任何通过核对的箱体，无法交接")
	}
	if _, err := tx.Exec(ctx,
		`UPDATE handovers SET status='checked', store_confirmed_by=$2 WHERE id=$1`,
		hID, operatorID); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return map[string]any{"status": "checked", "scanned_ok_boxes": ok, "rejected_boxes": rejected}, nil
}

// SignHandover 双方签收；签收后批次进入运输中，生成交接凭证与温控凭证。
func (s *Service) SignHandover(ctx context.Context, batchID, party, operatorID string) error {
	id, err := parseUUID(batchID)
	if err != nil {
		return apperror.New(400, "invalid_batch", "批次无效")
	}
	if party != "store" && party != "charity" {
		return apperror.New(400, "invalid_party", "party 仅支持 store/charity")
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var hStatus string
	if err := tx.QueryRow(ctx, `SELECT status FROM handovers WHERE batch_id=$1 FOR UPDATE`,
		id).Scan(&hStatus); err != nil {
		if err == pgx.ErrNoRows {
			return apperror.ErrNotFound
		}
		return err
	}
	if hStatus == "pending" {
		return apperror.New(422, "check_incomplete", "请先完成逐箱核对")
	}
	if hStatus == "signed" {
		return apperror.New(409, "already_signed", "双方已完成签收")
	}

	if party == "store" {
		if _, err := tx.Exec(ctx,
			`UPDATE handovers SET store_signed_at=now() WHERE batch_id=$1`, id); err != nil {
			return err
		}
	} else {
		// 公益机构签收：要求门店已签
		var storeSigned bool
		if err := tx.QueryRow(ctx,
			`SELECT store_signed_at IS NOT NULL FROM handovers WHERE batch_id=$1`, id).Scan(&storeSigned); err != nil {
			return err
		}
		if !storeSigned {
			return apperror.New(422, "store_not_signed", "需门店先签收，公益机构方可签收")
		}
		if _, err := tx.Exec(ctx, `
			UPDATE handovers SET charity_signed_at=now(), status='signed', completed_at=now()
			WHERE batch_id=$1`, id); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			UPDATE donation_batches SET status='in_transit', handed_over_at=now() WHERE id=$1`, id); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`UPDATE boxes SET delivered_at=now() WHERE batch_id=$1 AND scan_status='scanned_ok'`,
			id); err != nil {
			return err
		}
		// 机构完成一次履约
		var cid string
		if err := tx.QueryRow(ctx, `
			UPDATE charities SET completed_count = completed_count + 1
			WHERE id=(SELECT charity_id FROM reservations WHERE batch_id=$1 AND status='confirmed')
			RETURNING id::text`, id).Scan(&cid); err != nil {
			return err
		}
		if err := s.recalcReliability(ctx, tx, cid); err != nil {
			return err
		}
		if err := s.writeHandoverVouchers(ctx, tx, id); err != nil {
			return err
		}
		if err := emit(ctx, tx, id, events.TypeHandoverSigned,
			map[string]any{"handed_over_at": time.Now()}); err != nil {
			return err
		}
	}
	if party == "store" {
		if err := emit(ctx, tx, id, events.TypeHandoverChecked, map[string]any{"party": "store"}); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// writeHandoverVouchers 生成交接凭证与温控凭证。
func (s *Service) writeHandoverVouchers(ctx context.Context, tx pgx.Tx, bid uuid.UUID) error {
	var h struct {
		receiver, idMask, phoneMask string
		storeAt, charityAt          *time.Time
		ok, rejected                int
	}
	err := tx.QueryRow(ctx, `
		SELECT COALESCE(receiver_name,''), COALESCE(receiver_id_masked,''),
		       COALESCE(receiver_phone_mask,''), store_signed_at, charity_signed_at
		FROM handovers WHERE batch_id=$1`, bid).
		Scan(&h.receiver, &h.idMask, &h.phoneMask, &h.storeAt, &h.charityAt)
	if err != nil {
		return err
	}
	if err := tx.QueryRow(ctx, `
		SELECT COUNT(*) FILTER (WHERE scan_status='scanned_ok'),
		       COUNT(*) FILTER (WHERE scan_status='rejected')
		FROM boxes WHERE batch_id=$1`, bid).Scan(&h.ok, &h.rejected); err != nil {
		return err
	}
	var batchCode string
	if err := tx.QueryRow(ctx, `SELECT code FROM donation_batches WHERE id=$1`, bid).Scan(&batchCode); err != nil {
		return err
	}
	rows, err := tx.Query(ctx, `
		SELECT box_code, COALESCE(seal_no,''), measured_temp_c, checked_qty, COALESCE(reject_reason,'')
		FROM boxes WHERE batch_id=$1 ORDER BY box_code`, bid)
	if err != nil {
		return err
	}
	type boxLine struct {
		Code   string   `json:"box_code"`
		Seal   string   `json:"seal_no"`
		Temp   *float64 `json:"measured_temp_c"`
		Qty    int      `json:"checked_qty"`
		Reason string   `json:"reject_reason,omitempty"`
	}
	lines := []boxLine{}
	for rows.Next() {
		var l boxLine
		if err := rows.Scan(&l.Code, &l.Seal, &l.Temp, &l.Qty, &l.Reason); err != nil {
			rows.Close()
			return err
		}
		lines = append(lines, l)
	}
	rows.Close()

	handoverPayload := map[string]any{
		"batch_code": batchCode, "receiver_name": h.receiver,
		"receiver_id_masked": h.idMask, "receiver_phone_masked": h.phoneMask,
		"store_signed_at": h.storeAt, "charity_signed_at": h.charityAt,
		"scanned_ok_boxes": h.ok, "rejected_boxes": h.rejected,
		"boxes": lines, "signed_at": time.Now().Format(time.RFC3339),
	}
	if err := s.appendVoucher(ctx, tx, bid, "handover", "交接凭证：身份/封签/数量核对与双方签收", handoverPayload); err != nil {
		return err
	}

	// 温控凭证：汇总交接阶段全部温度读数
	trows, err := tx.Query(ctx, `
		SELECT phase, temp_c, COALESCE(limit_temp_c, 0), compliant, recorded_at
		FROM temperature_readings WHERE batch_id=$1 ORDER BY recorded_at`, bid)
	if err != nil {
		return err
	}
	type tempLine struct {
		Phase     string    `json:"phase"`
		Temp      float64   `json:"temp_c"`
		Limit     float64   `json:"limit_temp_c"`
		Compliant bool      `json:"compliant"`
		At        time.Time `json:"recorded_at"`
	}
	temps := []tempLine{}
	allCompliant := true
	for trows.Next() {
		var t tempLine
		if err := trows.Scan(&t.Phase, &t.Temp, &t.Limit, &t.Compliant, &t.At); err != nil {
			trows.Close()
			return err
		}
		if !t.Compliant {
			allCompliant = false
		}
		temps = append(temps, t)
	}
	trows.Close()
	tempPayload := map[string]any{
		"batch_code": batchCode, "all_compliant": allCompliant,
		"readings": temps, "generated_at": time.Now().Format(time.RFC3339),
	}
	return s.appendVoucher(ctx, tx, bid, "temperature", "温控凭证：交接/运输/受助点温度链", tempPayload)
}
