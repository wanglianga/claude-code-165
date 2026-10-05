package service

import (
	"context"
	"time"

	"bakery-donation/internal/apperror"
	"bakery-donation/internal/events"

	"github.com/jackc/pgx/v5"
)

// ---- DTO ----

type ItemInput struct {
	Category           string    `json:"category"`
	Name               string    `json:"name"`
	Quantity           int       `json:"quantity"`
	BoxCount           int       `json:"box_count"`
	ProductionAt       time.Time `json:"production_at"`
	ShelfLifeUntil     time.Time `json:"shelf_life_until"`
	Allergens          []string  `json:"allergens"`
	AllergenDeclared   *bool     `json:"allergen_declared"`
	NeedsRefrigeration bool      `json:"needs_refrigeration"`
	MaxStorageTempC    *float64  `json:"max_storage_temp_c"`
	PackageIntegrity   string    `json:"package_integrity"`
}

type ReportBatchInput struct {
	StoreID        string      `json:"store_id"`
	Title          string      `json:"title"`
	ReportType     string      `json:"report_type"` // scheduled / emergency
	PickupDeadline time.Time   `json:"pickup_deadline"`
	Items          []ItemInput `json:"items"`
}

// ReportBatch 门店闭店前上报可捐赠食品。
// 服务侧计算可捐赠时限、逐箱建档、生成来源凭证。
func (s *Service) ReportBatch(ctx context.Context, in ReportBatchInput, reportedBy string) (map[string]any, error) {
	if len(in.Items) == 0 {
		return nil, apperror.New(400, "invalid_items", "至少上报一个品类")
	}
	if in.PickupDeadline.IsZero() || !in.PickupDeadline.After(time.Now()) {
		return nil, apperror.New(400, "invalid_deadline", "最晚取走时间必须晚于当前时间")
	}
	if in.ReportType == "" {
		in.ReportType = "scheduled"
	}
	if in.ReportType != "scheduled" && in.ReportType != "emergency" {
		return nil, apperror.New(400, "invalid_report_type", "report_type 仅支持 scheduled/emergency")
	}

	tx, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	var storeName string
	err = tx.QueryRow(ctx, `SELECT name FROM stores WHERE id=$1 AND status='active'`, in.StoreID).
		Scan(&storeName)
	if err == pgx.ErrNoRows {
		return nil, apperror.New(404, "store_not_found", "门店不存在或已停用")
	}
	if err != nil {
		return nil, err
	}

	// 时间统一截断到微秒，与 PostgreSQL timestamptz 精度保持一致，保证凭证哈希可复现
	for i := range in.Items {
		in.Items[i].ProductionAt = in.Items[i].ProductionAt.Truncate(time.Microsecond)
		in.Items[i].ShelfLifeUntil = in.Items[i].ShelfLifeUntil.Truncate(time.Microsecond)
		if in.Items[i].Allergens == nil {
			in.Items[i].Allergens = []string{}
		}
	}
	in.PickupDeadline = in.PickupDeadline.Truncate(time.Microsecond)

	needsCold := false
	allergenComplete := true
	donationDeadline := in.PickupDeadline
	for i, it := range in.Items {
		if it.Quantity <= 0 || it.BoxCount <= 0 {
			return nil, apperror.New(400, "invalid_items", "品类数量与箱数必须大于 0")
		}
		if it.Quantity%it.BoxCount != 0 {
			return nil, apperror.New(400, "invalid_items", "品类数量必须能被箱数整除，便于逐箱核对")
		}
		if !it.ShelfLifeUntil.After(it.ProductionAt) {
			return nil, apperror.New(400, "invalid_items", "保质期必须晚于生产时间")
		}
		switch it.PackageIntegrity {
		case "intact", "repacked", "damaged":
		default:
			return nil, apperror.New(400, "invalid_items", "包装完整度仅支持 intact/repacked/damaged")
		}
		// 已过保质期或已到当日的不可上报
		if !it.ShelfLifeUntil.After(time.Now()) {
			return nil, apperror.New(400, "item_expired", "品类已过保质期，不可捐赠")
		}
		declared := true
		if it.AllergenDeclared != nil {
			declared = *it.AllergenDeclared
		}
		// 过敏原字段为空但声称已声明，视为缺失
		if declared && len(it.Allergens) == 0 {
			declared = false
		}
		in.Items[i].AllergenDeclared = &declared
		if !declared {
			allergenComplete = false
		}
		if it.NeedsRefrigeration {
			needsCold = true
		}
		// 可捐赠时限 = 保质期 - 安全缓冲（冷藏更严格）
		margin := s.cfg.AmbientSafetyMargin
		if it.NeedsRefrigeration {
			margin = s.cfg.ColdSafetyMargin
		}
		itemDeadline := it.ShelfLifeUntil.Add(-margin)
		if itemDeadline.Before(donationDeadline) {
			donationDeadline = itemDeadline
		}
	}
	if !donationDeadline.After(time.Now()) {
		return nil, apperror.New(400, "past_donation_window",
			"扣除食品安全缓冲后已无可捐赠时限，禁止上报捐赠")
	}

	batchCode := codePrefix("BAT")
	var batchID string
	totalBoxes, totalQty := 0, 0
	err = tx.QueryRow(ctx, `
		INSERT INTO donation_batches
		    (code, store_id, report_type, title, pickup_deadline, donation_deadline,
		     total_boxes, total_quantity, needs_refrigeration, allergen_complete,
		     status, reported_by)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'open',$11)
		RETURNING id`,
		batchCode, in.StoreID, in.ReportType, in.Title, in.PickupDeadline, donationDeadline,
		0, 0, needsCold, allergenComplete, reportedBy).Scan(&batchID)
	if err != nil {
		return nil, err
	}

	type itemRow struct {
		id     string
		perBox int
		input  ItemInput
	}
	var rows []itemRow
	for _, it := range in.Items {
		declared := it.AllergenDeclared != nil && *it.AllergenDeclared
		var maxTemp any
		if it.MaxStorageTempC != nil {
			maxTemp = *it.MaxStorageTempC
		}
		var id string
		err = tx.QueryRow(ctx, `
			INSERT INTO batch_items
			    (batch_id, category, name, quantity, box_count, production_at, shelf_life_until,
			     allergens, allergen_declared, needs_refrigeration, max_storage_temp_c, package_integrity)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) RETURNING id`,
			batchID, it.Category, it.Name, it.Quantity, it.BoxCount,
			it.ProductionAt, it.ShelfLifeUntil, it.Allergens, declared,
			it.NeedsRefrigeration, maxTemp, it.PackageIntegrity).Scan(&id)
		if err != nil {
			return nil, err
		}
		rows = append(rows, itemRow{id: id, perBox: it.Quantity / it.BoxCount, input: it})
		totalBoxes += it.BoxCount
		totalQty += it.Quantity
	}

	// 逐箱建档：箱号全局唯一，可扫码核对
	boxCodes := []map[string]any{}
	for _, r := range rows {
		for seq := 1; seq <= r.input.BoxCount; seq++ {
			boxCode := codePrefix("BOX")
			_, err = tx.Exec(ctx, `
				INSERT INTO boxes(box_code, batch_id, item_id, seq_in_item, expected_qty)
				VALUES ($1,$2,$3,$4,$5)`,
				boxCode, batchID, r.id, seq, r.perBox)
			if err != nil {
				return nil, err
			}
			boxCodes = append(boxCodes, map[string]any{
				"box_code": boxCode, "category": r.input.Category, "seq": seq,
			})
		}
	}

	_, err = tx.Exec(ctx,
		`UPDATE donation_batches SET total_boxes=$2, total_quantity=$3 WHERE id=$1`,
		batchID, totalBoxes, totalQty)
	if err != nil {
		return nil, err
	}

	// 来源凭证（哈希链第一节）
	sourcePayload := map[string]any{
		"batch_code": batchCode, "store_id": in.StoreID, "store_name": storeName,
		"title": in.Title, "report_type": in.ReportType,
		"pickup_deadline":   in.PickupDeadline,
		"donation_deadline": donationDeadline.Truncate(time.Microsecond),
		"total_boxes":       totalBoxes, "total_quantity": totalQty,
		"needs_refrigeration": needsCold, "allergen_complete": allergenComplete,
		"items": in.Items, "reported_by": reportedBy,
		"recorded_at": time.Now().Format(time.RFC3339),
	}
	if err := s.appendVoucher(ctx, tx, mustUUID(batchID), "source", "来源凭证：门店上报与品类/过敏原/时限", sourcePayload); err != nil {
		return nil, err
	}
	if !allergenComplete {
		if err := s.raiseException(ctx, tx, mustUUIDP(batchID), "allergen_missing",
			map[string]any{"batch_code": batchCode, "message": "存在过敏原信息缺失的品类，交接前必须补全"},
			false); err != nil {
			return nil, err
		}
	}
	if err := emit(ctx, tx, mustUUID(batchID), events.TypeBatchReported,
		map[string]any{"batch_code": batchCode, "store_id": in.StoreID,
			"total_boxes": totalBoxes, "report_type": in.ReportType}); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return map[string]any{
		"batch_id": batchID, "batch_code": batchCode,
		"donation_deadline": donationDeadline,
		"total_boxes":       totalBoxes, "total_quantity": totalQty,
		"needs_refrigeration": needsCold, "allergen_complete": allergenComplete,
		"boxes": boxCodes,
	}, nil
}

// MarkSoldOut 门店临时售罄：关闭批次；如已有有效预约则同步撤销。
func (s *Service) MarkSoldOut(ctx context.Context, batchID, operatorID, note string) error {
	id, err := parseUUID(batchID)
	if err != nil {
		return apperror.ErrNotFound
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var status string
	var storeID string
	if err := tx.QueryRow(ctx, `SELECT status, store_id::text FROM donation_batches WHERE id=$1`, id).
		Scan(&status, &storeID); err != nil {
		if err == pgx.ErrNoRows {
			return apperror.ErrNotFound
		}
		return err
	}
	switch status {
	case "open", "reserved":
	case "confirmed":
		return apperror.New(409, "already_confirmed",
			"批次已确认且交接在即，售罄请直接联系机构并改用异常流程")
	default:
		return apperror.New(409, "invalid_status", "当前批次状态不允许标记售罄: "+status)
	}

	// 撤销有效预约
	_, err = tx.Exec(ctx, `
		UPDATE reservations SET status='cancelled', decided_at=now(),
		       reject_reason='门店临时售罄，预约自动撤销'
		WHERE batch_id=$1 AND status IN ('requested','confirmed')`, id)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		UPDATE donation_batches SET status='sold_out', closed_at=now(),
		       close_reason='临时售罄'
		WHERE id=$1`, id)
	if err != nil {
		return err
	}
	if err := s.raiseException(ctx, tx, &id, "sold_out",
		map[string]any{"note": note, "operator": operatorID}, false); err != nil {
		return err
	}
	if err := emit(ctx, tx, id, events.TypeBatchSoldOut,
		map[string]any{"note": note}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// CancelBatch 门店在无预约时撤回批次。
func (s *Service) CancelBatch(ctx context.Context, batchID, reason string) error {
	id, err := parseUUID(batchID)
	if err != nil {
		return apperror.ErrNotFound
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	ct, err := tx.Exec(ctx, `
		UPDATE donation_batches SET status='cancelled', closed_at=now(), close_reason=$2
		WHERE id=$1 AND status='open'`, id, reason)
	if err != nil {
		return err
	}
	if ct.RowsAffected() == 0 {
		return apperror.New(409, "invalid_status", "仅 open 状态批次可撤回（可能已被预约）")
	}
	if err := emit(ctx, tx, id, events.TypeBatchCancelled, map[string]any{"reason": reason}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
