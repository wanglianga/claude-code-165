package service

import (
	"context"
	"time"

	"bakery-donation/internal/apperror"
)

type LedgerRow struct {
	BatchID          string     `json:"batch_id"`
	BatchCode        string     `json:"batch_code"`
	StoreName        string     `json:"store_name"`
	CharityName      *string    `json:"charity_name"`
	Title            string     `json:"title"`
	Categories       string     `json:"categories"`
	TotalQuantity    int        `json:"total_quantity"`
	TotalBoxes       int        `json:"total_boxes"`
	ReportType       string     `json:"report_type"`
	Status           string     `json:"status"`
	Result           string     `json:"result"`
	ReportedAt       time.Time  `json:"reported_at"`
	HandedOverAt     *time.Time `json:"handed_over_at"`
	DistributedAt    *time.Time `json:"distributed_at"`
	VouchersComplete bool       `json:"vouchers_complete"`
}

// ListLedger 捐赠公示：向公众/品牌展示批次去向结果。
func (s *Service) ListLedger(ctx context.Context, resultFilter string, limit int) ([]LedgerRow, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	q := `
		SELECT batch_id::text, batch_code, store_name, charity_name, title, categories,
		       total_quantity, total_boxes, report_type, status, result,
		       reported_at, handed_over_at, distributed_at, vouchers_complete
		FROM public_ledger`
	args := []any{}
	if resultFilter != "" {
		q += ` WHERE result=$1`
		args = append(args, resultFilter)
	}
	q += ` ORDER BY reported_at DESC LIMIT ` + itoa(limit)
	rows, err := s.db.Pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []LedgerRow{}
	for rows.Next() {
		var r LedgerRow
		if err := rows.Scan(&r.BatchID, &r.BatchCode, &r.StoreName, &r.CharityName,
			&r.Title, &r.Categories, &r.TotalQuantity, &r.TotalBoxes, &r.ReportType,
			&r.Status, &r.Result, &r.ReportedAt, &r.HandedOverAt, &r.DistributedAt,
			&r.VouchersComplete); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, nil
}

// BatchDetail 批次全链路详情。
func (s *Service) BatchDetail(ctx context.Context, batchID string) (map[string]any, error) {
	id, err := parseUUID(batchID)
	if err != nil {
		return nil, apperror.New(400, "invalid_batch", "批次无效")
	}
	out := map[string]any{}

	var b struct {
		Code, Title, Status, ReportType, StoreName, StoreAddress string    `json:"-"`
		Pickup, Donation                                         time.Time `json:"-"`
		Boxes, Qty                                               int       `json:"-"`
		Cold, AllergenOK                                         bool      `json:"-"`
		ReportedAt                                               time.Time `json:"-"`
	}
	err = s.db.Pool.QueryRow(ctx, `
		SELECT b.code, b.title, b.status, b.report_type, s.name, s.address,
		       b.pickup_deadline, b.donation_deadline, b.total_boxes, b.total_quantity,
		       b.needs_refrigeration, b.allergen_complete, b.created_at
		FROM donation_batches b JOIN stores s ON s.id=b.store_id
		WHERE b.id=$1`, id).
		Scan(&b.Code, &b.Title, &b.Status, &b.ReportType, &b.StoreName, &b.StoreAddress,
			&b.Pickup, &b.Donation, &b.Boxes, &b.Qty, &b.Cold, &b.AllergenOK, &b.ReportedAt)
	if err != nil {
		return nil, apperror.ErrNotFound
	}
	out["batch"] = map[string]any{
		"code": b.Code, "title": b.Title, "status": b.Status,
		"report_type": b.ReportType, "store_name": b.StoreName,
		"store_address": b.StoreAddress, "pickup_deadline": b.Pickup,
		"donation_deadline": b.Donation, "total_boxes": b.Boxes,
		"total_quantity": b.Qty, "needs_refrigeration": b.Cold,
		"allergen_complete": b.AllergenOK, "reported_at": b.ReportedAt,
	}

	// 品类
	rows, _ := s.db.Pool.Query(ctx, `
		SELECT category, name, quantity, box_count, production_at, shelf_life_until,
		       allergens, allergen_declared, needs_refrigeration, max_storage_temp_c, package_integrity
		FROM batch_items WHERE batch_id=$1`, id)
	items := []map[string]any{}
	if rows != nil {
		for rows.Next() {
			var it map[string]any = map[string]any{}
			var cat, name string
			var q, bc int
			var prod, shelf time.Time
			var allergens []string
			var declared, cold bool
			var maxTemp *float64
			var pkg string
			if err := rows.Scan(&cat, &name, &q, &bc, &prod, &shelf,
				&allergens, &declared, &cold, &maxTemp, &pkg); err != nil {
				rows.Close()
				return nil, err
			}
			it["category"], it["name"] = cat, name
			it["quantity"], it["box_count"] = q, bc
			it["production_at"], it["shelf_life_until"] = prod, shelf
			it["allergens"], it["allergen_declared"] = allergens, declared
			it["needs_refrigeration"], it["package_integrity"] = cold, pkg
			it["max_storage_temp_c"] = maxTemp
			items = append(items, it)
		}
		rows.Close()
	}
	out["items"] = items

	// 预约
	var rID, rCode, rStatus, cName string
	var planned time.Time
	var late *int
	err = s.db.Pool.QueryRow(ctx, `
		SELECT r.id::text, r.code, r.status, c.name, r.planned_pickup_at, r.late_minutes
		FROM reservations r JOIN charities c ON c.id=r.charity_id
		WHERE r.batch_id=$1 ORDER BY r.requested_at DESC LIMIT 1`, id).
		Scan(&rID, &rCode, &rStatus, &cName, &planned, &late)
	if err == nil {
		out["reservation"] = map[string]any{
			"id": rID, "code": rCode, "status": rStatus, "charity": cName,
			"planned_pickup_at": planned, "late_minutes": late,
		}
	}

	// 箱体
	brows, _ := s.db.Pool.Query(ctx, `
		SELECT box_code, scan_status, seal_no, expected_qty, checked_qty,
		       measured_temp_c, COALESCE(reject_reason,'')
		FROM boxes WHERE batch_id=$1 ORDER BY box_code`, id)
	boxList := []map[string]any{}
	if brows != nil {
		for brows.Next() {
			var code, scan, reason string
			var seal *string
			var expected int
			var checked *int
			var temp *float64
			if err := brows.Scan(&code, &scan, &seal, &expected, &checked, &temp, &reason); err != nil {
				brows.Close()
				return nil, err
			}
			boxList = append(boxList, map[string]any{
				"box_code": code, "scan_status": scan, "seal_no": seal,
				"expected_qty": expected, "checked_qty": checked,
				"measured_temp_c": temp, "reject_reason": reason,
			})
		}
		brows.Close()
	}
	out["boxes"] = boxList

	// 凭证链
	chain, _ := s.GetVoucherChain(ctx, batchID)
	out["vouchers"] = chain

	// 异常
	erows, _ := s.db.Pool.Query(ctx, `
		SELECT ex_type, blocking, resolved, COALESCE(resolution,''), raised_at
		FROM exception_records WHERE batch_id=$1 ORDER BY raised_at`, id)
	exs := []map[string]any{}
	if erows != nil {
		for erows.Next() {
			var t, res string
			var blocking, resolved bool
			var at time.Time
			if err := erows.Scan(&t, &blocking, &resolved, &res, &at); err != nil {
				erows.Close()
				return nil, err
			}
			exs = append(exs, map[string]any{
				"type": t, "blocking": blocking, "resolved": resolved,
				"resolution": res, "raised_at": at,
			})
		}
		erows.Close()
	}
	out["exceptions"] = exs
	return out, nil
}

// Stats 返回 Redis 实时统计；附带公示表精确统计用于核对。
func (s *Service) Stats(ctx context.Context) map[string]any {
	stats := map[string]any{}
	if s.rdb != nil {
		if m, err := s.rdb.HGetAll(ctx, "bakery:stats").Result(); err == nil {
			stats["redis"] = m
		}
	}
	type row struct {
		result string
		n      int
		boxes  int
		qty    int
	}
	rows, err := s.db.Pool.Query(ctx, `
		SELECT result, COUNT(*), COALESCE(SUM(total_boxes),0), COALESCE(SUM(total_quantity),0)
		FROM public_ledger GROUP BY result`)
	dbStats := map[string]any{}
	if err == nil {
		for rows.Next() {
			var r row
			_ = rows.Scan(&r.result, &r.n, &r.boxes, &r.qty)
			dbStats[r.result] = map[string]any{"batches": r.n, "boxes": r.boxes, "quantity": r.qty}
		}
		rows.Close()
	}
	stats["ledger"] = dbStats
	return stats
}

// Reconcile 触发公示与统计重建（监管/运维）。
func (s *Service) Reconcile(ctx context.Context) (int, error) {
	return s.projector.Reconcile(ctx)
}

// ListExceptions 未解决异常清单（监管/品牌风险视角）。
func (s *Service) ListExceptions(ctx context.Context, unresolvedOnly bool) ([]map[string]any, error) {
	q := `
		SELECT e.id::text, COALESCE(e.batch_id::text,''), e.ex_type, e.blocking,
		       e.resolved, COALESCE(e.resolution,''), e.detail, e.raised_at
		FROM exception_records e`
	if unresolvedOnly {
		q += ` WHERE e.resolved=false`
	}
	q += ` ORDER BY e.raised_at DESC LIMIT 200`
	rows, err := s.db.Pool.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		var id, bid, t, res string
		var blocking, resolved bool
		var detail map[string]any
		var at time.Time
		if err := rows.Scan(&id, &bid, &t, &blocking, &resolved, &res, &detail, &at); err != nil {
			return nil, err
		}
		out = append(out, map[string]any{
			"id": id, "batch_id": bid, "type": t, "blocking": blocking,
			"resolved": resolved, "resolution": res, "detail": detail, "raised_at": at,
		})
	}
	return out, nil
}
