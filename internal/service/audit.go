package service

import (
	"context"

	"donation-service/internal/domain"
)

// ListVouchers 批次凭证链。
func (s *Service) ListVouchers(ctx context.Context, batchID int64) ([]domain.Voucher, error) {
	rows, err := s.DB.Query(ctx, `
		SELECT id, batch_id, reservation_id, type, payload, hash, prev_hash, created_at
		FROM vouchers WHERE batch_id=$1 ORDER BY id`, batchID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Voucher
	for rows.Next() {
		var v domain.Voucher
		var payload []byte
		if err := rows.Scan(&v.ID, &v.BatchID, &v.ReservationID, &v.Type, &payload,
			&v.Hash, &v.PrevHash, &v.CreatedAt); err != nil {
			return nil, err
		}
		v.Payload = bytesToMap(payload)
		out = append(out, v)
	}
	return out, rows.Err()
}

// AuditTrail 监管抽查：批次全链路审计（凭证链 + 状态 + 交接 + 配送 + 发放 + 异常），
// 并实时重算哈希链校验完整性。
func (s *Service) AuditTrail(ctx context.Context, batchID int64) (map[string]any, error) {
	b, boxes, resv, err := s.GetBatchDetail(ctx, batchID)
	if err != nil {
		return nil, err
	}
	vouchers, err := s.ListVouchers(ctx, batchID)
	if err != nil {
		return nil, err
	}
	exceptions, err := s.ListExceptions(ctx, &batchID, "")
	if err != nil {
		return nil, err
	}
	result := map[string]any{
		"batch":       b,
		"boxes":       boxes,
		"reservation": resv,
		"vouchers":    vouchers,
		"chain_valid": domain.VerifyChain(vouchers),
		"exceptions":  exceptions,
	}
	if resv != nil {
		if scans, err := s.ListScans(ctx, resv.ID); err == nil {
			result["handover_scans"] = scans
		}
		var h domain.Handover
		err := s.DB.QueryRow(ctx, `
			SELECT id, reservation_id, status, store_staff_id, started_at, completed_at,
			       overall_temp_c, seal_intact, receiver_verified, receiver_name, receiver_id_no
			FROM handovers WHERE reservation_id=$1`, resv.ID).Scan(
			&h.ID, &h.ReservationID, &h.Status, &h.StoreStaffID, &h.StartedAt, &h.CompletedAt,
			&h.OverallTempC, &h.SealIntact, &h.ReceiverVerified, &h.ReceiverName, &h.ReceiverIDNo)
		if err == nil {
			result["handover"] = h
		}
		if delivery, err := s.GetDeliveryDetail(ctx, resv.ID); err == nil {
			result["delivery"] = delivery
		}
	}
	// 门店与机构名称
	var names struct {
		Store   string `json:"store"`
		Charity string `json:"charity"`
	}
	_ = s.DB.QueryRow(ctx, `SELECT name FROM stores WHERE id=$1`, b.StoreID).Scan(&names.Store)
	if resv != nil {
		_ = s.DB.QueryRow(ctx, `SELECT name FROM charities WHERE id=$1`, resv.CharityID).Scan(&names.Charity)
	}
	result["parties"] = names
	return result, nil
}
