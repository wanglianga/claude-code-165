package service

import (
	"context"
	"encoding/json"

	"donation-service/internal/domain"
)

func bytesToMap(b []byte) map[string]any {
	m := map[string]any{}
	if len(b) > 0 {
		_ = json.Unmarshal(b, &m)
	}
	return m
}

// projectPublicityTx 在事务内生成/更新捐赠公示投影（发放完成时调用），
// 保证库存、交接与公示强一致。
func (s *Service) projectPublicityTx(ctx context.Context, tx txQuerier, batchID int64) error {
	var (
		batchNo, storeName, charityName, category string
		boxes                                     int
	)
	err := tx.QueryRow(ctx, `
		SELECT b.batch_no, st.name, ch.name, b.category, b.total_boxes
		FROM batches b
		JOIN stores st ON st.id = b.store_id
		JOIN reservations r ON r.batch_id = b.id AND r.status='completed'
		JOIN charities ch ON ch.id = r.charity_id
		WHERE b.id=$1
		ORDER BY r.id DESC LIMIT 1`, batchID).
		Scan(&batchNo, &storeName, &charityName, &category, &boxes)
	if err != nil {
		return err
	}
	rows, err := tx.Query(ctx, `
		SELECT st.recipient_type, st.site_name, d.boxes
		FROM distributions d
		JOIN delivery_stops st ON st.id = d.stop_id
		JOIN delivery_plans dp ON dp.id = st.plan_id
		JOIN reservations r ON r.id = dp.reservation_id
		WHERE r.batch_id=$1 ORDER BY d.id`, batchID)
	if err != nil {
		return err
	}
	defer rows.Close()
	summary := []domain.RecipientSum{}
	distributed := 0
	for rows.Next() {
		var rs domain.RecipientSum
		if err := rows.Scan(&rs.RecipientType, &rs.SiteName, &rs.Boxes); err != nil {
			return err
		}
		distributed += rs.Boxes
		summary = append(summary, rs)
	}
	summaryJSON, _ := json.Marshal(summary)
	_, err = tx.Exec(ctx, `
		INSERT INTO publicity(batch_id, batch_no, store_name, charity_name, category, boxes,
		    distributed_boxes, recipient_summary, status, published_at)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,'distributed', now())
		ON CONFLICT (batch_id) DO UPDATE SET
		    distributed_boxes=EXCLUDED.distributed_boxes,
		    recipient_summary=EXCLUDED.recipient_summary,
		    status=EXCLUDED.status, published_at=now()`,
		batchID, batchNo, storeName, charityName, category, boxes, distributed, summaryJSON)
	return err
}

// ProjectPublicity 供 Kafka consumer 使用的幂等投影入口。
func (s *Service) ProjectPublicity(ctx context.Context, batchID int64) error {
	return s.projectPublicityTx(ctx, s.DB, batchID)
}

// ListPublicity 捐赠公示列表（公开接口）。
func (s *Service) ListPublicity(ctx context.Context) ([]domain.Publicity, error) {
	rows, err := s.DB.Query(ctx, `
		SELECT id, batch_id, batch_no, store_name, charity_name, category, boxes,
		       distributed_boxes, recipient_summary, status, published_at
		FROM publicity ORDER BY id DESC LIMIT 200`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Publicity
	for rows.Next() {
		var p domain.Publicity
		var summary []byte
		if err := rows.Scan(&p.ID, &p.BatchID, &p.BatchNo, &p.StoreName, &p.CharityName,
			&p.Category, &p.Boxes, &p.DistributedBoxes, &summary, &p.Status, &p.PublishedAt); err != nil {
			return nil, err
		}
		p.RecipientSummary = []domain.RecipientSum{}
		_ = json.Unmarshal(summary, &p.RecipientSummary)
		out = append(out, p)
	}
	return out, rows.Err()
}
