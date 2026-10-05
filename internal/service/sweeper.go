package service

import (
	"context"
	"errors"
	"log"
	"time"

	"bakery-donation/internal/events"
)

// SweepResult 一次定时扫描的处理结果
type SweepResult struct {
	ExpiredBatches []string `json:"expired_batches"`
	NoShows        []string `json:"no_show_reservations"`
}

// Sweep 定时处理：
//  1. 超过可捐赠时限仍无人预约/未确认的批次 -> expired（门店减少浪费统计体现为未捐出）；
//  2. confirmed 但超过计划取货时间+宽限期仍未到达 -> 机构爽约，批次释放/过期。
func (s *Service) Sweep(ctx context.Context) (SweepResult, error) {
	res := SweepResult{ExpiredBatches: []string{}, NoShows: []string{}}
	tx, err := s.begin(ctx)
	if err != nil {
		return res, err
	}
	defer tx.Rollback(ctx)

	// 1. 超时批次
	rows, err := tx.Query(ctx, `
		SELECT id FROM donation_batches
		WHERE donation_deadline <= now()
		  AND status IN ('open','released','reserved')
		FOR UPDATE SKIP LOCKED`)
	if err != nil {
		return res, err
	}
	for rows.Next() {
		var id string
		_ = rows.Scan(&id)
		res.ExpiredBatches = append(res.ExpiredBatches, id)
	}
	rows.Close()
	for _, id := range res.ExpiredBatches {
		if _, err := tx.Exec(ctx, `
			UPDATE donation_batches SET status='expired', closed_at=now(),
			       close_reason='超过可捐赠时限'
			WHERE id=$1`, id); err != nil {
			return res, err
		}
		if _, err := tx.Exec(ctx, `
			UPDATE reservations SET status='auto_expired', decided_at=now(),
			       reject_reason='超过可捐赠时限，系统自动失效'
			WHERE batch_id=$1 AND status='requested'`, id); err != nil {
			return res, err
		}
		if err := s.raiseException(ctx, tx, mustUUIDP(id), "past_deadline",
			map[string]any{"at": time.Now().Format(time.RFC3339)}, false); err != nil {
			return res, err
		}
		if err := emit(ctx, tx, mustUUID(id), events.TypeBatchExpired,
			map[string]any{"at": time.Now()}); err != nil {
			return res, err
		}
	}

	// 2. 爽约：confirmed 且计划取货时间+宽限已过、仍未到达
	nrows, err := tx.Query(ctx, `
		SELECT r.id::text, r.batch_id::text
		FROM reservations r
		WHERE r.status='confirmed' AND r.arrived_at IS NULL
		  AND r.planned_pickup_at <= now() - make_interval(mins => $1)
		FOR UPDATE OF r SKIP LOCKED`, s.cfg.NoShowGraceMin)
	if err != nil {
		return res, err
	}
	type ns struct{ rid, bid string }
	var noShows []ns
	for nrows.Next() {
		var n ns
		_ = nrows.Scan(&n.rid, &n.bid)
		noShows = append(noShows, n)
	}
	nrows.Close()
	for _, n := range noShows {
		res.NoShows = append(res.NoShows, n.rid)
		if _, err := tx.Exec(ctx, `
			UPDATE reservations SET status='no_show', decided_at=now()
			WHERE id=$1`, n.rid); err != nil {
			return res, err
		}
		var cid string
		if err := tx.QueryRow(ctx,
			`UPDATE charities SET no_show_count = no_show_count + 1
			 WHERE id=(SELECT charity_id FROM reservations WHERE id=$1)
			 RETURNING id::text`, n.rid).Scan(&cid); err != nil {
			return res, err
		}
		if err := s.recalcReliability(ctx, tx, cid); err != nil {
			return res, err
		}
		// 批次：仍有时限则释放给其他机构，否则过期
		var deadline time.Time
		if err := tx.QueryRow(ctx,
			`SELECT donation_deadline FROM donation_batches WHERE id=$1`, n.bid).Scan(&deadline); err != nil {
			return res, err
		}
		if deadline.After(time.Now()) {
			if err := resetForRebook(ctx, tx, mustUUID(n.bid)); err != nil {
				return res, err
			}
			if _, err := tx.Exec(ctx, `
				UPDATE donation_batches SET status='released', close_reason='机构爽约，库存重新开放'
				WHERE id=$1`, n.bid); err != nil {
				return res, err
			}
			if err := emit(ctx, tx, mustUUID(n.bid), events.TypeBatchReleased,
				map[string]any{"reason": "no_show"}); err != nil {
				return res, err
			}
		} else {
			if _, err := tx.Exec(ctx, `
				UPDATE donation_batches SET status='expired', closed_at=now(),
				       close_reason='机构爽约且已过可捐赠时限' WHERE id=$1`, n.bid); err != nil {
				return res, err
			}
			if err := emit(ctx, tx, mustUUID(n.bid), events.TypeBatchExpired,
				map[string]any{"reason": "no_show"}); err != nil {
				return res, err
			}
		}
		if err := s.raiseException(ctx, tx, mustUUIDP(n.bid), "no_show",
			map[string]any{"reservation_id": n.rid,
				"grace_minutes": s.cfg.NoShowGraceMin}, false); err != nil {
			return res, err
		}
		if err := emit(ctx, tx, mustUUID(n.bid), events.TypeNoShow,
			map[string]any{"reservation_id": n.rid}); err != nil {
			return res, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return res, err
	}
	return res, nil
}

// RunSweeper 周期性执行扫描。
func (s *Service) RunSweeper(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.SweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if res, err := s.Sweep(ctx); err != nil {
				if !errors.Is(err, context.Canceled) {
					log.Printf("sweeper: %v", err)
				}
			} else if len(res.ExpiredBatches)+len(res.NoShows) > 0 {
				log.Printf("sweeper: expired=%d no_show=%d",
					len(res.ExpiredBatches), len(res.NoShows))
			}
		}
	}
}
