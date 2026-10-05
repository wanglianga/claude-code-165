package service

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"donation-service/internal/domain"
	"donation-service/internal/store"
)

// ReportException 上报业务异常并联动状态机：
// sold_out(门店) / no_show(门店) / truck_late(机构) / regulatory_check(监管)。
func (s *Service) ReportException(ctx context.Context, u *domain.User, batchID int64,
	exType string, detail map[string]any) (*domain.Exception, error) {
	switch exType {
	case domain.ExSoldOut, domain.ExNoShow, domain.ExTruckLate, domain.ExRegulatoryCheck:
	default:
		return nil, domain.ErrBadRequest("异常类型取值: sold_out/no_show/truck_late/regulatory_check")
	}
	var ex *domain.Exception
	err := store.WithTx(ctx, s.DB, func(tx pgx.Tx) error {
		b, err := lockBatch(ctx, tx, batchID)
		if err != nil {
			return err
		}
		// 找到生效中的预约（如有）
		var resvID *int64
		var resv domain.Reservation
		var rid int64
		err = tx.QueryRow(ctx, `
			SELECT id FROM reservations
			WHERE batch_id=$1 AND status IN ('pending','confirmed')
			ORDER BY id DESC LIMIT 1 FOR UPDATE`, batchID).Scan(&rid)
		if err == nil {
			resvID = &rid
			if r, rerr := getReservation(ctx, tx, rid); rerr == nil {
				resv = *r
			}
		}

		switch exType {
		case domain.ExSoldOut:
			if err := RequireStore(u, b.StoreID); err != nil {
				return err
			}
			if b.Status == domain.BatchHandedOver || b.Status == domain.BatchInDelivery ||
				b.Status == domain.BatchDistributed {
				return domain.ErrConflict("批次已完成交接，不能按售罄取消")
			}
			if resvID != nil {
				if _, err := tx.Exec(ctx, `
					UPDATE reservations SET status=$1, cancel_reason='门店临时售罄', version=version+1, updated_at=now()
					WHERE id=$2`, domain.ResvCancelled, *resvID); err != nil {
					return err
				}
			}
			if _, err := tx.Exec(ctx, `
				UPDATE batches SET status=$1, version=version+1, updated_at=now()
				WHERE id=$2 AND status IN ('reported','reserved','confirmed','handover')`,
				domain.BatchCancelled, batchID); err != nil {
				return err
			}
		case domain.ExNoShow:
			if err := RequireStore(u, b.StoreID); err != nil {
				return err
			}
			if resvID == nil {
				return domain.ErrConflict("当前无生效预约，不能标记爽约")
			}
			if _, err := tx.Exec(ctx, `
				UPDATE reservations SET status=$1, cancel_reason='公益机构爽约未取货', version=version+1, updated_at=now()
				WHERE id=$2`, domain.ResvCancelled, *resvID); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `
				UPDATE batches SET status=$1, version=version+1, updated_at=now()
				WHERE id=$2 AND status IN ('reserved','confirmed','handover')`,
				domain.BatchExpired, batchID); err != nil {
				return err
			}
		case domain.ExTruckLate:
			if resvID == nil {
				return domain.ErrConflict("当前无生效预约，不能上报车辆迟到")
			}
			if err := RequireCharity(u, resv.CharityID); err != nil {
				return err
			}
			// 仅记录，不推进状态机；门店可据此决定等待或按爽约处理
		case domain.ExRegulatoryCheck:
			if u.Role != domain.RoleRegulator && u.Role != domain.RoleAdmin {
				return domain.ErrForbidden
			}
		}

		if detail == nil {
			detail = map[string]any{}
		}
		detail["reported_by"] = u.ID
		exID, err := insertException(ctx, tx, &batchID, resvID, exType, detail, &u.ID)
		if err != nil {
			return err
		}
		if err := s.emitExceptionEvent(ctx, tx, exID, &batchID, resvID, exType, detail); err != nil {
			return err
		}
		ex, err = s.getException(ctx, tx, exID)
		return err
	})
	return ex, err
}

// ResolveException 关闭异常（监管/管理员/上报方所属组织）。
func (s *Service) ResolveException(ctx context.Context, u *domain.User, exID int64, resolution string) (*domain.Exception, error) {
	if resolution == "" {
		return nil, domain.ErrBadRequest("处理结论必填")
	}
	var ex *domain.Exception
	err := store.WithTx(ctx, s.DB, func(tx pgx.Tx) error {
		e, err := s.getExceptionForUpdate(ctx, tx, exID)
		if err != nil {
			return err
		}
		if u.Role != domain.RoleAdmin && u.Role != domain.RoleRegulator {
			// 参与方可以关闭：门店方或机构方
			allowed := false
			if e.BatchID != nil {
				if b, berr := getBatch(ctx, tx, *e.BatchID); berr == nil {
					allowed = RequireStore(u, b.StoreID) == nil
				}
			}
			if !allowed && e.ReservationID != nil {
				if r, rerr := getReservation(ctx, tx, *e.ReservationID); rerr == nil {
					allowed = RequireCharity(u, r.CharityID) == nil
				}
			}
			if !allowed {
				return domain.ErrForbidden
			}
		}
		if e.Status == "resolved" {
			ex = e // 幂等
			return nil
		}
		if _, err := tx.Exec(ctx, `
			UPDATE exceptions SET status='resolved', resolution=$1, resolved_at=now()
			WHERE id=$2`, resolution, exID); err != nil {
			return err
		}
		if e.BatchID != nil {
			if err := insertOutbox(ctx, tx, s.Cfg.KafkaTopic, fmt.Sprintf("batch:%d", *e.BatchID), "exception.resolved", map[string]any{
				"exception_id": exID, "type": e.Type, "resolution": resolution,
			}); err != nil {
				return err
			}
		}
		ex, err = s.getException(ctx, tx, exID)
		return err
	})
	return ex, err
}

func (s *Service) getException(ctx context.Context, q txQuerier, id int64) (*domain.Exception, error) {
	e := &domain.Exception{}
	var detail []byte
	err := q.QueryRow(ctx, `
		SELECT id, ex_no, batch_id, reservation_id, type, detail, status, resolution, created_by, created_at, resolved_at
		FROM exceptions WHERE id=$1`, id).Scan(
		&e.ID, &e.ExNo, &e.BatchID, &e.ReservationID, &e.Type, &detail, &e.Status,
		&e.Resolution, &e.CreatedBy, &e.CreatedAt, &e.ResolvedAt)
	if err != nil {
		return nil, err
	}
	e.Detail = bytesToMap(detail)
	return e, nil
}

func (s *Service) getExceptionForUpdate(ctx context.Context, tx pgx.Tx, id int64) (*domain.Exception, error) {
	e := &domain.Exception{}
	var detail []byte
	err := tx.QueryRow(ctx, `
		SELECT id, ex_no, batch_id, reservation_id, type, detail, status, resolution, created_by, created_at, resolved_at
		FROM exceptions WHERE id=$1 FOR UPDATE`, id).Scan(
		&e.ID, &e.ExNo, &e.BatchID, &e.ReservationID, &e.Type, &detail, &e.Status,
		&e.Resolution, &e.CreatedBy, &e.CreatedAt, &e.ResolvedAt)
	if err != nil {
		return nil, err
	}
	e.Detail = bytesToMap(detail)
	return e, nil
}

// ListExceptions 按批次/状态查询异常。
func (s *Service) ListExceptions(ctx context.Context, batchID *int64, status string) ([]domain.Exception, error) {
	rows, err := s.DB.Query(ctx, `
		SELECT id, ex_no, batch_id, reservation_id, type, detail, status, resolution, created_by, created_at, resolved_at
		FROM exceptions
		WHERE ($1::bigint IS NULL OR batch_id=$1)
		  AND ($2::text IS NULL OR status=$2)
		ORDER BY id DESC LIMIT 200`, batchID, nilIfEmpty(status))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Exception
	for rows.Next() {
		var e domain.Exception
		var detail []byte
		if err := rows.Scan(&e.ID, &e.ExNo, &e.BatchID, &e.ReservationID, &e.Type, &detail,
			&e.Status, &e.Resolution, &e.CreatedBy, &e.CreatedAt, &e.ResolvedAt); err != nil {
			return nil, err
		}
		e.Detail = bytesToMap(detail)
		out = append(out, e)
	}
	return out, rows.Err()
}

// RunExpiryOnce 过期巡检：超过最晚取走时间的批次置为过期并取消生效预约；
// 超时未确认的预约自动过期释放批次。返回处理数量。
func (s *Service) RunExpiryOnce(ctx context.Context) (int, error) {
	return s.runExpiry(ctx)
}

func (s *Service) runExpiry(ctx context.Context) (int, error) {
	now := time.Now()
	processed := 0
	// 1) 过最晚取走时间的批次
	rows, err := s.DB.Query(ctx, `
		SELECT id FROM batches
		WHERE status IN ('reported','reserved','confirmed','handover') AND latest_pickup_at < $1
		ORDER BY id LIMIT 100`, now)
	if err != nil {
		return 0, err
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return processed, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	for _, id := range ids {
		err := store.WithTx(ctx, s.DB, func(tx pgx.Tx) error {
			b, err := lockBatch(ctx, tx, id)
			if err != nil {
				return err
			}
			if time.Now().Before(b.LatestPickupAt) {
				return nil // 已被其他流程处理
			}
			var resvID *int64
			var rid int64
			if err := tx.QueryRow(ctx, `
				SELECT id FROM reservations WHERE batch_id=$1 AND status IN ('pending','confirmed')
				ORDER BY id DESC LIMIT 1 FOR UPDATE`, id).Scan(&rid); err == nil {
				resvID = &rid
				if _, err := tx.Exec(ctx, `
					UPDATE reservations SET status=$1, cancel_reason='超过可捐赠时限', version=version+1, updated_at=now()
					WHERE id=$2`, domain.ResvExpired, rid); err != nil {
					return err
				}
			}
			if _, err := tx.Exec(ctx, `
				UPDATE batches SET status=$1, version=version+1, updated_at=now()
				WHERE id=$2 AND status IN ('reported','reserved','confirmed','handover')`,
				domain.BatchExpired, id); err != nil {
				return err
			}
			detail := map[string]any{"batch_no": b.BatchNo, "latest_pickup_at": b.LatestPickupAt}
			exID, err := insertException(ctx, tx, &id, resvID, domain.ExExpired, detail, nil)
			if err != nil {
				return err
			}
			if err := s.emitExceptionEvent(ctx, tx, exID, &id, resvID, domain.ExExpired, detail); err != nil {
				return err
			}
			return insertOutbox(ctx, tx, s.Cfg.KafkaTopic, fmt.Sprintf("batch:%d", id), "batch.expired", map[string]any{
				"batch_id": id, "batch_no": b.BatchNo,
			})
		})
		if err == nil {
			processed++
		}
	}
	// 2) 超时未确认的预约自动过期，批次释放回可预约池
	rrows, err := s.DB.Query(ctx, `
		SELECT id FROM reservations
		WHERE status='pending' AND created_at < $1
		ORDER BY id LIMIT 100`, now.Add(-s.Cfg.ReservationTTL))
	if err != nil {
		return processed, err
	}
	var rids []int64
	for rrows.Next() {
		var id int64
		if err := rrows.Scan(&id); err != nil {
			rrows.Close()
			return processed, err
		}
		rids = append(rids, id)
	}
	rrows.Close()
	for _, rid := range rids {
		err := store.WithTx(ctx, s.DB, func(tx pgx.Tx) error {
			r, err := lockReservation(ctx, tx, rid)
			if err != nil {
				return err
			}
			if r.Status != domain.ResvPending {
				return nil
			}
			if _, err := tx.Exec(ctx, `
				UPDATE reservations SET status=$1, cancel_reason='预约超时未确认，自动释放', version=version+1, updated_at=now()
				WHERE id=$2 AND status='pending'`, domain.ResvExpired, rid); err != nil {
				return err
			}
			newStatus := domain.BatchReported
			b, err := lockBatch(ctx, tx, r.BatchID)
			if err != nil {
				return err
			}
			if time.Now().After(b.LatestPickupAt) {
				newStatus = domain.BatchExpired
			}
			if _, err := tx.Exec(ctx, `
				UPDATE batches SET status=$1, version=version+1, updated_at=now()
				WHERE id=$2 AND status='reserved'`, newStatus, r.BatchID); err != nil {
				return err
			}
			return insertOutbox(ctx, tx, s.Cfg.KafkaTopic, fmt.Sprintf("batch:%d", r.BatchID), "reservation.expired", map[string]any{
				"batch_id": r.BatchID, "reservation_id": rid, "batch_status": newStatus,
			})
		})
		if err == nil {
			processed++
		}
	}
	return processed, nil
}
