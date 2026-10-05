package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"donation-service/internal/domain"
	"donation-service/internal/store"
)

// ReserveInput 公益机构预约批次的输入。
type ReserveInput struct {
	CharityID       int64
	Boxes           int
	PlannedPickupAt time.Time
	VehiclePlate    string
	DriverName      string
	ColdChain       bool
	ReceiverName    string
	ReceiverPhone   string
	ReceiverIDNo    string
	IdempotencyKey  string
}

// Reserve 预约批次：Redis 幂等键 + 批次锁 + 数据库唯一索引三重防重，
// 并执行服务半径/接收能力/冷链/时限/大批量释放规则。
func (s *Service) Reserve(ctx context.Context, u *domain.User, batchID int64, in ReserveInput) (*domain.Reservation, bool, error) {
	if err := RequireCharity(u, in.CharityID); err != nil {
		return nil, false, err
	}
	if in.IdempotencyKey == "" {
		return nil, false, domain.ErrBadRequest("缺少 Idempotency-Key 请求头")
	}
	if in.Boxes <= 0 {
		return nil, false, domain.ErrBadRequest("预约箱数必须为正数")
	}
	if in.PlannedPickupAt.IsZero() {
		return nil, false, domain.ErrBadRequest("计划取货时间必填")
	}

	// 1) 幂等：同一 Idempotency-Key 直接返回首次结果
	idemKey := fmt.Sprintf("idem:reserve:%d:%s", in.CharityID, in.IdempotencyKey)
	if cached, err := s.RDB.Get(ctx, idemKey).Result(); err == nil {
		var resvID int64
		if json.Unmarshal([]byte(cached), &resvID) == nil {
			if r, err := getReservation(ctx, s.DB, resvID); err == nil {
				return r, true, nil
			}
		}
	}
	// 数据库层幂等兜底（Redis 失效时）
	if r, err := s.findReservationByIdemKey(ctx, in.IdempotencyKey); err == nil {
		return r, true, nil
	}

	// 2) 分布式锁：同一批次串行预约
	lockKey := fmt.Sprintf("lock:batch:%d", batchID)
	ok, err := s.RDB.SetNX(ctx, lockKey, "1", 15*time.Second).Result()
	if err != nil {
		return nil, false, fmt.Errorf("redis lock: %w", err)
	}
	if !ok {
		return nil, false, domain.ErrConflict("该批次正在被其他机构预约，请稍后重试")
	}
	defer s.RDB.Del(ctx, lockKey)

	var resv *domain.Reservation
	var checkErr *domain.AppError
	var rejectedBatch *domain.Batch
	err = store.WithTx(ctx, s.DB, func(tx pgx.Tx) error {
		b, err := lockBatch(ctx, tx, batchID)
		if err != nil {
			return err
		}
		c, err := getCharity(ctx, tx, in.CharityID)
		if err != nil {
			return err
		}
		// 机构当日已占用能力（生效中的预约）
		var reservedToday, coldReservedToday int
		if err := tx.QueryRow(ctx, `
			SELECT COALESCE(SUM(boxes),0),
			       COALESCE(SUM(CASE WHEN cold_chain THEN boxes ELSE 0 END),0)
			FROM reservations
			WHERE charity_id=$1 AND status IN ('pending','confirmed')
			  AND created_at::date = CURRENT_DATE`, in.CharityID).
			Scan(&reservedToday, &coldReservedToday); err != nil {
			return err
		}

		if ce := domain.CheckReservation(domain.ReserveCheckInput{
			Batch: *b, Charity: *c,
			ReservedTodayBoxes: reservedToday, ColdReservedToday: coldReservedToday,
			RequestBoxes: in.Boxes, ColdChainVehicle: in.ColdChain,
			PlannedPickupAt: in.PlannedPickupAt, Now: time.Now(),
			LargeReleaseRatio: s.Cfg.LargeReleaseRatio, LargeReleaseMaxWait: s.Cfg.LargeReleaseMaxWait,
		}); ce != nil {
			// 业务回滚，阻断异常在事务外独立落库（见下方）
			checkErr = ce
			rejectedBatch = b
			return ce
		}

		var id int64
		var resvNo string
		err = tx.QueryRow(ctx, `
			INSERT INTO reservations(reservation_no, batch_id, charity_id, idempotency_key, boxes,
			    planned_pickup_at, vehicle_plate, driver_name, cold_chain,
			    receiver_name, receiver_phone, receiver_id_no)
			VALUES('R' || to_char(now(),'YYYYMMDD') || '-' || lpad(nextval('resv_no_seq')::text, 5, '0'),
			    $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
			RETURNING id, reservation_no`,
			batchID, in.CharityID, in.IdempotencyKey, in.Boxes, in.PlannedPickupAt,
			in.VehiclePlate, in.DriverName, in.ColdChain,
			in.ReceiverName, in.ReceiverPhone, in.ReceiverIDNo).Scan(&id, &resvNo)
		if isUniqueViolation(err) {
			return domain.ErrConflict("该批次已被预约或幂等键已使用")
		}
		if err != nil {
			return err
		}
		if err := transitionBatch(ctx, tx, batchID, domain.BatchReported, domain.BatchReserved); err != nil {
			return err
		}
		if err := insertVoucher(ctx, tx, batchID, &id, domain.VoucherReservation, map[string]any{
			"reservation_no": resvNo, "charity_id": in.CharityID, "boxes": in.Boxes,
			"planned_pickup_at": in.PlannedPickupAt, "cold_chain": in.ColdChain,
			"vehicle_plate": in.VehiclePlate,
		}); err != nil {
			return err
		}
		if err := insertOutbox(ctx, tx, s.Cfg.KafkaTopic, fmt.Sprintf("batch:%d", batchID), "batch.reserved", map[string]any{
			"batch_id": batchID, "reservation_id": id, "charity_id": in.CharityID, "boxes": in.Boxes,
		}); err != nil {
			return err
		}
		resv, err = getReservation(ctx, tx, id)
		return err
	})
	if checkErr != nil {
		// 预约被规则阻断：独立事务记录异常轨迹（不随业务回滚）
		s.recordReserveRejection(ctx, u, rejectedBatch, in, checkErr)
		return nil, false, checkErr
	}
	if err != nil {
		return nil, false, err
	}
	// 3) 写入幂等缓存（24h）
	if b, err := json.Marshal(resv.ID); err == nil {
		s.RDB.Set(ctx, idemKey, b, 24*time.Hour)
	}
	return resv, false, nil
}

// recordReserveRejection 预约被规则阻断后，独立事务落库异常凭证与事件。
func (s *Service) recordReserveRejection(ctx context.Context, u *domain.User, b *domain.Batch, in ReserveInput, checkErr *domain.AppError) {
	exType := domain.ExCapacityRejected
	if !b.AllergenConfirmed {
		exType = domain.ExAllergenMissing
	}
	detail := map[string]any{"reason": checkErr.Message, "charity_id": in.CharityID, "boxes": in.Boxes}
	_ = store.WithTx(ctx, s.DB, func(tx pgx.Tx) error {
		if _, err := lockBatch(ctx, tx, b.ID); err != nil {
			return err
		}
		exID, err := insertException(ctx, tx, &b.ID, nil, exType, detail, &u.ID)
		if err != nil {
			return err
		}
		return s.emitExceptionEvent(ctx, tx, exID, &b.ID, nil, exType, detail)
	})
}

func (s *Service) findReservationByIdemKey(ctx context.Context, key string) (*domain.Reservation, error) {
	var id int64
	err := s.DB.QueryRow(ctx,
		`SELECT id FROM reservations WHERE idempotency_key=$1`, key).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return getReservation(ctx, s.DB, id)
}

// Confirm 门店确认预约，生成交接待办。
func (s *Service) Confirm(ctx context.Context, u *domain.User, resvID int64) (*domain.Reservation, error) {
	var resv *domain.Reservation
	err := store.WithTx(ctx, s.DB, func(tx pgx.Tx) error {
		r, err := lockReservation(ctx, tx, resvID)
		if err != nil {
			return err
		}
		b, err := lockBatch(ctx, tx, r.BatchID)
		if err != nil {
			return err
		}
		if err := RequireStore(u, b.StoreID); err != nil {
			return err
		}
		if u.Role != domain.RoleStoreManager && u.Role != domain.RoleAdmin {
			return domain.NewError(403, "NEED_MANAGER", "仅门店店长可确认预约")
		}
		if r.Status != domain.ResvPending {
			return domain.ErrConflict(fmt.Sprintf("预约状态为 %s，不可确认", r.Status))
		}
		if time.Now().After(b.LatestPickupAt) {
			return domain.ErrUnprocessable("批次已超过最晚取走时间，无法确认")
		}
		tag, err := tx.Exec(ctx, `
			UPDATE reservations SET status=$1, version=version+1, updated_at=now()
			WHERE id=$2 AND status=$3`, domain.ResvConfirmed, resvID, domain.ResvPending)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return domain.ErrConflict("预约状态已变化")
		}
		if err := transitionBatch(ctx, tx, b.ID, domain.BatchReserved, domain.BatchConfirmed); err != nil {
			return err
		}
		if err := insertVoucher(ctx, tx, b.ID, &resvID, domain.VoucherConfirmation, map[string]any{
			"reservation_no": r.ReservationNo, "confirmed_by": u.ID, "store_id": b.StoreID,
		}); err != nil {
			return err
		}
		if err := insertOutbox(ctx, tx, s.Cfg.KafkaTopic, fmt.Sprintf("batch:%d", b.ID), "batch.confirmed", map[string]any{
			"batch_id": b.ID, "reservation_id": resvID,
		}); err != nil {
			return err
		}
		resv, err = getReservation(ctx, tx, resvID)
		return err
	})
	return resv, err
}

// CancelReservation 取消预约：批次回到可预约池（未过最晚取走时间时）。
func (s *Service) CancelReservation(ctx context.Context, u *domain.User, resvID int64, reason string) (*domain.Reservation, error) {
	if reason == "" {
		return nil, domain.ErrBadRequest("取消原因必填")
	}
	var resv *domain.Reservation
	err := store.WithTx(ctx, s.DB, func(tx pgx.Tx) error {
		r, err := lockReservation(ctx, tx, resvID)
		if err != nil {
			return err
		}
		b, err := lockBatch(ctx, tx, r.BatchID)
		if err != nil {
			return err
		}
		// 机构本人或门店店长可取消
		if err := RequireCharity(u, r.CharityID); err != nil {
			if err2 := RequireStore(u, b.StoreID); err2 != nil {
				return domain.ErrForbidden
			}
		}
		if r.Status != domain.ResvPending && r.Status != domain.ResvConfirmed {
			return domain.ErrConflict(fmt.Sprintf("预约状态为 %s，不可取消", r.Status))
		}
		if _, err := tx.Exec(ctx, `
			UPDATE reservations SET status=$1, cancel_reason=$2, version=version+1, updated_at=now()
			WHERE id=$3`, domain.ResvCancelled, reason, resvID); err != nil {
			return err
		}
		// 批次回到可预约池或过期
		newStatus := domain.BatchReported
		if time.Now().After(b.LatestPickupAt) {
			newStatus = domain.BatchExpired
		}
		if _, err := tx.Exec(ctx, `
			UPDATE batches SET status=$1, version=version+1, updated_at=now()
			WHERE id=$2 AND status IN ('reserved','confirmed')`, newStatus, b.ID); err != nil {
			return err
		}
		if err := insertOutbox(ctx, tx, s.Cfg.KafkaTopic, fmt.Sprintf("batch:%d", b.ID), "reservation.cancelled", map[string]any{
			"batch_id": b.ID, "reservation_id": resvID, "reason": reason, "batch_status": newStatus,
		}); err != nil {
			return err
		}
		resv, err = getReservation(ctx, tx, resvID)
		return err
	})
	return resv, err
}

// HandoverList 待交接清单：预约信息 + 全部箱号及扫码状态。
func (s *Service) HandoverList(ctx context.Context, u *domain.User, resvID int64) (map[string]any, error) {
	r, err := getReservation(ctx, s.DB, resvID)
	if err != nil {
		return nil, err
	}
	b, err := getBatch(ctx, s.DB, r.BatchID)
	if err != nil {
		return nil, err
	}
	if err := RequireStore(u, b.StoreID); err != nil {
		if err2 := RequireCharity(u, r.CharityID); err2 != nil {
			return nil, domain.ErrForbidden
		}
	}
	boxes, err := s.listBoxes(ctx, r.BatchID)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"reservation": r,
		"batch":       b,
		"boxes":       boxes,
	}, nil
}

// GetReservation 查询预约（参与方与监管可见）。
func (s *Service) GetReservation(ctx context.Context, u *domain.User, resvID int64) (*domain.Reservation, error) {
	r, err := getReservation(ctx, s.DB, resvID)
	if err != nil {
		return nil, err
	}
	if u.Role == domain.RoleRegulator || u.Role == domain.RoleAdmin {
		return r, nil
	}
	if err := RequireCharity(u, r.CharityID); err != nil {
		b, berr := getBatch(ctx, s.DB, r.BatchID)
		if berr != nil {
			return nil, berr
		}
		if err2 := RequireStore(u, b.StoreID); err2 != nil {
			return nil, domain.ErrForbidden
		}
	}
	return r, nil
}
