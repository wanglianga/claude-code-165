package service

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"donation-service/internal/domain"
	"donation-service/internal/store"
)

// StartHandover 门店开始交接：批次进入交接中，创建交接单。
func (s *Service) StartHandover(ctx context.Context, u *domain.User, resvID int64) (*domain.Handover, error) {
	var h *domain.Handover
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
		if r.Status != domain.ResvConfirmed {
			return domain.ErrConflict(fmt.Sprintf("预约状态为 %s，不可开始交接", r.Status))
		}
		// 幂等：已存在交接单直接返回
		var id int64
		err = tx.QueryRow(ctx,
			`SELECT id FROM handovers WHERE reservation_id=$1`, resvID).Scan(&id)
		if err == nil {
			h, err = s.getHandover(ctx, tx, id)
			return err
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		if err := transitionBatch(ctx, tx, b.ID, domain.BatchConfirmed, domain.BatchHandover); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `
			INSERT INTO handovers(reservation_id, store_staff_id) VALUES($1,$2) RETURNING id`,
			resvID, u.ID).Scan(&id); err != nil {
			return err
		}
		if err := insertOutbox(ctx, tx, s.Cfg.KafkaTopic, fmt.Sprintf("batch:%d", b.ID), "handover.started", map[string]any{
			"batch_id": b.ID, "reservation_id": resvID, "handover_id": id,
		}); err != nil {
			return err
		}
		h, err = s.getHandover(ctx, tx, id)
		return err
	})
	return h, err
}

// ScanBox 交接扫码核对：箱号、温度、封签。同箱号重复扫码幂等返回首次结果；
// 错扫箱号记录异常；温度超标记录 temp_breach 异常。
func (s *Service) ScanBox(ctx context.Context, u *domain.User, resvID int64, boxNo string, tempC *float64, sealIntact *bool) (*domain.HandoverScan, bool, error) {
	if boxNo == "" {
		return nil, false, domain.ErrBadRequest("箱号必填")
	}
	var scan *domain.HandoverScan
	created := true
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
		var handoverID int64
		var hStatus string
		err = tx.QueryRow(ctx,
			`SELECT id, status FROM handovers WHERE reservation_id=$1 FOR UPDATE`, resvID).
			Scan(&handoverID, &hStatus)
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.ErrConflict("交接尚未开始")
		}
		if err != nil {
			return err
		}
		if hStatus != "in_progress" {
			return domain.ErrConflict("交接已结束，不可继续扫码")
		}
		// 幂等：同交接单同箱号直接返回首扫结果
		existing, err := s.findScan(ctx, tx, handoverID, boxNo)
		if err == nil {
			scan = existing
			created = false // 幂等重放
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}

		var boxID int64
		var boxStatus string
		err = tx.QueryRow(ctx,
			`SELECT id, status FROM boxes WHERE batch_id=$1 AND box_no=$2 FOR UPDATE`,
			b.ID, boxNo).Scan(&boxID, &boxStatus)
		if errors.Is(err, pgx.ErrNoRows) {
			// 箱号错扫：记录扫码结果 + 异常，不中断交接
			detail := map[string]any{"box_no": boxNo, "batch_no": b.BatchNo, "scanned_by": u.ID}
			exID, ierr := insertException(ctx, tx, &b.ID, &resvID, domain.ExWrongScan, detail, &u.ID)
			if ierr != nil {
				return ierr
			}
			if ierr := s.emitExceptionEvent(ctx, tx, exID, &b.ID, &resvID, domain.ExWrongScan, detail); ierr != nil {
				return ierr
			}
			scan, ierr = s.insertScan(ctx, tx, handoverID, nil, boxNo, "wrong_box", tempC, sealIntact,
				fmt.Sprintf("箱号 %s 不属于批次 %s", boxNo, b.BatchNo))
			return ierr
		}
		if err != nil {
			return err
		}
		if boxStatus != domain.BoxPending {
			scan, err = s.insertScan(ctx, tx, handoverID, &boxID, boxNo, "duplicate", tempC, sealIntact,
				"该箱已扫描，请勿重复交接")
			return err
		}
		// 温度校验：冷链批次超标记录 temp_breach 异常（仍允许交接，由监管与机构追踪）
		msg := "ok"
		if tempC != nil && !domain.CheckTemp(b, *tempC) {
			detail := map[string]any{
				"box_no": boxNo, "temp_c": *tempC,
				"storage_temp_max_c": b.StorageTempMaxC, "scanned_by": u.ID,
			}
			exID, ierr := insertException(ctx, tx, &b.ID, &resvID, domain.ExTempBreach, detail, &u.ID)
			if ierr != nil {
				return ierr
			}
			if ierr := s.emitExceptionEvent(ctx, tx, exID, &b.ID, &resvID, domain.ExTempBreach, detail); ierr != nil {
				return ierr
			}
			msg = fmt.Sprintf("温度 %.1f℃ 超过上限 %.1f℃，已记录温度异常", *tempC, *b.StorageTempMaxC)
		}
		if _, err := tx.Exec(ctx,
			`UPDATE boxes SET status=$1 WHERE id=$2`, domain.BoxScanned, boxID); err != nil {
			return err
		}
		scan, err = s.insertScan(ctx, tx, handoverID, &boxID, boxNo, "ok", tempC, sealIntact, msg)
		return err
	})
	return scan, created, err
}

// CompleteHandover 完成交接：全部箱扫描完成 + 接收人身份核验 + 整体温度与封签。
func (s *Service) CompleteHandover(ctx context.Context, u *domain.User, resvID int64,
	receiverName, receiverIDNo string, overallTempC *float64, sealIntact *bool) (*domain.Handover, error) {
	if receiverName == "" || receiverIDNo == "" {
		return nil, domain.ErrBadRequest("接收人姓名与证件号必填")
	}
	if sealIntact == nil {
		return nil, domain.ErrBadRequest("封签状态必填")
	}
	var h *domain.Handover
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
		var handoverID int64
		var hStatus string
		err = tx.QueryRow(ctx,
			`SELECT id, status FROM handovers WHERE reservation_id=$1 FOR UPDATE`, resvID).
			Scan(&handoverID, &hStatus)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return domain.ErrConflict("交接尚未开始")
			}
			return err
		}
		if hStatus == "completed" {
			h, err = s.getHandover(ctx, tx, handoverID) // 幂等返回
			return err
		}
		// 全部箱必须完成扫码
		var pending int
		if err := tx.QueryRow(ctx,
			`SELECT COUNT(*) FROM boxes WHERE batch_id=$1 AND status='pending'`, b.ID).Scan(&pending); err != nil {
			return err
		}
		if pending > 0 {
			return domain.ErrUnprocessable(fmt.Sprintf("仍有 %d 箱未扫码核对，不能完成交接", pending))
		}
		// 接收人身份核验：预约时登记了接收人则必须一致
		verified := true
		if r.ReceiverIDNo != "" && r.ReceiverIDNo != receiverIDNo {
			verified = false
		}
		if !verified {
			return domain.ErrUnprocessable("接收人证件号与预约登记不一致，身份核验未通过")
		}
		if b.RequiresColdChain && overallTempC == nil {
			return domain.ErrBadRequest("冷链批次必须记录交接整体温度 overall_temp_c")
		}
		if overallTempC != nil && !domain.CheckTemp(b, *overallTempC) {
			detail := map[string]any{
				"overall_temp_c": *overallTempC, "storage_temp_max_c": b.StorageTempMaxC,
				"stage": "handover_complete",
			}
			exID, ierr := insertException(ctx, tx, &b.ID, &resvID, domain.ExTempBreach, detail, &u.ID)
			if ierr != nil {
				return ierr
			}
			if ierr := s.emitExceptionEvent(ctx, tx, exID, &b.ID, &resvID, domain.ExTempBreach, detail); ierr != nil {
				return ierr
			}
		}
		if _, err := tx.Exec(ctx, `
			UPDATE handovers SET status='completed', completed_at=now(), overall_temp_c=$1,
			    seal_intact=$2, receiver_verified=$3, receiver_name=$4, receiver_id_no=$5
			WHERE id=$6`, overallTempC, *sealIntact, verified, receiverName, receiverIDNo, handoverID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`UPDATE boxes SET status=$1 WHERE batch_id=$2 AND status=$3`,
			domain.BoxHandedOver, b.ID, domain.BoxScanned); err != nil {
			return err
		}
		if err := transitionBatch(ctx, tx, b.ID, domain.BatchHandover, domain.BatchHandedOver); err != nil {
			return err
		}
		if err := insertVoucher(ctx, tx, b.ID, &resvID, domain.VoucherHandover, map[string]any{
			"handover_id": handoverID, "reservation_no": r.ReservationNo,
			"overall_temp_c": overallTempC, "seal_intact": *sealIntact,
			"receiver_name": receiverName, "receiver_id_no": receiverIDNo,
			"receiver_verified": verified, "store_staff_id": u.ID, "boxes": b.TotalBoxes,
		}); err != nil {
			return err
		}
		if err := insertOutbox(ctx, tx, s.Cfg.KafkaTopic, fmt.Sprintf("batch:%d", b.ID), "batch.handed_over", map[string]any{
			"batch_id": b.ID, "reservation_id": resvID, "handover_id": handoverID,
		}); err != nil {
			return err
		}
		h, err = s.getHandover(ctx, tx, handoverID)
		return err
	})
	return h, err
}

func (s *Service) getHandover(ctx context.Context, q txQuerier, id int64) (*domain.Handover, error) {
	h := &domain.Handover{}
	err := q.QueryRow(ctx, `
		SELECT id, reservation_id, status, store_staff_id, started_at, completed_at,
		       overall_temp_c, seal_intact, receiver_verified, receiver_name, receiver_id_no
		FROM handovers WHERE id=$1`, id).Scan(
		&h.ID, &h.ReservationID, &h.Status, &h.StoreStaffID, &h.StartedAt, &h.CompletedAt,
		&h.OverallTempC, &h.SealIntact, &h.ReceiverVerified, &h.ReceiverName, &h.ReceiverIDNo)
	return h, err
}

func (s *Service) findScan(ctx context.Context, q txQuerier, handoverID int64, boxNo string) (*domain.HandoverScan, error) {
	sc := &domain.HandoverScan{}
	err := q.QueryRow(ctx, `
		SELECT id, handover_id, box_id, box_no, result, temp_c, seal_intact, message, scanned_at
		FROM handover_scans WHERE handover_id=$1 AND box_no=$2`, handoverID, boxNo).Scan(
		&sc.ID, &sc.HandoverID, &sc.BoxID, &sc.BoxNo, &sc.Result, &sc.TempC, &sc.SealIntact,
		&sc.Message, &sc.ScannedAt)
	return sc, err
}

func (s *Service) insertScan(ctx context.Context, q txQuerier, handoverID int64, boxID *int64,
	boxNo, result string, tempC *float64, sealIntact *bool, msg string) (*domain.HandoverScan, error) {
	sc := &domain.HandoverScan{}
	err := q.QueryRow(ctx, `
		INSERT INTO handover_scans(handover_id, box_id, box_no, result, temp_c, seal_intact, message)
		VALUES($1,$2,$3,$4,$5,$6,$7)
		RETURNING id, handover_id, box_id, box_no, result, temp_c, seal_intact, message, scanned_at`,
		handoverID, boxID, boxNo, result, tempC, sealIntact, msg).Scan(
		&sc.ID, &sc.HandoverID, &sc.BoxID, &sc.BoxNo, &sc.Result, &sc.TempC, &sc.SealIntact,
		&sc.Message, &sc.ScannedAt)
	return sc, err
}

// ListScans 交接扫码记录。
func (s *Service) ListScans(ctx context.Context, resvID int64) ([]domain.HandoverScan, error) {
	rows, err := s.DB.Query(ctx, `
		SELECT s.id, s.handover_id, s.box_id, s.box_no, s.result, s.temp_c, s.seal_intact, s.message, s.scanned_at
		FROM handover_scans s JOIN handovers h ON h.id = s.handover_id
		WHERE h.reservation_id=$1 ORDER BY s.id`, resvID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.HandoverScan
	for rows.Next() {
		var sc domain.HandoverScan
		if err := rows.Scan(&sc.ID, &sc.HandoverID, &sc.BoxID, &sc.BoxNo, &sc.Result,
			&sc.TempC, &sc.SealIntact, &sc.Message, &sc.ScannedAt); err != nil {
			return nil, err
		}
		out = append(out, sc)
	}
	return out, rows.Err()
}
