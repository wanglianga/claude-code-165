package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"donation-service/internal/domain"
	"donation-service/internal/store"
)

// DeliveryStopInput 配送站点输入。
type DeliveryStopInput struct {
	Seq           int
	RecipientType string
	SiteName      string
	Address       string
	Lat           *float64
	Lng           *float64
	PlannedBoxes  int
}

// SubmitDeliveryPlan 公益机构签收后上传配送计划（车辆 + 站点 + 受助对象类型）。
func (s *Service) SubmitDeliveryPlan(ctx context.Context, u *domain.User, resvID int64,
	vehiclePlate, driverName string, coldChain bool, stops []DeliveryStopInput) (*domain.DeliveryPlan, []domain.DeliveryStop, error) {
	if vehiclePlate == "" || driverName == "" || len(stops) == 0 {
		return nil, nil, domain.ErrBadRequest("车牌、司机与至少一个配送站点必填")
	}
	var plan *domain.DeliveryPlan
	var out []domain.DeliveryStop
	err := store.WithTx(ctx, s.DB, func(tx pgx.Tx) error {
		r, err := lockReservation(ctx, tx, resvID)
		if err != nil {
			return err
		}
		if err := RequireCharity(u, r.CharityID); err != nil {
			return err
		}
		b, err := lockBatch(ctx, tx, r.BatchID)
		if err != nil {
			return err
		}
		if b.Status != domain.BatchHandedOver {
			return domain.ErrConflict(fmt.Sprintf("批次状态为 %s，不可提交配送计划", b.Status))
		}
		if b.RequiresColdChain && !coldChain {
			return domain.ErrUnprocessable("冷链批次配送必须使用冷藏车")
		}
		c, err := getCharity(ctx, tx, r.CharityID)
		if err != nil {
			return err
		}
		allowed := map[string]bool{}
		for _, t := range c.RecipientTypes {
			allowed[t] = true
		}
		total := 0
		seen := map[int]bool{}
		for _, st := range stops {
			if st.PlannedBoxes <= 0 {
				return domain.ErrBadRequest("站点计划箱数必须为正数")
			}
			if seen[st.Seq] {
				return domain.ErrBadRequest("站点序号重复")
			}
			seen[st.Seq] = true
			if !allowed[st.RecipientType] {
				return domain.ErrUnprocessable(fmt.Sprintf(
					"受助对象类型 %s 不在机构服务能力范围 %v 内", st.RecipientType, c.RecipientTypes))
			}
			total += st.PlannedBoxes
		}
		if total != b.TotalBoxes {
			return domain.ErrUnprocessable(fmt.Sprintf(
				"配送计划总箱数 %d 与交接箱数 %d 不一致", total, b.TotalBoxes))
		}
		var planID int64
		err = tx.QueryRow(ctx, `
			INSERT INTO delivery_plans(reservation_id, vehicle_plate, driver_name, cold_chain)
			VALUES($1,$2,$3,$4) RETURNING id`, resvID, vehiclePlate, driverName, coldChain).Scan(&planID)
		if isUniqueViolation(err) {
			return domain.ErrConflict("配送计划已提交，请勿重复提交")
		}
		if err != nil {
			return err
		}
		var submittedAt time.Time
		if err := tx.QueryRow(ctx, `SELECT submitted_at FROM delivery_plans WHERE id=$1`, planID).Scan(&submittedAt); err != nil {
			return err
		}
		plan = &domain.DeliveryPlan{ID: planID, ReservationID: resvID, VehiclePlate: vehiclePlate,
			DriverName: driverName, ColdChain: coldChain, SubmittedAt: submittedAt}
		for _, st := range stops {
			var id int64
			if err := tx.QueryRow(ctx, `
				INSERT INTO delivery_stops(plan_id, seq, recipient_type, site_name, address, lat, lng, planned_boxes)
				VALUES($1,$2,$3,$4,$5,$6,$7,$8) RETURNING id`,
				planID, st.Seq, st.RecipientType, st.SiteName, st.Address, st.Lat, st.Lng, st.PlannedBoxes).
				Scan(&id); err != nil {
				return err
			}
			out = append(out, domain.DeliveryStop{ID: id, PlanID: planID, Seq: st.Seq,
				RecipientType: st.RecipientType, SiteName: st.SiteName, Address: st.Address,
				Lat: st.Lat, Lng: st.Lng, PlannedBoxes: st.PlannedBoxes, Status: "planned"})
		}
		if _, err := tx.Exec(ctx,
			`UPDATE boxes SET status=$1 WHERE batch_id=$2 AND status=$3`,
			domain.BoxInDelivery, b.ID, domain.BoxHandedOver); err != nil {
			return err
		}
		if err := transitionBatch(ctx, tx, b.ID, domain.BatchHandedOver, domain.BatchInDelivery); err != nil {
			return err
		}
		if err := insertVoucher(ctx, tx, b.ID, &resvID, domain.VoucherDelivery, map[string]any{
			"plan_id": planID, "vehicle_plate": vehiclePlate, "driver_name": driverName,
			"cold_chain": coldChain, "stops": len(stops), "total_boxes": total,
		}); err != nil {
			return err
		}
		if err := insertOutbox(ctx, tx, s.Cfg.KafkaTopic, fmt.Sprintf("batch:%d", b.ID), "batch.delivery_planned", map[string]any{
			"batch_id": b.ID, "reservation_id": resvID, "plan_id": planID, "stops": len(stops),
		}); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	return plan, out, nil
}

// CompleteStop 完成一个配送站点的发放（幂等：站点唯一发放记录）。
func (s *Service) CompleteStop(ctx context.Context, u *domain.User, stopID int64,
	boxes int, receiverSign string) (*domain.Distribution, bool, error) {
	if boxes <= 0 || receiverSign == "" {
		return nil, false, domain.ErrBadRequest("发放箱数与签收人必填")
	}
	var dist *domain.Distribution
	finished := false
	err := store.WithTx(ctx, s.DB, func(tx pgx.Tx) error {
		var (
			planID, seq, plannedBoxes int64
			status, siteName, rtype   string
			resvID                    int64
		)
		err := tx.QueryRow(ctx, `
			SELECT st.id, st.plan_id, st.seq, st.planned_boxes, st.status, st.site_name, st.recipient_type, dp.reservation_id
			FROM delivery_stops st JOIN delivery_plans dp ON dp.id = st.plan_id
			WHERE st.id=$1 FOR UPDATE`, stopID).
			Scan(&stopID, &planID, &seq, &plannedBoxes, &status, &siteName, &rtype, &resvID)
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.ErrNotFound
		}
		if err != nil {
			return err
		}
		r, err := lockReservation(ctx, tx, resvID)
		if err != nil {
			return err
		}
		if err := RequireCharity(u, r.CharityID); err != nil {
			return err
		}
		if status == "completed" {
			// 幂等：返回已存在的发放记录
			d, derr := s.findDistributionByStop(ctx, tx, stopID)
			if derr != nil {
				return derr
			}
			dist = d
			return nil
		}
		if int64(boxes) != plannedBoxes {
			return domain.ErrUnprocessable(fmt.Sprintf(
				"实发箱数 %d 与计划箱数 %d 不一致", boxes, plannedBoxes))
		}
		b, err := lockBatch(ctx, tx, r.BatchID)
		if err != nil {
			return err
		}
		if b.Status != domain.BatchInDelivery {
			return domain.ErrConflict(fmt.Sprintf("批次状态为 %s，不可发放", b.Status))
		}
		var distID int64
		err = tx.QueryRow(ctx, `
			INSERT INTO distributions(stop_id, reservation_id, boxes, receiver_sign)
			VALUES($1,$2,$3,$4) RETURNING id`, stopID, resvID, boxes, receiverSign).Scan(&distID)
		if isUniqueViolation(err) {
			d, derr := s.findDistributionByStop(ctx, tx, stopID)
			if derr != nil {
				return derr
			}
			dist = d
			return nil
		}
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`UPDATE delivery_stops SET status='completed' WHERE id=$1`, stopID); err != nil {
			return err
		}
		if err := insertVoucher(ctx, tx, b.ID, &resvID, domain.VoucherDistribution, map[string]any{
			"distribution_id": distID, "stop_id": stopID, "site_name": siteName,
			"recipient_type": rtype, "boxes": boxes, "receiver_sign": receiverSign,
		}); err != nil {
			return err
		}
		// 全部站点完成 -> 批次闭环
		var remaining int
		if err := tx.QueryRow(ctx,
			`SELECT COUNT(*) FROM delivery_stops WHERE plan_id=$1 AND status='planned'`, planID).
			Scan(&remaining); err != nil {
			return err
		}
		if remaining == 0 {
			if _, err := tx.Exec(ctx,
				`UPDATE boxes SET status=$1 WHERE batch_id=$2 AND status=$3`,
				domain.BoxDistributed, b.ID, domain.BoxInDelivery); err != nil {
				return err
			}
			if err := transitionBatch(ctx, tx, b.ID, domain.BatchInDelivery, domain.BatchDistributed); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `
				UPDATE reservations SET status=$1, version=version+1, updated_at=now()
				WHERE id=$2`, domain.ResvCompleted, resvID); err != nil {
				return err
			}
			if err := insertVoucher(ctx, tx, b.ID, &resvID, domain.VoucherClosure, map[string]any{
				"reservation_no": r.ReservationNo, "total_boxes": b.TotalBoxes,
			}); err != nil {
				return err
			}
			// 捐赠公示与发放同事务落库，保证库存/交接/公示一致
			if err := s.projectPublicityTx(ctx, tx, b.ID); err != nil {
				return err
			}
			if err := insertOutbox(ctx, tx, s.Cfg.KafkaTopic, fmt.Sprintf("batch:%d", b.ID), "batch.distributed", map[string]any{
				"batch_id": b.ID, "reservation_id": resvID, "boxes": b.TotalBoxes,
			}); err != nil {
				return err
			}
			finished = true
		}
		dist, err = s.findDistributionByStop(ctx, tx, stopID)
		return err
	})
	return dist, finished, err
}

func (s *Service) findDistributionByStop(ctx context.Context, q txQuerier, stopID int64) (*domain.Distribution, error) {
	d := &domain.Distribution{}
	err := q.QueryRow(ctx, `
		SELECT id, stop_id, reservation_id, boxes, receiver_sign, distributed_at,
		       feedback_status, feedback_note, feedback_at
		FROM distributions WHERE stop_id=$1`, stopID).Scan(
		&d.ID, &d.StopID, &d.ReservationID, &d.Boxes, &d.ReceiverSign, &d.DistributedAt,
		&d.FeedbackStatus, &d.FeedbackNote, &d.FeedbackAt)
	return d, err
}

// SubmitFeedback 受助点反馈：变质则生成 spoilage_report 异常并标记箱状态。
func (s *Service) SubmitFeedback(ctx context.Context, u *domain.User, distID int64,
	status, note string) (*domain.Distribution, error) {
	if status != "ok" && status != "spoiled" {
		return nil, domain.ErrBadRequest("反馈状态取值: ok/spoiled")
	}
	var dist *domain.Distribution
	err := store.WithTx(ctx, s.DB, func(tx pgx.Tx) error {
		d, err := s.findDistributionByStop(ctx, tx, distID)
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.ErrNotFound
		}
		if err != nil {
			return err
		}
		r, err := lockReservation(ctx, tx, d.ReservationID)
		if err != nil {
			return err
		}
		if err := RequireCharity(u, r.CharityID); err != nil {
			return err
		}
		if d.FeedbackStatus != "none" {
			return domain.ErrConflict("该发放记录已有反馈")
		}
		if _, err := tx.Exec(ctx, `
			UPDATE distributions SET feedback_status=$1, feedback_note=$2, feedback_at=now()
			WHERE id=$3`, status, note, distID); err != nil {
			return err
		}
		if status == "spoiled" {
			detail := map[string]any{
				"distribution_id": distID, "stop_id": d.StopID, "note": note,
				"reported_by": u.ID,
			}
			exID, ierr := insertException(ctx, tx, &r.BatchID, &r.ID, domain.ExSpoilageReport, detail, &u.ID)
			if ierr != nil {
				return ierr
			}
			if ierr := s.emitExceptionEvent(ctx, tx, exID, &r.BatchID, &r.ID, domain.ExSpoilageReport, detail); ierr != nil {
				return ierr
			}
			// 该站点发放的箱标记为变质，便于追溯
			if _, ierr := tx.Exec(ctx, `
				UPDATE boxes SET status=$1
				WHERE batch_id=$2 AND status=$3
				  AND id IN (SELECT id FROM boxes WHERE batch_id=$2 AND status=$3 LIMIT $4)`,
				domain.BoxSpoiled, r.BatchID, domain.BoxDistributed, d.Boxes); ierr != nil {
				return ierr
			}
		}
		if err := insertOutbox(ctx, tx, s.Cfg.KafkaTopic, fmt.Sprintf("batch:%d", r.BatchID), "distribution.feedback", map[string]any{
			"batch_id": r.BatchID, "distribution_id": distID, "status": status,
		}); err != nil {
			return err
		}
		dist, err = s.findDistributionByStop(ctx, tx, distID)
		return err
	})
	return dist, err
}

// GetDeliveryDetail 查询配送计划与站点、发放记录。
func (s *Service) GetDeliveryDetail(ctx context.Context, resvID int64) (map[string]any, error) {
	var plan domain.DeliveryPlan
	err := s.DB.QueryRow(ctx, `
		SELECT id, reservation_id, vehicle_plate, driver_name, cold_chain, submitted_at
		FROM delivery_plans WHERE reservation_id=$1`, resvID).Scan(
		&plan.ID, &plan.ReservationID, &plan.VehiclePlate, &plan.DriverName, &plan.ColdChain, &plan.SubmittedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	rows, err := s.DB.Query(ctx, `
		SELECT id, plan_id, seq, recipient_type, site_name, address, lat, lng, planned_boxes, status
		FROM delivery_stops WHERE plan_id=$1 ORDER BY seq`, plan.ID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var stops []domain.DeliveryStop
	var stopIDs []int64
	for rows.Next() {
		var st domain.DeliveryStop
		if err := rows.Scan(&st.ID, &st.PlanID, &st.Seq, &st.RecipientType, &st.SiteName,
			&st.Address, &st.Lat, &st.Lng, &st.PlannedBoxes, &st.Status); err != nil {
			return nil, err
		}
		stops = append(stops, st)
		stopIDs = append(stopIDs, st.ID)
	}
	var dists []domain.Distribution
	if len(stopIDs) > 0 {
		drows, err := s.DB.Query(ctx, `
			SELECT id, stop_id, reservation_id, boxes, receiver_sign, distributed_at,
			       feedback_status, feedback_note, feedback_at
			FROM distributions WHERE stop_id = ANY($1) ORDER BY id`, stopIDs)
		if err != nil {
			return nil, err
		}
		defer drows.Close()
		for drows.Next() {
			var d domain.Distribution
			if err := drows.Scan(&d.ID, &d.StopID, &d.ReservationID, &d.Boxes, &d.ReceiverSign,
				&d.DistributedAt, &d.FeedbackStatus, &d.FeedbackNote, &d.FeedbackAt); err != nil {
				return nil, err
			}
			dists = append(dists, d)
		}
	}
	return map[string]any{"plan": plan, "stops": stops, "distributions": dists}, nil
}
