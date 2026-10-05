package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"donation-service/internal/config"
	"donation-service/internal/domain"
)

// Service 聚合全部业务用例，依赖 PostgreSQL / Redis / Kafka(outbox)。
type Service struct {
	DB  *pgxpool.Pool
	RDB *redis.Client
	Cfg config.Config
	// ProjectPublicity 在 Kafka 不可用时用于同步投影，默认 nil 表示仅依赖 consumer。
	ProjectInline bool
}

func New(db *pgxpool.Pool, rdb *redis.Client, cfg config.Config) *Service {
	return &Service{DB: db, RDB: rdb, Cfg: cfg}
}

// ---------- 通用查询 ----------

type txQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "23505"
	}
	return false
}

// lockBatch 锁定并返回批次行（FOR UPDATE），保证同一批次的写操作串行，
// 既保护状态机也保护凭证哈希链。
func lockBatch(ctx context.Context, tx txQuerier, id int64) (*domain.Batch, error) {
	b := &domain.Batch{}
	err := tx.QueryRow(ctx, `
		SELECT id, batch_no, store_id, status, category, description, total_boxes, quantity_desc,
		       produced_at, shelf_life_hours, expires_at, allergens, allergen_confirmed,
		       requires_cold_chain, storage_temp_max_c, packaging, address, lat, lng,
		       latest_pickup_at, notes, version, created_at, updated_at
		FROM batches WHERE id=$1 FOR UPDATE`, id).Scan(
		&b.ID, &b.BatchNo, &b.StoreID, &b.Status, &b.Category, &b.Description, &b.TotalBoxes,
		&b.QuantityDesc, &b.ProducedAt, &b.ShelfLifeHours, &b.ExpiresAt, &b.Allergens,
		&b.AllergenConfirmed, &b.RequiresColdChain, &b.StorageTempMaxC, &b.Packaging,
		&b.Address, &b.Lat, &b.Lng, &b.LatestPickupAt, &b.Notes, &b.Version, &b.CreatedAt, &b.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return b, nil
}

func getBatch(ctx context.Context, q txQuerier, id int64) (*domain.Batch, error) {
	b := &domain.Batch{}
	err := q.QueryRow(ctx, `
		SELECT id, batch_no, store_id, status, category, description, total_boxes, quantity_desc,
		       produced_at, shelf_life_hours, expires_at, allergens, allergen_confirmed,
		       requires_cold_chain, storage_temp_max_c, packaging, address, lat, lng,
		       latest_pickup_at, notes, version, created_at, updated_at
		FROM batches WHERE id=$1`, id).Scan(
		&b.ID, &b.BatchNo, &b.StoreID, &b.Status, &b.Category, &b.Description, &b.TotalBoxes,
		&b.QuantityDesc, &b.ProducedAt, &b.ShelfLifeHours, &b.ExpiresAt, &b.Allergens,
		&b.AllergenConfirmed, &b.RequiresColdChain, &b.StorageTempMaxC, &b.Packaging,
		&b.Address, &b.Lat, &b.Lng, &b.LatestPickupAt, &b.Notes, &b.Version, &b.CreatedAt, &b.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return b, nil
}

func getCharity(ctx context.Context, q txQuerier, id int64) (*domain.Charity, error) {
	c := &domain.Charity{}
	err := q.QueryRow(ctx, `
		SELECT id, code, name, address, lat, lng, service_radius_km, daily_capacity_boxes,
		       cold_chain_vehicles, cold_chain_capacity_boxes, recipient_types, contact_phone, created_at
		FROM charities WHERE id=$1`, id).Scan(
		&c.ID, &c.Code, &c.Name, &c.Address, &c.Lat, &c.Lng, &c.ServiceRadiusKm,
		&c.DailyCapacityBoxes, &c.ColdChainVehicles, &c.ColdChainCapacityBoxes,
		&c.RecipientTypes, &c.ContactPhone, &c.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return c, nil
}

func getReservation(ctx context.Context, q txQuerier, id int64) (*domain.Reservation, error) {
	r := &domain.Reservation{}
	err := q.QueryRow(ctx, `
		SELECT id, reservation_no, batch_id, charity_id, status, COALESCE(idempotency_key,''),
		       boxes, planned_pickup_at, vehicle_plate, driver_name, cold_chain,
		       receiver_name, receiver_phone, receiver_id_no, cancel_reason,
		       version, created_at, updated_at
		FROM reservations WHERE id=$1`, id).Scan(
		&r.ID, &r.ReservationNo, &r.BatchID, &r.CharityID, &r.Status, &r.IdempotencyKey,
		&r.Boxes, &r.PlannedPickupAt, &r.VehiclePlate, &r.DriverName, &r.ColdChain,
		&r.ReceiverName, &r.ReceiverPhone, &r.ReceiverIDNo, &r.CancelReason,
		&r.Version, &r.CreatedAt, &r.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return r, nil
}

func lockReservation(ctx context.Context, tx txQuerier, id int64) (*domain.Reservation, error) {
	r := &domain.Reservation{}
	err := tx.QueryRow(ctx, `
		SELECT id, reservation_no, batch_id, charity_id, status, COALESCE(idempotency_key,''),
		       boxes, planned_pickup_at, vehicle_plate, driver_name, cold_chain,
		       receiver_name, receiver_phone, receiver_id_no, cancel_reason,
		       version, created_at, updated_at
		FROM reservations WHERE id=$1 FOR UPDATE`, id).Scan(
		&r.ID, &r.ReservationNo, &r.BatchID, &r.CharityID, &r.Status, &r.IdempotencyKey,
		&r.Boxes, &r.PlannedPickupAt, &r.VehiclePlate, &r.DriverName, &r.ColdChain,
		&r.ReceiverName, &r.ReceiverPhone, &r.ReceiverIDNo, &r.CancelReason,
		&r.Version, &r.CreatedAt, &r.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return r, nil
}

// transitionBatch 带状态守卫的批次状态迁移（乐观并发：WHERE status=旧状态）。
func transitionBatch(ctx context.Context, tx txQuerier, batchID int64, from, to string) error {
	if !domain.CanTransition(from, to) {
		return domain.ErrConflict(fmt.Sprintf("批次状态不允许 %s -> %s", from, to))
	}
	tag, err := tx.Exec(ctx, `
		UPDATE batches SET status=$1, version=version+1, updated_at=now()
		WHERE id=$2 AND status=$3`, to, batchID, from)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrConflict("批次状态已变化，请刷新后重试")
	}
	return nil
}

// ---------- 凭证 / 事件 / 异常（同事务写入） ----------

func insertVoucher(ctx context.Context, tx txQuerier, batchID int64, reservationID *int64,
	vtype string, payload map[string]any) error {
	var prev string
	err := tx.QueryRow(ctx,
		`SELECT hash FROM vouchers WHERE batch_id=$1 ORDER BY id DESC LIMIT 1`, batchID).Scan(&prev)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	payloadBytes := domain.MarshalPayload(payload)
	// timestamptz 精度为微秒，先截断保证落库后重算哈希一致
	now := time.Now().UTC().Truncate(time.Microsecond)
	hash := domain.ComputeVoucherHash(prev, vtype, payloadBytes, now)
	_, err = tx.Exec(ctx, `
		INSERT INTO vouchers(batch_id, reservation_id, type, payload, hash, prev_hash, created_at)
		VALUES($1,$2,$3,$4,$5,$6,$7)`,
		batchID, reservationID, vtype, payloadBytes, hash, prev, now)
	return err
}

func insertOutbox(ctx context.Context, tx txQuerier, topic, key, eventType string, payload map[string]any) error {
	body, err := json.Marshal(map[string]any{
		"event":       eventType,
		"occurred_at": time.Now().UTC().Format(time.RFC3339Nano),
		"data":        payload,
	})
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx,
		`INSERT INTO outbox(topic, key, payload) VALUES($1,$2,$3)`, topic, key, body)
	return err
}

func insertException(ctx context.Context, tx txQuerier, batchID, reservationID *int64,
	exType string, detail map[string]any, createdBy *int64) (int64, error) {
	var id int64
	err := tx.QueryRow(ctx, `
		INSERT INTO exceptions(ex_no, batch_id, reservation_id, type, detail, created_by)
		VALUES('EX' || to_char(now(),'YYYYMMDD') || '-' || lpad(nextval('ex_no_seq')::text, 6, '0'),
		       $1, $2, $3, $4, $5)
		RETURNING id`, batchID, reservationID, exType, domain.MarshalPayload(detail), createdBy).Scan(&id)
	return id, err
}

// emitExceptionEvent 异常凭证 + outbox 事件。
func (s *Service) emitExceptionEvent(ctx context.Context, tx txQuerier, exID int64,
	batchID, reservationID *int64, exType string, detail map[string]any) error {
	if batchID != nil {
		if err := insertVoucher(ctx, tx, *batchID, reservationID, domain.VoucherException, map[string]any{
			"exception_id": exID, "type": exType, "detail": detail,
		}); err != nil {
			return err
		}
	}
	key := ""
	if batchID != nil {
		key = fmt.Sprintf("batch:%d", *batchID)
	}
	return insertOutbox(ctx, tx, s.Cfg.KafkaTopic, key, "exception.raised", map[string]any{
		"exception_id": exID, "type": exType, "batch_id": batchID, "reservation_id": reservationID,
	})
}

// ---------- 权限辅助 ----------

// RequireStore 校验用户属于指定门店。
func RequireStore(u *domain.User, storeID int64) error {
	if u.Role == domain.RoleAdmin {
		return nil
	}
	if (u.Role == domain.RoleStoreStaff || u.Role == domain.RoleStoreManager) &&
		u.StoreID != nil && *u.StoreID == storeID {
		return nil
	}
	return domain.ErrForbidden
}

// RequireCharity 校验用户属于指定公益机构。
func RequireCharity(u *domain.User, charityID int64) error {
	if u.Role == domain.RoleAdmin {
		return nil
	}
	if u.Role == domain.RoleCharityStaff && u.CharityID != nil && *u.CharityID == charityID {
		return nil
	}
	return domain.ErrForbidden
}
