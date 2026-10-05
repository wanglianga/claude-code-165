package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	"bakery-donation/internal/apperror"
	"bakery-donation/internal/config"
	"bakery-donation/internal/database"
	"bakery-donation/internal/events"
	"bakery-donation/internal/lock"
	"bakery-donation/internal/projector"
	"bakery-donation/internal/voucher"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/redis/go-redis/v9"
)

type Service struct {
	db        *database.DB
	rdb       *redis.Client
	locker    *lock.Locker
	cfg       config.Config
	projector *projector.Projector
}

func New(db *database.DB, rdb *redis.Client, cfg config.Config, proj *projector.Projector) *Service {
	return &Service{db: db, rdb: rdb, locker: lock.New(rdb), cfg: cfg, projector: proj}
}

// ---------- 通用辅助 ----------

func (s *Service) begin(ctx context.Context) (pgx.Tx, error) { return s.db.Pool.Begin(ctx) }

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

func codePrefix(prefix string) string {
	b := make([]byte, 5)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%s-%s-%s", prefix, time.Now().Format("20060102"), strings.ToUpper(hex.EncodeToString(b)))
}

func codePrefixRaw(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func digits(n int) string {
	max := big.NewInt(int64(n))
	v, _ := rand.Int(rand.Reader, max)
	return fmt.Sprintf("%0*d", len(fmt.Sprintf("%d", n-1)), v.Int64())
}

func maskID(id string) string {
	id = strings.TrimSpace(id)
	if len(id) <= 6 {
		return strings.Repeat("*", len(id))
	}
	return id[:3] + strings.Repeat("*", len(id)-6) + id[len(id)-3:]
}

func maskPhone(p string) string {
	if len(p) < 7 {
		return strings.Repeat("*", len(p))
	}
	return p[:3] + "****" + p[len(p)-4:]
}

// appendVoucher 在批次哈希链上追加一节凭证（须在事务内调用）。
func (s *Service) appendVoucher(ctx context.Context, tx pgx.Tx, batchID uuid.UUID,
	vType, title string, payload map[string]any) error {
	var seq int
	var prev string
	err := tx.QueryRow(ctx,
		`SELECT COALESCE(MAX(seq),0), COALESCE((SELECT chain_hash FROM vouchers
		          WHERE batch_id=$1 ORDER BY seq DESC LIMIT 1), $2)
		 FROM vouchers WHERE batch_id=$1`, batchID, voucher.Genesis).
		Scan(&seq, &prev)
	if err != nil {
		return err
	}
	pHash := voucher.PayloadHash(payload)
	cHash := voucher.ChainHash(prev, pHash)
	pb, _ := json.Marshal(payload)
	_, err = tx.Exec(ctx, `
		INSERT INTO vouchers(batch_id, seq, voucher_type, title, payload, payload_hash, prev_hash, chain_hash)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
		batchID, seq+1, vType, title, pb, pHash, prev, cHash)
	return err
}

func (s *Service) raiseException(ctx context.Context, tx pgx.Tx, batchID *uuid.UUID,
	exType string, detail map[string]any, blocking bool) error {
	var bid any
	if batchID != nil {
		bid = *batchID
	}
	db, _ := json.Marshal(detail)
	var id uuid.UUID
	err := tx.QueryRow(ctx, `
		INSERT INTO exception_records(batch_id, ex_type, detail, blocking, raised_by)
		VALUES ($1,$2,$3,$4, NULL) RETURNING id`, bid, exType, db, blocking).Scan(&id)
	if err != nil {
		return err
	}
	if batchID != nil {
		return events.WriteOutbox(ctx, tx, "donation_batch", *batchID, events.TypeExceptionRaised,
			map[string]any{"exception_id": id.String(), "type": exType, "blocking": blocking, "detail": detail})
	}
	return nil
}

func resolveExceptions(ctx context.Context, tx pgx.Tx, batchID uuid.UUID, exTypes []string, resolution string) error {
	_, err := tx.Exec(ctx, `
		UPDATE exception_records SET resolved=true, resolution=$2, resolved_at=now()
		WHERE batch_id=$1 AND resolved=false AND ex_type = ANY($3)`,
		batchID, resolution, exTypes)
	return err
}

// resetForRebook 已确认预约被撤销/爽约后，清理交接产物使批次可被重新预约。
func resetForRebook(ctx context.Context, tx pgx.Tx, batchID uuid.UUID) error {
	if _, err := tx.Exec(ctx, `DELETE FROM handovers WHERE batch_id=$1`, batchID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE boxes SET seal_no=NULL, scan_status='pending', scanned_at=NULL,
		       measured_temp_c=NULL, seal_ok=NULL, checked_qty=NULL,
		       reject_reason=NULL, delivered_at=NULL, distributed_at=NULL
		WHERE batch_id=$1`, batchID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM temperature_readings WHERE batch_id=$1`, batchID); err != nil {
		return err
	}
	return nil
}

func emit(ctx context.Context, tx pgx.Tx, batchID uuid.UUID, eventType string, payload any) error {
	return events.WriteOutbox(ctx, tx, "donation_batch", batchID, eventType, payload)
}

// batchGuard 校验批次存在并返回门店归属
func batchOwner(ctx context.Context, tx pgx.Tx, batchID uuid.UUID) (storeID uuid.UUID, status string, err error) {
	err = tx.QueryRow(ctx, `SELECT store_id, status FROM donation_batches WHERE id=$1`, batchID).
		Scan(&storeID, &status)
	if err == pgx.ErrNoRows {
		return uuid.Nil, "", apperror.ErrNotFound
	}
	return
}

func parseUUID(s string) (uuid.UUID, error) {
	return uuid.Parse(s)
}

func mustUUID(s string) uuid.UUID { return uuid.MustParse(s) }

func mustUUIDP(s string) *uuid.UUID { u := uuid.MustParse(s); return &u }
