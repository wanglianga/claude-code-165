package projector

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"

	"bakery-donation/internal/database"
	"bakery-donation/internal/events"

	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

// Projector 消费批次事件，重建捐赠公示读模型并刷新 Redis 统计。
// 幂等：processed_events 去重；公示行始终从权威表重算，避免事件字段漂移。
type Projector struct {
	db      *database.DB
	rdb     *redis.Client
	brokers []string
	topic   string
	enabled bool
	group   string
	batch   int
}

func New(db *database.DB, rdb *redis.Client, brokers []string, topic string, enabled bool) *Projector {
	return &Projector{db: db, rdb: rdb, brokers: brokers, topic: topic,
		enabled: enabled, group: "donation-ledger-projector", batch: 100}
}

func (p *Projector) Run(ctx context.Context) {
	if p.enabled {
		p.runKafka(ctx)
	} else {
		p.runLocalOutbox(ctx)
	}
}

// ---- Kafka 来源 ----
func (p *Projector) runKafka(ctx context.Context) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     p.brokers,
		Topic:       p.topic,
		GroupID:     p.group,
		MinBytes:    1,
		MaxBytes:    10e6,
		StartOffset: kafka.FirstOffset,
	})
	defer reader.Close()
	for {
		m, err := reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			log.Printf("projector fetch: %v", err)
			time.Sleep(time.Second)
			continue
		}
		var env events.Envelope
		if err := json.Unmarshal(m.Value, &env); err != nil {
			log.Printf("projector bad message: %v", err)
			_ = reader.CommitMessages(ctx, m)
			continue
		}
		if err := p.handle(ctx, env); err != nil {
			log.Printf("projector handle %s: %v", env.EventType, err)
			time.Sleep(time.Second)
			continue
		}
		if err := reader.CommitMessages(ctx, m); err != nil {
			log.Printf("projector commit: %v", err)
		}
	}
}

// ---- 本地来源（无 Kafka 时直接读 outbox） ----
func (p *Projector) runLocalOutbox(ctx context.Context) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := p.drainLocal(ctx); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("projector local drain: %v", err)
			}
		}
	}
}

func (p *Projector) drainLocal(ctx context.Context) error {
	tx, err := p.db.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `
		SELECT id, event_id, aggregate_type, aggregate_id, event_type, payload, occurred_at
		FROM outbox_events
		ORDER BY id
		LIMIT $1
		FOR UPDATE SKIP LOCKED`, p.batch)
	if err != nil {
		return err
	}
	type rec struct {
		id  int64
		env events.Envelope
	}
	var recs []rec
	handled := map[string]bool{}
	for rows.Next() {
		var r rec
		if err := rows.Scan(&r.id, &r.env.EventID, &r.env.AggregateType,
			&r.env.AggregateID, &r.env.EventType, &r.env.Payload, &r.env.OccurredAt); err != nil {
			rows.Close()
			return err
		}
		recs = append(recs, r)
	}
	rows.Close()
	var applied []events.Envelope
	for _, r := range recs {
		var exists bool
		if err := tx.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM processed_events WHERE event_id=$1)`, r.env.EventID).Scan(&exists); err != nil {
			return err
		}
		if exists {
			if _, err := tx.Exec(ctx, `UPDATE outbox_events SET published_at=now() WHERE id=$1`, r.id); err != nil {
				return err
			}
			continue
		}
		if !handled[r.env.AggregateID] {
			if err := projectBatch(ctx, tx, r.env.AggregateID); err != nil {
				return err
			}
			handled[r.env.AggregateID] = true
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO processed_events(event_id) VALUES($1)`, r.env.EventID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE outbox_events SET published_at=now() WHERE id=$1`, r.id); err != nil {
			return err
		}
		applied = append(applied, r.env)
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	for _, env := range applied {
		p.refreshRedis(ctx, env.AggregateID)
	}
	return nil
}

func (p *Projector) handle(ctx context.Context, env events.Envelope) error {
	var applied bool
	err := pgx.BeginTxFunc(ctx, p.db.Pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
		var exists bool
		if err := tx.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM processed_events WHERE event_id=$1)`, env.EventID).Scan(&exists); err != nil {
			return err
		}
		if exists {
			return nil
		}
		if err := projectBatch(ctx, tx, env.AggregateID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO processed_events(event_id) VALUES($1)`, env.EventID); err != nil {
			return err
		}
		applied = true
		return nil
	})
	if err != nil {
		return err
	}
	if applied {
		p.refreshRedis(ctx, env.AggregateID)
	}
	return nil
}

// projectBatch 从权威表重算单个批次的公示行。
func projectBatch(ctx context.Context, tx pgx.Tx, batchID string) error {
	var (
		code, title, storeName, status, reportType, charityName *string
		totalQty, totalBoxes                                    int
		categories                                              string
		reportedAt, handedAt, distAt                            *time.Time
	)
	err := tx.QueryRow(ctx, `
		SELECT b.code, b.title, s.name, b.status, COALESCE(c.name, ''),
		       b.total_quantity, b.total_boxes, b.report_type,
		       (SELECT string_agg(DISTINCT i.category, ',') FROM batch_items i WHERE i.batch_id=b.id),
		       b.created_at, b.handed_over_at, b.distributed_at
		FROM donation_batches b
		JOIN stores s ON s.id = b.store_id
		LEFT JOIN (
		    SELECT r.batch_id, ch.name
		    FROM reservations r JOIN charities ch ON ch.id = r.charity_id
		    WHERE r.status IN ('confirmed')
		) c ON c.batch_id = b.id
		WHERE b.id = $1`, batchID).
		Scan(&code, &title, &storeName, &status, &charityName,
			&totalQty, &totalBoxes, &reportType, &categories,
			&reportedAt, &handedAt, &distAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}

	// 五类凭证是否齐全
	var fiveComplete bool
	if err := tx.QueryRow(ctx, `
		SELECT COUNT(DISTINCT voucher_type) = 5
		FROM vouchers WHERE batch_id=$1
		  AND voucher_type IN ('source','temperature','handover','delivery','distribution')`,
		batchID).Scan(&fiveComplete); err != nil {
		return err
	}

	// 受助点变质反馈 → spoiled；门店售罄/超时 → wasted；取消 → cancelled；
	// 全部箱体发放完成 → donated；其余进行中。
	result := "in_progress"
	switch *status {
	case "distributed":
		result = "donated"
	case "quarantined":
		result = "spoiled"
	case "sold_out", "expired":
		result = "wasted"
	case "cancelled":
		result = "cancelled"
	}
	if *status == "distributed" {
		var undistributed int
		if err := tx.QueryRow(ctx,
			`SELECT COUNT(*) FROM boxes WHERE batch_id=$1 AND distributed_at IS NULL`,
			batchID).Scan(&undistributed); err != nil {
			return err
		}
		if undistributed > 0 {
			result = "in_progress"
		}
	}

	var cName any
	if charityName != nil && *charityName != "" {
		cName = *charityName
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO public_ledger
		    (batch_id, batch_code, store_name, charity_name, title, categories,
		     total_quantity, total_boxes, report_type, status, result,
		     reported_at, handed_over_at, distributed_at, vouchers_complete)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
		ON CONFLICT (batch_id) DO UPDATE SET
		    store_name=EXCLUDED.store_name, charity_name=EXCLUDED.charity_name,
		    title=EXCLUDED.title, categories=EXCLUDED.categories,
		    total_quantity=EXCLUDED.total_quantity, total_boxes=EXCLUDED.total_boxes,
		    report_type=EXCLUDED.report_type, status=EXCLUDED.status, result=EXCLUDED.result,
		    handed_over_at=EXCLUDED.handed_over_at, distributed_at=EXCLUDED.distributed_at,
		    vouchers_complete=EXCLUDED.vouchers_complete, updated_at=now()`,
		batchID, *code, *storeName, cName, *title, categories,
		totalQty, totalBoxes, *reportType, *status, result,
		reportedAt, handedAt, distAt, fiveComplete)
	return err
}

func (p *Projector) refreshRedis(ctx context.Context, batchID string) {
	if p.rdb == nil {
		return
	}
	var status, result, code string
	var boxes, qty int
	err := p.db.Pool.QueryRow(ctx,
		`SELECT status, result, total_boxes, total_quantity, batch_code
		 FROM public_ledger WHERE batch_id=$1`, batchID).
		Scan(&status, &result, &boxes, &qty, &code)
	if err != nil {
		return
	}
	pipe := p.rdb.Pipeline()
	// 仅在结果状态首次迁移时计数，保证多事件不重复累加
	prev, _ := p.rdb.HGet(ctx, "bakery:batch:"+code, "result").Result()
	pipe.HSet(ctx, "bakery:batch:"+code, "status", status, "result", result)
	if prev != result {
		switch result {
		case "donated":
			pipe.HIncrBy(ctx, "bakery:stats", "donated_batches", 1)
			pipe.HIncrBy(ctx, "bakery:stats", "donated_boxes", int64(boxes))
			pipe.HIncrBy(ctx, "bakery:stats", "donated_quantity", int64(qty))
			if prev == "wasted" || prev == "spoiled" {
				undoTerminalCount(pipe, prev, boxes, qty)
			}
		case "wasted", "spoiled":
			pipe.HIncrBy(ctx, "bakery:stats", result+"_batches", 1)
			if result == "wasted" {
				pipe.HIncrBy(ctx, "bakery:stats", "wasted_boxes", int64(boxes))
			}
			if prev == "donated" {
				undoTerminalCount(pipe, "donated", boxes, qty)
			}
		}
	}
	_, _ = pipe.Exec(ctx)
}

func undoTerminalCount(pipe redis.Pipeliner, prevResult string, boxes, qty int) {
	switch prevResult {
	case "donated":
		pipe.HIncrBy(context.Background(), "bakery:stats", "donated_batches", -1)
		pipe.HIncrBy(context.Background(), "bakery:stats", "donated_boxes", int64(-boxes))
		pipe.HIncrBy(context.Background(), "bakery:stats", "donated_quantity", int64(-qty))
	case "wasted":
		pipe.HIncrBy(context.Background(), "bakery:stats", "wasted_batches", -1)
		pipe.HIncrBy(context.Background(), "bakery:stats", "wasted_boxes", int64(-boxes))
	case "spoiled":
		pipe.HIncrBy(context.Background(), "bakery:stats", "spoiled_batches", -1)
	}
}

// Reconcile 从权威表全量重建公示与 Redis 统计（监管对账/修复用）。
func (p *Projector) Reconcile(ctx context.Context) (int, error) {
	ids := []string{}
	err := pgx.BeginTxFunc(ctx, p.db.Pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT id FROM donation_batches ORDER BY created_at`)
		if err != nil {
			return err
		}
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return err
			}
			ids = append(ids, id)
		}
		rows.Close()
		for _, id := range ids {
			if err := projectBatch(ctx, tx, id); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	if p.rdb != nil {
		// 统计从公示表精确重算后整体覆盖，消除增量误差
		var donatedB, wastedB, spoiledB, donatedBoxes, donatedQty, wastedBoxes int64
		rows, err := p.db.Pool.Query(ctx, `
			SELECT result, COUNT(*), COALESCE(SUM(total_boxes),0), COALESCE(SUM(total_quantity),0)
			FROM public_ledger GROUP BY result`)
		if err == nil {
			for rows.Next() {
				var result string
				var n, b, q int64
				_ = rows.Scan(&result, &n, &b, &q)
				switch result {
				case "donated":
					donatedB, donatedBoxes, donatedQty = n, b, q
				case "wasted":
					wastedB, wastedBoxes = n, b
				case "spoiled":
					spoiledB = n
				}
			}
			rows.Close()
			_ = p.rdb.HSet(ctx, "bakery:stats",
				"donated_batches", donatedB, "donated_boxes", donatedBoxes,
				"donated_quantity", donatedQty,
				"wasted_batches", wastedB, "wasted_boxes", wastedBoxes,
				"spoiled_batches", spoiledB).Err()
		}
	}
	return len(ids), nil
}
