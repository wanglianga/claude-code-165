package events

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)

// Relay 轮询 outbox 未发布事件，投递到 Kafka 后回写 published_at。
// at-least-once：崩溃后重复投递由消费端 processed_events 幂等去重。
type Relay struct {
	pool    *pgxpool.Pool
	writer  *kafka.Writer
	enabled bool
	batch   int
}

func NewRelay(brokers []string, topic string, pool *pgxpool.Pool, enabled bool) *Relay {
	r := &Relay{pool: pool, enabled: enabled, batch: 100}
	if enabled {
		r.writer = &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        topic,
			Balancer:     &kafka.LeastBytes{},
			BatchTimeout: 50 * time.Millisecond,
			RequiredAcks: kafka.RequireAll,
		}
	}
	return r
}

func (r *Relay) Run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	if r.writer != nil {
		defer r.writer.Close()
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.pumpOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("relay pump error: %v", err)
			}
		}
	}
}

// pumpOnce 抓取一批未发布事件并投递。
func (r *Relay) pumpOnce(ctx context.Context) error {
	if !r.enabled {
		// 本地模式：投影器直接轮询 outbox
		return nil
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		SELECT id, event_id, aggregate_type, aggregate_id, event_type, payload, occurred_at
		FROM outbox_events
		WHERE published_at IS NULL
		ORDER BY id
		LIMIT $1
		FOR UPDATE SKIP LOCKED`, r.batch)
	if err != nil {
		return err
	}
	type pending struct {
		id  int64
		env Envelope
	}
	var pend []pending
	for rows.Next() {
		var p pending
		if err := rows.Scan(&p.id, &p.env.EventID, &p.env.AggregateType,
			&p.env.AggregateID, &p.env.EventType, &p.env.Payload, &p.env.OccurredAt); err != nil {
			rows.Close()
			return err
		}
		pend = append(pend, p)
	}
	rows.Close()
	if len(pend) == 0 {
		return tx.Rollback(ctx)
	}

	if r.enabled {
		msgs := make([]kafka.Message, 0, len(pend))
		for _, p := range pend {
			raw, _ := json.Marshal(p.env)
			msgs = append(msgs, kafka.Message{Key: []byte(p.env.AggregateID), Value: raw})
		}
		if err := r.writer.WriteMessages(ctx, msgs...); err != nil {
			return err
		}
	}
	// 本地模式（无 Kafka）：投影器直接从 outbox 消费，relay 不置位；
	// 由 LocalDrain 拉取处理。Kafka 模式下置位已发布。
	for _, p := range pend {
		if _, err := tx.Exec(ctx,
			`UPDATE outbox_events SET published_at=now() WHERE id=$1`, p.id); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
