package events

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)

// StartOutboxRelay 轮询 outbox 表，把与业务同事务落库的事件可靠投递到 Kafka。
func StartOutboxRelay(ctx context.Context, pool *pgxpool.Pool, writer *kafka.Writer) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := publishPending(ctx, pool, writer); err != nil {
				log.Printf("[outbox-relay] %v", err)
			}
		}
	}
}

func publishPending(ctx context.Context, pool *pgxpool.Pool, writer *kafka.Writer) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `
		SELECT id, topic, key, payload FROM outbox
		WHERE published_at IS NULL ORDER BY id LIMIT 100
		FOR UPDATE SKIP LOCKED`)
	if err != nil {
		return err
	}
	type item struct {
		id      int64
		topic   string
		key     string
		payload []byte
	}
	var items []item
	for rows.Next() {
		var it item
		if err := rows.Scan(&it.id, &it.topic, &it.key, &it.payload); err != nil {
			rows.Close()
			return err
		}
		items = append(items, it)
	}
	rows.Close()
	if len(items) == 0 {
		return nil
	}
	var msgs []kafka.Message
	for _, it := range items {
		msgs = append(msgs, kafka.Message{
			Topic: it.topic,
			Key:   []byte(it.key),
			Value: it.payload,
		})
	}
	if err := writer.WriteMessages(ctx, msgs...); err != nil {
		return err
	}
	ids := make([]int64, 0, len(items))
	for _, it := range items {
		ids = append(ids, it.id)
	}
	if _, err := tx.Exec(ctx,
		`UPDATE outbox SET published_at=now() WHERE id = ANY($1)`, ids); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Event 是 outbox 中事件的标准结构。
type Event struct {
	Event      string          `json:"event"`
	OccurredAt string          `json:"occurred_at"`
	Data       json.RawMessage `json:"data"`
}
