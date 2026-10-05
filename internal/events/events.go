package events

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const Topic = "donation-batch-events"

// 事件类型
const (
	TypeBatchReported        = "batch.reported"
	TypeBatchReleased        = "batch.released"
	TypeBatchSoldOut         = "batch.sold_out"
	TypeBatchExpired         = "batch.expired"
	TypeBatchCancelled       = "batch.cancelled"
	TypeReservationMade      = "reservation.made"
	TypeReservationConfirmed = "reservation.confirmed"
	TypeReservationRejected  = "reservation.rejected"
	TypeReservationCancelled = "reservation.cancelled"
	TypeTruckLate            = "reservation.truck_late"
	TypeNoShow               = "reservation.no_show"
	TypeHandoverChecked      = "handover.checked"
	TypeHandoverSigned       = "handover.signed"
	TypeDeliveryPlan         = "delivery.plan_uploaded"
	TypeDistributed          = "distribution.recorded"
	TypeSpoiled              = "distribution.spoiled_feedback"
	TypeExceptionRaised      = "exception.raised"
)

type Envelope struct {
	EventID       string          `json:"event_id"`
	EventType     string          `json:"event_type"`
	AggregateType string          `json:"aggregate_type"`
	AggregateID   string          `json:"aggregate_id"`
	OccurredAt    time.Time       `json:"occurred_at"`
	Payload       json.RawMessage `json:"payload"`
}

// WriteOutbox 在领域事务内写 outbox（同事务保证事件不丢）。
func WriteOutbox(ctx context.Context, tx pgx.Tx, aggType string, aggID uuid.UUID, eventType string, payload any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO outbox_events(event_id, aggregate_type, aggregate_id, event_type, payload)
		VALUES ($1,$2,$3,$4,$5)`,
		uuid.New(), aggType, aggID, eventType, raw)
	return err
}
