package events

import (
	"context"
	"encoding/json"
	"log"

	"github.com/segmentio/kafka-go"

	"donation-service/internal/service"
)

// StartConsumer 消费领域事件：发放完成时幂等投影捐赠公示（服务内同事务已投影，
// 此处作为流式兜底与审计日志），并打印全部事件便于观察链路。
func StartConsumer(ctx context.Context, svc *service.Service, brokers []string, topic string) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		GroupID:  "donation-publicity-projector",
		MinBytes: 1,
		MaxBytes: 10 << 20,
	})
	defer reader.Close()
	log.Printf("[consumer] subscribed topic=%s brokers=%v", topic, brokers)
	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("[consumer] read: %v", err)
			continue
		}
		var ev Event
		if err := json.Unmarshal(msg.Value, &ev); err != nil {
			log.Printf("[consumer] bad event: %v", err)
			continue
		}
		log.Printf("[consumer] event=%s key=%s", ev.Event, string(msg.Key))
		if ev.Event == "batch.distributed" {
			var data struct {
				BatchID int64 `json:"batch_id"`
			}
			if err := json.Unmarshal(ev.Data, &data); err == nil && data.BatchID > 0 {
				if err := svc.ProjectPublicity(ctx, data.BatchID); err != nil {
					log.Printf("[consumer] project publicity batch=%d: %v", data.BatchID, err)
				} else {
					log.Printf("[consumer] publicity projected batch=%d", data.BatchID)
				}
			}
		}
	}
}
