package jobs

import (
	"context"
	"log"
	"time"

	"donation-service/internal/service"
)

// StartExpiryJob 周期巡检：过期批次与超时预约的状态收敛。
func StartExpiryJob(ctx context.Context, svc *service.Service, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := svc.RunExpiryOnce(ctx)
			if err != nil {
				log.Printf("[expiry-job] %v", err)
			} else if n > 0 {
				log.Printf("[expiry-job] processed %d item(s)", n)
			}
		}
	}
}
