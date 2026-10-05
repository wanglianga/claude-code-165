package main

import (
	"context"
	_ "embed"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"

	"donation-service/internal/config"
	"donation-service/internal/events"
	"donation-service/internal/httpapi"
	"donation-service/internal/jobs"
	"donation-service/internal/seed"
	"donation-service/internal/service"
	"donation-service/internal/store"
)

//go:embed openapi.yaml
var openapiYAML []byte

func main() {
	cfg := config.Load()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := store.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("postgres: %v", err)
	}
	defer pool.Close()
	if err := store.Migrate(ctx, pool, cfg.MigrationsDir); err != nil {
		log.Fatalf("migrate: %v", err)
	}
	if cfg.SeedDemo {
		if err := seed.Run(ctx, pool); err != nil {
			log.Fatalf("seed: %v", err)
		}
	}

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr, Password: cfg.RedisPassword})
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("redis: %v", err)
	}
	defer rdb.Close()

	svc := service.New(pool, rdb, cfg)

	// Kafka：outbox relay + consumer；未配置 brokers 时降级为仅库内事件（本地裸跑可用）
	if len(cfg.KafkaBrokers) > 0 {
		writer := &kafka.Writer{
			Addr:         kafka.TCP(cfg.KafkaBrokers...),
			Balancer:     &kafka.LeastBytes{},
			BatchTimeout: 200 * time.Millisecond,
			RequiredAcks: kafka.RequireOne,
		}
		defer writer.Close()
		go events.StartOutboxRelay(ctx, pool, writer)
		go events.StartConsumer(ctx, svc, cfg.KafkaBrokers, cfg.KafkaTopic)
		log.Printf("[kafka] enabled brokers=%v topic=%s", cfg.KafkaBrokers, cfg.KafkaTopic)
	} else {
		log.Printf("[kafka] KAFKA_BROKERS 未配置，事件仅落库 outbox（跳过投递）")
	}

	go jobs.StartExpiryJob(ctx, svc, cfg.ExpiryJobInterval)

	router := httpapi.NewRouter(svc, openapiYAML)
	srv := &http.Server{Addr: cfg.HTTPAddr, Handler: router}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	log.Printf("[http] listening on %s", cfg.HTTPAddr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("http: %v", err)
	}
}
