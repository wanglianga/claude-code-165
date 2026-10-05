package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"bakery-donation/internal/api"
	"bakery-donation/internal/config"
	"bakery-donation/internal/database"
	"bakery-donation/internal/events"
	"bakery-donation/internal/projector"
	"bakery-donation/internal/service"

	"github.com/redis/go-redis/v9"
)

func connectWithRetry(ctx context.Context, url string, attempts int) (*database.DB, error) {
	var lastErr error
	for i := 0; i < attempts; i++ {
		db, err := database.New(ctx, url)
		if err == nil {
			return db, nil
		}
		lastErr = err
		log.Printf("waiting for postgres (%d/%d): %v", i+1, attempts, err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return nil, lastErr
}

func main() {
	cfg := config.Load()
	log.Printf("starting bakery-donation service on %s (kafka=%v)", cfg.HTTPAddr, cfg.KafkaEnabled)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	db, err := connectWithRetry(ctx, cfg.DatabaseURL, 30)
	if err != nil {
		log.Fatalf("connect postgres: %v", err)
	}
	if err := db.Migrate(ctx); err != nil {
		log.Fatalf("migrate: %v", err)
	}
	if err := database.SeedUsers(ctx, db); err != nil {
		log.Fatalf("seed users: %v", err)
	}

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr, DB: cfg.RedisDB})
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Printf("warning: redis unavailable, running without locks/cache: %v", err)
		rdb = nil
	}

	proj := projector.New(db, rdb, cfg.KafkaBrokers, cfg.KafkaTopic, cfg.KafkaEnabled)
	go proj.Run(ctx)

	relay := events.NewRelay(cfg.KafkaBrokers, cfg.KafkaTopic, db.Pool, cfg.KafkaEnabled)
	go relay.Run(ctx, 200*time.Millisecond)

	svc := service.New(db, rdb, cfg, proj)
	go svc.RunSweeper(ctx)

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           api.New(cfg, db, rdb, svc, proj).Router(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http: %v", err)
		}
	}()
	log.Printf("ready: http://localhost%s  swagger: http://localhost%s/docs", cfg.HTTPAddr, cfg.HTTPAddr)

	<-ctx.Done()
	log.Println("shutting down...")
	shutdownCtx, c := context.WithTimeout(context.Background(), 10*time.Second)
	defer c()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("shutdown: %v", err)
	}
	if rdb != nil {
		_ = rdb.Close()
	}
	db.Close()
	os.Exit(0)
}
