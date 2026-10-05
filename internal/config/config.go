package config

import (
	"os"
	"strings"
	"time"
)

type Config struct {
	HTTPAddr      string
	DatabaseURL   string
	RedisAddr     string
	RedisDB       int
	KafkaBrokers  []string
	KafkaEnabled  bool
	KafkaTopic    string
	TokenSecret   string
	SweepInterval time.Duration
	// 食安规则参数（可通过环境变量覆盖）
	ColdSafetyMargin    time.Duration // 冷藏食品距离保质期的最小缓冲
	AmbientSafetyMargin time.Duration
	LateToleranceMin    int // 冷链车迟到容忍分钟
	NoShowGraceMin      int // 超过计划取货时间多久未到判定爽约
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func Load() Config {
	c := Config{
		HTTPAddr:            getenv("HTTP_ADDR", ":8080"),
		DatabaseURL:         getenv("DATABASE_URL", "postgres://app@127.0.0.1:5433/bakery?sslmode=disable"),
		RedisAddr:           getenv("REDIS_ADDR", "127.0.0.1:6379"),
		RedisDB:             0,
		KafkaEnabled:        getenv("KAFKA_ENABLED", "true") != "false",
		KafkaTopic:          getenv("KAFKA_TOPIC", "donation-batch-events"),
		TokenSecret:         getenv("TOKEN_SECRET", "dev-secret-change-me"),
		SweepInterval:       30 * time.Second,
		ColdSafetyMargin:    4 * time.Hour,
		AmbientSafetyMargin: 2 * time.Hour,
		LateToleranceMin:    15,
		NoShowGraceMin:      20,
	}
	if v := os.Getenv("REDIS_DB"); v != "" {
		n := 0
		for _, ch := range v {
			n = n*10 + int(ch-'0')
		}
		c.RedisDB = n
	}
	if brokers := getenv("KAFKA_BROKERS", "127.0.0.1:9092"); brokers != "" {
		for _, b := range strings.Split(brokers, ",") {
			if b = strings.TrimSpace(b); b != "" {
				c.KafkaBrokers = append(c.KafkaBrokers, b)
			}
		}
	}
	return c
}
