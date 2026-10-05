package lock

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

var ErrLocked = errors.New("lock already held")

// Locker 基于 Redis SET NX PX 的短期互斥锁。
// 用于预约热点串行化；数据库唯一索引是最终硬保证。
type Locker struct {
	rdb *redis.Client
}

func New(rdb *redis.Client) *Locker { return &Locker{rdb: rdb} }

type Release func()

// Acquire 获取 key 上的锁，持锁 ttl，返回释放函数。Redis 不可用时退化为无锁（靠 DB 约束兜底）。
func (l *Locker) Acquire(ctx context.Context, key string, ttl time.Duration) (Release, error) {
	if l.rdb == nil {
		return func() {}, nil
	}
	token := uuid.NewString()
	ok, err := l.rdb.SetNX(ctx, key, token, ttl).Result()
	if err != nil {
		// Redis 故障不阻断业务：降级为不加锁
		return func() {}, nil
	}
	if !ok {
		return nil, ErrLocked
	}
	return func() {
		// 仅允许持有者释放
		script := `if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("del", KEYS[1]) else return 0 end`
		_ = l.rdb.Eval(context.Background(), script, []string{key}, token).Err()
	}, nil
}
