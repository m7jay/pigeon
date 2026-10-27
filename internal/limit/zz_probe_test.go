package limit

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestProbeAcquireLoop(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	r, _ := NewRedisFrom(rdb)
	ctx := context.Background()
	for i := 0; i < 15; i++ {
		lease, ok, wait, err := r.Acquire(ctx, "ep", 10, 30*time.Second, "h")
		fmt.Printf("attempt %2d ok=%v wait=%s err=%v lease=%v\n", i+1, ok, wait, err, lease != nil)
	}
	n, err := rdb.ZCard(ctx, ConcurrencyKey("ep")).Result()
	fmt.Printf("zcard=%d err=%v\n", n, err)
	ok, wait, err := r.Allow(ctx, "ep", 25, 25)
	fmt.Printf("allow ok=%v wait=%s err=%v\n", ok, wait, err)
}
