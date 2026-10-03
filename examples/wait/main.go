// Command wait shows context-aware waiting: blocking for a token, giving up at
// a deadline, and cancelling from outside.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/example/token-bucket-rate-limiter/limiter"
)

func main() {
	// One token per 200ms, no burst.
	l, err := limiter.NewLimiter(limiter.Config{Rate: 5, Capacity: 1})
	if err != nil {
		log.Fatal(err)
	}

	// 1. Wait paces callers to the rate instead of rejecting them.
	start := time.Now()
	for i := 1; i <= 4; i++ {
		if err := l.Wait(context.Background()); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("call %d proceeded at +%v\n", i, time.Since(start).Round(50*time.Millisecond))
	}

	// 2. A deadline shorter than the required wait fails immediately instead
	// of sleeping until the deadline.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	t0 := time.Now()
	err = l.Wait(ctx)
	fmt.Printf("short deadline: err=%v (deadline exceeded: %v) after %v\n",
		err != nil, errors.Is(err, context.DeadlineExceeded), time.Since(t0).Round(time.Millisecond))

	// 3. Cancellation from another goroutine wakes a blocked Wait.
	ctx2, cancel2 := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel2()
	}()
	t0 = time.Now()
	err = l.Wait(ctx2)
	fmt.Printf("cancelled:      canceled=%v after %v\n", errors.Is(err, context.Canceled), time.Since(t0).Round(10*time.Millisecond))
}
