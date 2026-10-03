// Command basic shows the smallest useful use of a limiter: create a bucket,
// spend it, and watch it refill.
package main

import (
	"fmt"
	"log"
	"time"

	"github.com/example/token-bucket-rate-limiter/limiter"
)

func main() {
	// 2 tokens per second, at most 3 stored: a burst of 3, then 2 per second.
	l, err := limiter.NewLimiter(limiter.Config{Rate: 2, Capacity: 3})
	if err != nil {
		log.Fatal(err) // invalid configuration is reported here, not mid-traffic
	}

	for i := 1; i <= 5; i++ {
		fmt.Printf("request %d: allowed=%v\n", i, l.Allow())
	}

	fmt.Println("sleeping 1s: the bucket earns 2 tokens")
	time.Sleep(time.Second)
	for i := 6; i <= 8; i++ {
		fmt.Printf("request %d: allowed=%v\n", i, l.Allow())
	}

	// A request can cost more than one token, e.g. a bulk operation.
	time.Sleep(2 * time.Second) // plenty of time to refill to capacity
	fmt.Printf("AllowN(3) on a refilled bucket: %v\n", l.AllowN(3))
	fmt.Printf("AllowN(1) right after:          %v\n", l.AllowN(1))
}
