// Command burst contrasts burst capacity with sustained rate.
//
// The bucket holds 10 tokens and refills at 5 per second. A flood of 30
// simultaneous requests gets the 10 stored tokens; after that, throughput is
// limited to the refill rate.
package main

import (
	"fmt"
	"log"
	"time"

	"github.com/example/token-bucket-rate-limiter/limiter"
)

func main() {
	l, err := limiter.NewLimiter(limiter.Config{Rate: 5, Capacity: 10})
	if err != nil {
		log.Fatal(err)
	}

	admitted := 0
	for i := 0; i < 30; i++ {
		if l.Allow() {
			admitted++
		}
	}
	fmt.Printf("burst: %d of 30 simultaneous requests admitted (capacity is 10)\n", admitted)

	// Keep sending 100 requests/second for 2 seconds; only the refill rate
	// (5/s) gets through, so about 10 of the 200 attempts succeed.
	admitted = 0
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.After(2 * time.Second)
	attempts := 0
loop:
	for {
		select {
		case <-ticker.C:
			attempts++
			if l.Allow() {
				admitted++
			}
		case <-deadline:
			break loop
		}
	}
	fmt.Printf("sustained: %d of %d attempts admitted in 2s (refill rate 5/s, expect about 10)\n", admitted, attempts)
}
