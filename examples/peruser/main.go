// Command peruser shows per-user isolation, a global limit shared by all users,
// and cleanup of idle per-user state.
package main

import (
	"fmt"
	"log"

	"github.com/example/token-bucket-rate-limiter/limiter"
)

func main() {
	// Whole service: 6 requests in a burst. Each user: 4.
	global, err := limiter.NewLimiter(limiter.Config{Rate: 1, Capacity: 6})
	if err != nil {
		log.Fatal(err)
	}
	users, err := limiter.NewPerUser(limiter.PerUserConfig{
		Limit:  limiter.Config{Rate: 1, Capacity: 4},
		Global: global,
	})
	if err != nil {
		log.Fatal(err)
	}

	count := func(key string, attempts int) int {
		ok := 0
		for i := 0; i < attempts; i++ {
			if users.Allow(key) {
				ok++
			}
		}
		return ok
	}

	fmt.Printf("user-123 sends 10 requests: %d admitted (per-user capacity 4)\n", count("user-123", 10))
	fmt.Printf("user-456 sends 10 requests: %d admitted (own bucket is untouched by user-123,\n", count("user-456", 10))
	fmt.Println("                            but only 2 tokens remain in the global bucket)")
	fmt.Printf("user-789 sends 10 requests: %d admitted (global bucket is empty)\n", count("user-789", 10))

	// Requests refused by the global layer spent nothing from the user's own
	// bucket, so user-789's bucket is still full.
	fmt.Printf("tracked users: %d\n", users.Len())

	// A bucket that has refilled to capacity is indistinguishable from a new
	// one, so Cleanup can drop it without changing any future decision. Here
	// that is user-789's untouched bucket; the other two are still partly spent.
	fmt.Printf("removed by Cleanup now: %d (user-789, whose bucket is full)\n", users.Cleanup())
}
