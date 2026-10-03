package limiter

import (
	"fmt"
	"sync/atomic"
	"testing"
)

// The benchmarks use the real clock, so every operation includes a time.Now
// call; that is part of the cost a caller pays. Buckets are configured with a
// huge capacity and rate so operations are admitted and the admit path, not
// the reject path, is measured, except where a benchmark says otherwise.
//
// What they measure:
//   - Allow:                 uncontended cost of one decision.
//   - AllowParallel:         all goroutines share ONE bucket, so they serialize
//                            on its mutex (worst case contention).
//   - AllowParallel_Rejecting: the same, on the reject path.
//   - PerUser_Sequential:    lookup plus decision for many keys, no contention.
//   - PerUser_Parallel:      goroutines spread over many keys; contention is
//                            limited to the table's RWMutex reader count.
//   - PerUser_HotKey:        all goroutines hit one key: contention on one
//                            bucket plus the table lock.
//   - PerUser_WithGlobal:    adds the shared global limiter, which reintroduces
//                            a single contended mutex.
//
// Results depend on hardware and GOMAXPROCS; compare runs on the same machine.
// Run with: go test -run '^$' -bench . -benchmem ./limiter

func benchLimiter(b *testing.B) *Limiter {
	b.Helper()
	return mustLimiter(b, Config{Rate: 1e12, Capacity: 1e12})
}

func BenchmarkLimiter_Allow(b *testing.B) {
	l := benchLimiter(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.Allow()
	}
}

func BenchmarkLimiter_AllowParallel(b *testing.B) {
	l := benchLimiter(b)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			l.Allow()
		}
	})
}

func BenchmarkLimiter_AllowParallel_Rejecting(b *testing.B) {
	l := mustLimiter(b, Config{Rate: 1e-9, Capacity: 1})
	l.Allow() // empty, and effectively never refills
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			l.Allow()
		}
	})
}

const benchKeys = 4096

func benchKeyNames() []string {
	keys := make([]string, benchKeys)
	for i := range keys {
		keys[i] = fmt.Sprintf("user-%d", i)
	}
	return keys
}

func benchPerUser(b *testing.B, global *Limiter) *PerUser {
	b.Helper()
	return mustPerUser(b, PerUserConfig{Limit: Config{Rate: 1e12, Capacity: 1e12}, Global: global})
}

func BenchmarkPerUser_Sequential(b *testing.B) {
	p, keys := benchPerUser(b, nil), benchKeyNames()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Allow(keys[i%benchKeys])
	}
}

func BenchmarkPerUser_AllowParallel(b *testing.B) {
	p, keys := benchPerUser(b, nil), benchKeyNames()
	var next atomic.Uint64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := next.Add(1) * 7919 // spread goroutines across the key space
		for pb.Next() {
			i++
			p.Allow(keys[i%benchKeys])
		}
	})
}

func BenchmarkPerUser_AllowParallel_HotKey(b *testing.B) {
	p := benchPerUser(b, nil)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			p.Allow("hot")
		}
	})
}

func BenchmarkPerUser_AllowParallel_WithGlobal(b *testing.B) {
	p, keys := benchPerUser(b, benchLimiter(b)), benchKeyNames()
	var next atomic.Uint64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := next.Add(1) * 7919
		for pb.Next() {
			i++
			p.Allow(keys[i%benchKeys])
		}
	})
}
