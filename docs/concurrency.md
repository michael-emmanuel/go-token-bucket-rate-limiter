# Concurrency

A rate limiter sits on the hot path of a server where every request runs in its own goroutine. Hundreds of goroutines may call `Allow()` on the same bucket at the same moment. This document explains what can go wrong, how `Limiter` prevents it, and what it costs.

## 1. The shared state

A `Limiter` has two mutable fields:

```go
tokens float64   // how many tokens are available
last   time.Time // when tokens was last brought up to date
```

Every call reads both and writes both. They are not independent: `tokens` is only meaningful relative to `last`. That coupling is the source of every bug below.

## 2. The critical section

```go
func (l *Limiter) take(n int) Decision {
    ...
    l.mu.Lock()
    defer l.mu.Unlock()
    l.refillLocked(l.clock.Now()) // 1. read the clock, credit earned tokens, set last
    if l.tokens >= need {         // 2. check
        l.tokens -= need          // 3. consume
        return Decision{Allowed: true}
    }
    return Decision{RetryAfter: l.retryAfterLocked(need)}
}
```

Refill, check, consume and the timestamp update form one logical operation. "Logical operation" means other goroutines must never observe, or interleave with, a half-finished state such as "refilled but not yet consumed", or "consumed but `last` not yet advanced". A mutex gives exactly that: while one goroutine is inside, every other goroutine waits, so the operation is atomic with respect to the others.

The critical section contains only arithmetic and one `time.Now()` call. It performs no I/O, no allocation, no channel operations and no calls into user code other than `Clock.Now` (documented as required to be fast and non-blocking). Observers are notified *after* the lock is released, so a slow metrics backend cannot lengthen the critical section.

## 3. What goes wrong without it

Each of the following is a plausible first attempt. Each is wrong, and each is wrong in a way the test suite is designed to catch.

### 3.1 Check-then-act (double spend)

```go
// WRONG: individually atomic operations, but the sequence is not atomic.
func (l *Bad) Allow() bool {
    if atomic.LoadInt64(&l.tokens) > 0 {   // check
        atomic.AddInt64(&l.tokens, -1)     // act
        return true
    }
    return false
}
```

The race detector will not flag this code: every access is atomic, so there is no data race. The bug is a *logic* race:

```
tokens = 1
G1: Load -> 1  (> 0, proceed)
G2: Load -> 1  (> 0, proceed)
G1: Add -1 -> 0, returns true
G2: Add -1 -> -1, returns true   <- two requests spent one token; tokens is now negative
```

This is double spending, and it violates `0 <= tokens`. A compare-and-swap loop fixes it for an integer counter, but the limiter needs two fields to change together, which a single CAS word cannot express without packing them (see section 6).

`TestConcurrent_AllowFrozenClockAdmitsExactlyCapacity` would fail this implementation: with a frozen clock there is no refill, so exactly `capacity` requests must succeed, and this code would admit more.

### 3.2 Refill and consume under separate locks

```go
// WRONG: two critical sections where one is needed.
func (l *Bad) Allow() bool {
    l.mu.Lock(); l.refill(); l.mu.Unlock()
    l.mu.Lock(); ok := l.tokens >= 1; if ok { l.tokens-- }; l.mu.Unlock()
    return ok
}
```

Each step is protected, yet the pair is not atomic. Between them another goroutine can consume the tokens this goroutine's refill just produced; this goroutine then sees "empty" and rejects although the bucket, taken as a whole, had budget for it at the moment it was called. That is a spurious rejection. Combined with variations (refill in one lock, timestamp update in another) it also produces lost refill credit or double credit.

### 3.3 Reading the clock before taking the lock

```go
// WRONG: timestamp obtained outside the critical section.
func (l *Bad) Allow() bool {
    now := time.Now()
    l.mu.Lock()
    defer l.mu.Unlock()
    elapsed := now.Sub(l.last)
    ...
    l.last = now
}
```

```
last = 9.990
G1: now = 10.000                      (reads the clock, then is descheduled)
G2: now = 10.001
G2: Lock; credits 11ms; last = 10.001; Unlock
G1: Lock; elapsed = 10.000 - 10.001 = -1ms (negative); last = 10.000 (moved backwards); Unlock
```

Two failures follow. A negative `elapsed` either mints negative tokens or needs special-casing. Worse, moving `last` backwards means the next refill credits the same millisecond a second time, over-admitting. Reading the clock inside the lock removes the whole category: timestamps applied to the bucket are monotonically non-decreasing because they are taken one at a time.

### 3.4 Background refill goroutine

```go
// WRONG for this project: per-bucket goroutine.
go func() { for range time.Tick(time.Second) { l.mu.Lock(); l.tokens = min(l.capacity, l.tokens+l.rate); l.mu.Unlock() } }()
```

It is safe against data races but fails on design grounds: it leaks a goroutine and a ticker per bucket unless shut down explicitly, refills in coarse steps, drifts when the scheduler is busy, and cannot be used with millions of buckets. See [token-bucket.md](token-bucket.md) section 2.

## 4. Global plus per-user: two locks, one decision

`PerUser` with a `Global` limiter must charge a request to two buckets atomically (the design is explained in [architecture.md](architecture.md)). `takeTwo` holds both mutexes while it checks both buckets and only then consumes from both:

```go
a.mu.Lock(); defer a.mu.Unlock()   // global
b.mu.Lock(); defer b.mu.Unlock()   // user
refill both
if a.tokens >= need && b.tokens >= need { consume both }
```

**Deadlock avoidance by lock ordering.** Deadlock needs a cycle: G1 holds X and wants Y while G2 holds Y and wants X. The code rules out cycles with one global order:

1. the `PerUser` table lock (read or write), then
2. the global limiter's mutex, then
3. a user limiter's mutex.

No code path acquires them in another order, and no path holds two user mutexes at once (the sweep locks user buckets one at a time, while holding the table write lock and no global mutex). A total order implies no cycle.

**No token leakage.** Because the availability check for both buckets happens before either is changed, a request rejected by one bucket consumes nothing from the other. The alternatives, and why they were rejected, are in [architecture.md](architecture.md) (Decision 5).

## 5. The per-user table

`PerUser` adds a second shared structure: the `map[string]*Limiter`. Go maps are not safe for concurrent writes, nor for a write concurrent with a read, so the map has its own lock.

```
request for existing key:   RLock table -> lookup -> charge bucket (bucket mutex) -> RUnlock
request for new key:        RLock -> miss -> RUnlock -> Lock -> re-check -> insert -> charge -> Unlock
sweep:                      Lock table -> for each bucket: lock it, check if full, delete -> Unlock
```

Three details are deliberate:

- **Re-check after upgrading.** `sync.RWMutex` has no upgrade. Between `RUnlock` and `Lock` another goroutine may insert the same key; without the re-check two buckets would be created for one key and one of them, with its spent tokens, would be lost.
- **The read lock is held while the bucket is charged.** Otherwise a sweep could delete a bucket between lookup and use; the request would then spend a token from a bucket no longer in the table and the next request would get a fresh full one. Holding the read lock makes sweep and request mutually exclusive for that step. The hold is a few arithmetic operations long.
- **Different keys do not serialize.** Readers share the table's read lock; each bucket has its own mutex. The remaining shared write is the `RWMutex` reader counter itself, a single contended cache line.

## 6. Contention

Under high concurrency a single bucket is a serialization point: N goroutines calling `Allow()` on one `Limiter` take turns through its mutex. Expected behavior:

- **One hot bucket** (for example the global limiter, or one user key hammered by many connections): throughput on that bucket is bounded by how fast one core can run the critical section, and goroutines queue behind the mutex. The critical section is small, but under heavy parallelism the mutex's cache line bounces between cores.
- **Many buckets** (per-user with many distinct users): contention spreads out; the table's read lock is then the main shared point.
- **Global plus per-user**: every request still passes through the global mutex, so the global limiter caps the parallelism of the whole layer no matter how many users there are.

Do not guess the numbers. `bench_test.go` has benchmarks for each case (single goroutine, all goroutines on one bucket, many keys, one hot key, with global). Run them on the target hardware; see [testing.md](testing.md).

### Ways to reduce contention, and what they cost

| Technique | How | Trade-off |
|---|---|---|
| Atomics / lock-free | Pack state into one word (for example GCRA's theoretical arrival time) and update with compare-and-swap | No lock, but a failing CAS retries under contention; needs an integer representation; harder to extend (two-bucket atomic takes become difficult); harder to reason about |
| Sharded limiters | Split one global bucket into K buckets each with `rate/K`, pick by goroutine or hash | Less contention, but the effective limit is now approximate: a request can be rejected by an empty shard while others have tokens, and burst behavior changes |
| Sharded table | Split the key map into K shards, each with its own lock | Smaller lock scope, shorter sweep pauses; slightly more code |
| Batching / leases | Each worker takes a block of tokens from the shared bucket and spends it locally | Far fewer shared operations; unspent leased tokens are stranded, and accuracy drops |
| Move to the edge | Enforce at a gateway, where traffic is already aggregated | Operational change, not a code change |

The baseline mutex is the right starting point: it is obviously correct, the critical section is tiny, and the invariants are easy to state. Reach for the alternatives when a benchmark or a production profile points at this mutex.

## 7. Fairness and `Wait`

`Wait` does not queue. A waiter sleeps for the computed shortfall and retries; a concurrent `Allow` can take the tokens first, and the waiter recomputes and sleeps again. This keeps the `0 <= tokens` invariant and needs no refund logic, but waiters are served in no particular order, and a waiter can be starved under sustained overload. Bound waits with a context deadline. A FIFO design would need a queue of waiters or a reservation scheme in which tokens are promised in advance; that is a deliberate extension, not a small tweak.

## 8. The race detector and what it does not do

`go test -race ./...` instruments memory accesses and reports *data races*: two goroutines access the same memory, at least one writes, with no synchronization between them. It would catch a limiter that reads `l.tokens` without the lock.

It does **not** catch logic races such as 3.1, where every access is atomic but the algorithm is wrong. That is why this repository pairs `-race` with exact-count tests: a frozen clock makes the number of admitted requests a known constant (`TestConcurrent_AllowFrozenClockAdmitsExactlyCapacity`), and conservation checks (`granted + remaining == capacity`) catch lost or duplicated tokens (`TestConcurrent_AllowNConservesTokens`, `TestPerUser_ConcurrentGlobalAndUserAccounting`). Both kinds of test are required to claim thread safety.
