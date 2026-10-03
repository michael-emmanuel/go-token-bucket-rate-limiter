# Token Bucket Rate Limiter: Engineering Review

**Problem.** A service needs to protect itself, and its dependencies, from clients that send too much, without punishing normal bursty behavior.

**Algorithm.** A token bucket has two parameters: capacity, the largest burst, and rate, the sustained throughput. Tokens refill continuously up to capacity; each request takes one or more. Capacity 100 and rate 10 means a client can send 100 requests at once, then 10 per second.

**Data structures.** A `Limiter` is a mutex, a `float64` token count, and the time of the last refill. There is no timer. Each call computes `elapsed x rate`, adds it, clamps to capacity, then checks and subtracts. Idle buckets cost nothing.

**Concurrency.** Refill, check, consume and the timestamp update are one critical section under a mutex. The clock is read inside it so timestamps can't be applied out of order. I tested it with a frozen clock so the number of admitted requests is exactly the capacity across many goroutines, plus the race detector.

**Burst.** Capacity is burst, rate is sustained. That separates it from a counter that resets every second.

**Per-user limits.** A `PerUser` type keeps a map from key to bucket behind an `RWMutex`, so different users mostly don't contend. With a global limiter configured, a request must pass both, and I charge both atomically under both locks, so a request rejected by one layer doesn't drain the other.

**HTTP.** Middleware extracts an identity, asks the limiter, and returns 429 with a rounded-up Retry-After. Identity strategies are separate functions because IP-based limiting has real pitfalls.

**Production limits.** It's in-process: N instances give up to N times the limit, restarts reset buckets, and memory needs management. I handle memory with lossless cleanup and an optional user cap, and I documented Redis, gateways and fail-open versus fail-closed rather than building them.

## 3. Five-Minute Deep Dive

Follow the life of one request through `limiter/middleware.go` and `limiter/per_user.go`.

```
request -> identity -> bucket lookup -> refill -> check -> consume -> allow / reject
```

1. **Request arrives** at `Middleware`. It is one goroutine among many.
2. **Identity.** A `KeyFunc` turns the request into a key: authenticated user, API key, or peer IP. No identity gives 400 and creates no state. Keys from request data are capped at 256 bytes so a client can't spend server memory at header size.
3. **Validation.** `PerUser.take` rejects `n < 1` or `n > capacity` before touching the table, so impossible requests can't create entries.
4. **Bucket lookup.** Take the table read lock; look up the key. On a miss, release, take the write lock, _re-check_ (another goroutine may have inserted), then create. If `MaxUsers` is reached, try a rate-limited sweep, else reject the new key.
5. **Charge.** Still holding the read lock, so a sweep can't delete the bucket under us, call `takeTwo` (with a global limiter) or `take`.
6. **Critical section** in `takeTwo`: lock global, then user. For each bucket: read the clock, compute `elapsed = now - last`, `tokens = min(capacity, tokens + elapsed x rate)`, `last = now`. Then the decision: both buckets have at least `n` tokens, or not. If yes, subtract from both. If no, change nothing except the refill bookkeeping, and compute `RetryAfter` as the larger of the two deficits divided by their rates.
7. **Unlock, then observe.** Locks are released before the `Observer` is called.
8. **Respond.** Allowed requests proceed to the handler. Denied ones get `429` and `Retry-After` in whole seconds, rounded up.

**The critical section, in one sentence:** everything between locking the bucket and unlocking it is refill plus check plus consume plus timestamp update, and it has to be one atomic step because those four pieces of state depend on each other. Splitting them allows double spending, lost refills, or timestamps applied out of order ([concurrency.md](concurrency.md) section 3 shows each bug).

## 4. Whiteboard Explaination

```
        rate = 5 tokens/s   (tap, always dripping)
              |
              v
        +-----------+
        |  tokens   |  capacity = 10  (the bucket's size; overflow is lost)
        |   T = 10  |
        +-----+-----+
              |
              v   each request takes n tokens; empty means "rejected"

   last = time of the previous refill;  elapsed = now - last
   refill:   T = min(capacity, T + elapsed x rate)
```

Inputs used in `TestDocumentedWalkthrough` function along with results:

| Time   | Event                     | T after refill    | Result                   | T after |
| ------ | ------------------------- | ----------------- | ------------------------ | ------- |
| 0.0 s  | 10 requests at once       | 10                | all allowed              | 0       |
| 0.0 s  | one more                  | 0                 | rejected                 | 0       |
| 0.2 s  | 1 request                 | 0 + 0.2 x 5 = 1.0 | allowed                  | 0       |
| 0.3 s  | 1 request                 | 0.5               | rejected (fraction kept) | 0.5     |
| 0.4 s  | 1 request                 | 1.0               | allowed                  | 0       |
| 11.4 s | after a long quiet period | min(10, 52) = 10  | allowed                  | 9       |

The burst is the bucket size; the sustained rate is the tap; and idle time can never make the bucket larger than its capacity, which is the `0 <= T <= capacity` invariant.

## 5. Why Token Bucket?

| Algorithm                  | Why someone picks it                                                                             | Why someone doesn't                                                                               |
| -------------------------- | ------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------- |
| **Token bucket**           | Two independent knobs (burst and rate), O(1) state, constant-time operation, no boundary effects | Permits bursts, which may be unwanted; per-request cost equal unless you use `AllowN`             |
| **Fixed window**           | Simplest; cheap; natural for quotas ("10,000 per day")                                           | Boundary problem: limit 100/min allows 100 at 12:00:59 and 100 at 12:01:00, so 200 in two seconds |
| **Sliding window log**     | Exact; no boundary effect                                                                        | O(limit) memory per key, pruning cost; impractical for big limits                                 |
| **Sliding window counter** | O(1) and smoother than fixed                                                                     | An approximation; no separate burst parameter                                                     |
| **Leaky bucket (queue)**   | Perfectly smooth output rate to a fragile downstream                                             | Adds latency, needs a queue, no bursts at all                                                     |

There is no universally best choice. Choose by what the limit protects: a burst-tolerant API wants a token bucket; a fixed-rate link wants a leaky-bucket queue; a daily quota wants a fixed window; an audit-exact constraint on a small limit may justify a sliding window log.

## 6. Why a Mutex?

- **Shared state:** `tokens` and `last` are read and written together on every call.
- **Atomicity:** the operation is read-modify-write across two fields; the decision must see a consistent snapshot and its update must be indivisible.
- **Race conditions avoided:** double spend, lost refill, negative tokens, out-of-order timestamps.
- **Correctness first:** the critical section is a handful of arithmetic operations, so the lock is held briefly; and with a mutex the two-bucket atomic take stays straightforward.

**Alternatives and why not (yet):**

- _Atomics / lock-free._ Possible by packing state into one 64-bit word, for example GCRA's theoretical arrival time, updated with compare-and-swap. It removes the lock but forces an integer representation, makes the two-bucket atomic take hard (you cannot CAS two words atomically), and under contention failed CAS attempts retry. A reasonable optimization _after_ a profile shows the mutex matters.
- _Sharded locks._ Split a hot bucket into K buckets with `rate/K` each. Less contention; the limit becomes approximate (one shard can reject while others have tokens).
- _Channels._ A goroutine owning the state and receiving requests serializes access too, but costs a channel operation and a context switch per call; slower and more complicated than a mutex for a few instructions of work.

I chose the mutex because the invariants are easy to state and verify, and because it's the honest baseline against which any optimization must be measured.

## 7. What Happens Under High Concurrency?

- **Many goroutines, one bucket.** They take turns through the bucket's mutex. Correctness is unaffected; throughput on that bucket is bounded by how fast one core runs the critical section, and cache-line bouncing on the mutex adds cost as parallelism rises.
- **Serialized access to one bucket** is inherent to any exact limiter: a shared budget needs a single point of agreement. The critical section is kept minimal (no allocation, no I/O; observers run after unlock).
- **Per-user buckets.** Users are independent, so contention spreads across as many mutexes as there are active users. The shared pieces are the table's `RWMutex` (readers share it; there is still one reader counter all cores touch) and, if configured, the global limiter, which every request passes through. That global mutex caps the parallelism of the whole layer.
- **Map contention.** Reads are concurrent. Inserts and sweeps take the write lock and briefly stop readers. The sweep cost grows with the number of keys, so very large tables need sharding.
- **Scaling strategies.** In order of effort: measure; shard the table by key hash; shard the global bucket; batch tokens per worker (leases); move enforcement to the edge. Each one trades accuracy or complexity for scalability.

## 8. What Are the Bottlenecks?

| Bottleneck                                               | Symptom                                             | How to measure                                                                                                                                 |
| -------------------------------------------------------- | --------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| Mutex contention on one bucket (hot key, global limiter) | throughput stops scaling with cores                 | `BenchmarkLimiter_AllowParallel` across `-cpu` values; Go mutex profile (`runtime.SetMutexProfileFraction`, `pprof`) in a real service         |
| Per-user table                                           | lock contention on the reader count; sweep pauses   | `BenchmarkPerUser_AllowParallel` vs `_HotKey`; time `Cleanup()` against a table of the expected size; p99 latency spikes at the sweep interval |
| Memory growth                                            | heap grows with distinct keys                       | `Len()` gauge; heap profile; `runtime.MemStats` before and after populating N keys                                                             |
| High-cardinality identities                              | many keys, few requests each; table near `MaxUsers` | `Len()` versus `MaxUsers`; rejection counts for new keys                                                                                       |
| Distributed limiter latency (if you add one)             | added round trip on every request                   | request latency histograms with and without the limiter; store p99; timeout rate                                                               |
| Clock reads                                              | `time.Now` inside the critical section              | compare `BenchmarkLimiter_Allow` with a cheaper fake clock to isolate the cost                                                                 |

## 9. How Would You Scale It?

**Single process.** The in-memory limiter as built. Correct, fast, no dependencies. The right answer for one instance or for protecting one process's own resources.

**Multiple instances.** Local limiters multiply the limit by up to N. Options: configure `limit/N` per instance (cheap, wrong with uneven traffic or changing N); or a shared distributed limiter.

**Large scale.** Combine layers:

- _Edge:_ a gateway or service mesh filters obvious abuse before it reaches the service.
- _Shared store (Redis):_ authoritative per-user buckets using an atomic Lua script, sharded by key across a cluster. Hot keys are split into sub-buckets.
- _Local first:_ keep the in-process limiter in front so the store is protected and the service degrades gracefully.
- _Dedicated rate-limiting service:_ central policy and metrics at the cost of another hop.
- _Hybrid leases:_ instances take blocks of tokens from the store to cut round trips, accepting bounded overshoot.

See [distributed-rate-limiting.md](distributed-rate-limiting.md).

## 10. What Happens If Redis Goes Down?

The correct behavior depends on _what the limit protects_ and the _threat model_. Lay out the options and say which you'd choose and why:

- **Fail open:** allow everything. Protects availability of the product; leaves the protected resource exposed, and an attacker who can degrade Redis can lift the limit. Reasonable for fairness limits and public read APIs.
- **Fail closed:** reject everything. Protects the downstream and security-sensitive paths (login); makes Redis a single point of failure for the whole service.
- **Local fallback (degraded mode):** switch to the in-process limiter, maybe at `limit/N`. Accuracy falls back to the local case but neither extreme is hit. My default for most services, for which the in-process limiter I've built is exactly the fallback.
- **Mechanics:** a short timeout on the Redis call so a slow store doesn't slow every request; a circuit breaker so you stop paying the timeout; metrics and alerts when in degraded mode; a decision per endpoint, made before the outage and tested.

## 11. How Do You Prevent Memory Leaks?

The per-user map is the only unbounded structure. Three layers:

1. **Lossless cleanup.** A bucket that has refilled to capacity is indistinguishable from a new one, so `Cleanup()` deletes exactly the full buckets and never changes a future decision. `TestPerUser_CleanupIsLossless` runs a swept and an unswept limiter on the same random workload and requires identical decisions.
2. **Caller-controlled lifecycle.** `PerUser` starts no goroutine. The owner calls `Cleanup()` or runs `go p.RunCleanup(ctx, interval)`, which stops when the context is cancelled. No hidden goroutine, nothing to leak in tests.
3. **Hard cap.** `MaxUsers` bounds the table. When full, it sweeps (at most once a second) and then rejects _new_ keys. I rejected LRU eviction because evicting an active user resets their budget, which an attacker rotating keys could exploit.

Alternatives: a TTL on last-seen (lossy unless the TTL is at least `capacity/rate`), LRU (O(1) but needs an exclusive lock on every hit), external stores with key expiry (Redis `PEXPIRE` set to the refill time gives the same lossless property).

## 12. How Would You Test This?

- **Unit tests** for each documented behavior: initially full, consume, empty rejects, refill, cap, burst, `AllowN`, invalid input.
- **Deterministic clock.** An injected `Clock` with `Advance()` makes time-dependent tests instant and exact. I kept the interface to one method.
- **Concurrent tests with exact answers.** A frozen clock removes refill, so the number of admitted requests across 64 goroutines must equal the capacity exactly. Conservation tests check `granted + remaining == capacity`.
- **Race detector.** `go test -race`, run in CI. It finds data races but not logic races, which is why the exact-count tests exist. Say this sentence; it shows you understand what the tool does.
- **Property and invariant tests.** A model-based test replays random operations against an exact rational-arithmetic model and requires agreement within a tiny epsilon; it also checks `0 <= tokens <= capacity` after every step.
- **Benchmarks** for single goroutine, parallel, hot key and with global.
- **Shuffling and low parallelism.** `-shuffle=on` and `GOMAXPROCS=1` to flush out order or scheduling assumptions.

## 13. Important Invariants

1. **`0 <= tokens <= capacity`.** Lower bound: consumption is guarded by `tokens >= need` inside the same critical section. Upper bound: every refill clamps with `min`.
2. **Supply bound.** Over any interval `Δ`, granted tokens `<= capacity + rate x Δ` for a bucket that started full. Tokens only enter through the initial fill and through refill.
3. **Conservation.** `granted + remaining == initial + refilled - discarded`, where `discarded` is refill lost to the capacity clamp. With a frozen clock nothing is refilled or discarded, so `granted + remaining == capacity` exactly, and concurrent access neither creates nor loses tokens (`TestConcurrent_AllowNConservesTokens`).
4. **Atomic all-or-nothing.** `AllowN` consumes `n` tokens or none; `takeTwo` consumes from both buckets or neither.
5. **Timestamp discipline.** `last` only moves forward, except for the explicit re-anchor when a custom clock goes backwards, and it is always written under the lock with a time read under the lock.
6. **Rejection is free.** A rejected request changes no token balance, in either layer.
7. **Eviction is invisible.** Deleting a full bucket never changes a later decision.
8. **Lock order.** table, then global, then user. No cycles, no deadlock.

## 14. Failure Modes

| Failure                   | What happens                                                                                                                         | Mitigation                                                                             |
| ------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------- |
| Invalid configuration     | `NewLimiter` returns an error wrapping `ErrInvalidConfig` (rate or capacity non-positive, capacity below 1, NaN, infinite)           | fail at startup, not at request time                                                   |
| Clock behavior            | monotonic clock by default; a custom clock going backwards credits nothing and re-anchors; huge forward jumps are capped by capacity | use the default clock in production                                                    |
| Memory growth             | unbounded keys exhaust memory                                                                                                        | lossless cleanup, `MaxUsers`, `Len()` metric                                           |
| Excessive contention      | hot bucket serializes callers                                                                                                        | benchmarks, sharding, edge enforcement                                                 |
| Distributed state failure | (if Redis is added) limiter dependency fails                                                                                         | fail-open, fail-closed, or local fallback, chosen explicitly                           |
| Incorrect client identity | everyone behind one NAT shares a bucket; one attacker rotating IPs escapes                                                           | authenticated identity first; IPv6 /64; generous IP limits                             |
| Proxy and header issues   | `X-Forwarded-For` spoofed or missing                                                                                                 | `KeyFromForwardedFor(n)` trusts only the rightmost `n` entries; fails when fewer exist |
| Process restart           | buckets reset to full                                                                                                                | accepted for local limiting; shared store if unacceptable                              |
| Table full (`MaxUsers`)   | new keys rejected with 429, no `Retry-After`                                                                                         | size the cap well above the legitimate working set; alert on `Len()`                   |

## 15. Security Considerations

- **API key abuse.** Only rate limit on a key after authenticating it, or a client can invent unlimited keys, each getting a fresh full bucket (bypassing the limit) and a table entry (consuming memory). Rate limit failed authentication separately, by IP, since those requests have no valid identity.
- **IP spoofing.** The TCP peer address can't be forged by the client in a completed connection; headers can. `KeyFromRemoteAddr` ignores headers.
- **Trusted proxy configuration.** `X-Forwarded-For` entries to the left are client-supplied. `KeyFromForwardedFor(n)` uses the entry `n` positions from the right, where `n` is the count of your own proxies. A wrong `n`, or a service reachable around the proxy, makes it bypassable.
- **Identity cardinality.** An attacker who controls identity controls table size. Key length is capped, `MaxUsers` bounds the table, and IPv6 is truncated to /64 so one host can't mint 2^64 keys.
- **Denial-of-service considerations.** The limiter is cheap per request but not free; put a coarse limiter or connection limits in front of the expensive parts. Key flooding against `MaxUsers` can lock out new users (I documented this trade-off). Sweeps hold the table write lock, so very large tables need sharding to avoid latency spikes.
- **Authentication endpoints** want a small capacity and a slow rate, keyed by both account and IP, so neither password spraying across accounts nor guessing against one account goes unthrottled.
- **429 responses leak information.** `Retry-After` tells an attacker exactly when to retry; for security-sensitive endpoints consider omitting it or adding jitter.

## 16. Deeper dive Questions

### Q1. Why token bucket?

**Short Answer.** It gives two independent knobs, burst size and sustained rate, in O(1) state with no window-boundary artifacts.

**Deeper Explanation.** Real clients are bursty: a page load fires ten requests at once. A token bucket allows that bounded burst and then enforces the average, which matches how most APIs describe their limits ("10 per second, bursts of 100"). Fixed window cannot express both and has the boundary problem; sliding window logs are exact but expensive.

**Tradeoffs.** Bursts are permitted by design. If the downstream can't take a burst, use a smaller capacity or a leaky-bucket queue.

### Q2. Why not fixed window?

**Short Answer.** A fixed window allows up to double the limit across a window boundary.

**Deeper Explanation.** With 100 requests per minute, a client sends 100 at 12:00:59 and 100 at 12:01:00: 200 in two seconds, each window compliant. The token bucket refills continuously, so the maximum over any interval is `capacity + rate x Δ` regardless of alignment.

**Tradeoffs.** Fixed window is simpler and cheaper, and fine for coarse quotas like per-day limits where the boundary effect doesn't matter.

### Q3. How does burst capacity work?

**Short Answer.** Capacity is the bucket size; a full bucket can serve that many requests instantly, after which throughput is limited to the refill rate.

**Deeper Explanation.** With capacity 100 and rate 10: 100 requests succeed at once, the 101st fails, and one second later exactly 10 more succeed. `TestLimiter_BurstThenSustained` shows this. After a long quiet period the bucket is full again, never more, because refill clamps.

**Tradeoffs.** Bigger capacity is friendlier to bursty clients and gives the downstream a larger spike to absorb.

### Q4. Why lazy refill?

**Short Answer.** Compute the tokens earned from elapsed time when a request arrives, instead of running timers.

**Deeper Explanation.** `refillLocked` does `tokens = min(capacity, tokens + elapsed x rate)`. The cost is O(1) per request, idle buckets cost nothing, there's no goroutine to manage, and with an injected clock everything is deterministic.

**Tradeoffs.** State is only current at the moment of a call (`Tokens()` computes on demand), and the clock read happens inside the critical section.

### Q5. Why not a background goroutine per bucket?

**Short Answer.** It scales badly and leaks unless carefully shut down.

**Deeper Explanation.** A goroutine and ticker per bucket means millions of goroutines for millions of users, even idle ones; they need explicit lifecycle management; refill is quantized to the tick and delayed when the scheduler is busy. A shared sweeper touches every bucket on every tick regardless of traffic.

**Tradeoffs.** None structural; lazy refill is strictly simpler. The one cost is reading the clock per request.

### Q6. How is thread safety guaranteed?

**Short Answer.** Every read or write of `tokens` and `last` happens under the bucket's mutex, and the whole refill-check-consume sequence is one critical section.

**Deeper Explanation.** The immutable fields (`rate`, `capacity`, `clock`) need no lock. The per-user map has its own `RWMutex`. Two locks are only ever taken in the fixed order table, global, user. Evidence: `-race` in CI, plus exact-count tests that would catch logic races the detector can't.

**Tradeoffs.** A mutex serializes callers on one bucket. That is the price of obvious correctness.

### Q7. What is the critical section?

**Short Answer.** Read the clock, refill, check availability, consume, update the timestamp.

**Deeper Explanation.** These share state and depend on each other: if another goroutine could run between refill and consume, it could consume the tokens just computed (spurious rejection), or both could pass the check (double spend). The clock read is inside so timestamps are applied in order; reading it outside can move `last` backwards and double-credit an interval.

**Tradeoffs.** `time.Now()` inside the lock lengthens the section slightly; correctness requires it.

### Q8. What happens with 1,000 concurrent requests?

**Short Answer.** With capacity 100 and no time passing, exactly 100 succeed and 900 are rejected; which ones is arbitrary.

**Deeper Explanation.** Goroutines serialize on the mutex, each does constant-time work. `TestConcurrent_AllowFrozenClockAdmitsExactlyCapacity` checks this shape with 6,400 attempts against 1,000 tokens. With a real clock the total is bounded by `capacity + rate x elapsed`.

**Tradeoffs.** No fairness or ordering among callers; `Wait` callers can be starved under sustained overload.

### Q9. How do you test race conditions?

**Short Answer.** Two complementary techniques: the race detector for data races, and exact-answer stress tests for logic races.

**Deeper Explanation.** `go test -race` finds unsynchronized access. It can't find check-then-act bugs where each access is atomic. A frozen clock makes the expected number of successes exact, so a double spend or lost update shows up as a wrong count. Add `-count`, `-shuffle=on` and `GOMAXPROCS=1` to vary scheduling.

**Tradeoffs.** Stress tests probabilistically expose bugs; they can't prove absence. Keeping the critical section small and the invariants stated is what gives confidence.

### Q10. How does per-user limiting work?

**Short Answer.** A map from key to its own bucket, guarded by an `RWMutex`, created on first use.

**Deeper Explanation.** `PerUser.take` validates `n`, read-locks the table for the common case, and upgrades to a write lock only to insert, re-checking after the upgrade. Different users contend only on the table's read lock. `TestPerUser_UsersAreIsolated` shows one user exhausting their bucket doesn't affect another.

**Tradeoffs.** The table is shared state with its own cost and lifecycle issues (next question).

### Q11. What happens when there are millions of users?

**Short Answer.** Memory grows with distinct keys, so the table needs cleanup and a cap.

**Deeper Explanation.** `Cleanup` deletes buckets that have refilled to capacity, which is lossless; `RunCleanup` schedules it under the caller's control; `MaxUsers` adds a hard bound, rejecting new keys when full. Key length is capped, IPv6 is truncated, and the `Len()` gauge shows growth. For very large tables, shard the map to shorten sweep pauses.

**Tradeoffs.** Retention is up to `capacity/rate`; `MaxUsers` fails closed for new users during key flooding.

### Q12. How would you distribute the limiter?

**Short Answer.** Move the bucket state to a shared store (commonly Redis) and make the check-and-consume atomic there, or enforce at a gateway.

**Deeper Explanation.** Local limiters give an effective limit of up to N times the configured one. A Lua script in Redis does refill, check and consume atomically, using the store's clock. Keep a local limiter in front so the store is protected and as a fallback.

**Tradeoffs.** Exactness versus an extra round trip and a new dependency. Hybrid leases reduce round trips at the cost of accuracy.

### Q13. What happens when an instance restarts?

**Short Answer.** All of its buckets reset to full.

**Deeper Explanation.** Nothing is persisted. During a rolling deploy every user gets a fresh burst on each restarted instance. Acceptable for fairness limits; not for hard quotas.

**Tradeoffs.** Persisting state adds latency and complexity; a shared store is the usual answer if restarts matter.

### Q14. How would Redis change the architecture?

**Short Answer.** The bucket state moves out of process; `Limiter` becomes a client of an atomic script; the request path gains a network round trip and a dependency.

**Deeper Explanation.** The interface stays familiar (`Take(key, n)`), but the implementation is a script that does the same lazy refill with the server's clock, and sets key expiry to the time to refill from empty, which reproduces my lossless-cleanup property for free. You add timeouts, a circuit breaker, a failure policy, and metrics for store latency.

**Tradeoffs.** Global exactness versus latency, availability and operational cost.

### Q15. How would you handle Redis failure?

**Short Answer.** Decide per endpoint: fail open, fail closed, or fall back to the local limiter.

**Deeper Explanation.** It depends on what the limit protects. Login protection argues for closed or degraded-local; a public read API argues for open or degraded-local. Use a short timeout and a circuit breaker so a slow store doesn't slow every request, and alert when in degraded mode.

**Tradeoffs.** Fail-open sacrifices protection; fail-closed sacrifices availability; local fallback sacrifices accuracy.

### Q16. What are the consistency guarantees?

**Short Answer.** Within one process, each bucket is linearizable: every decision sees all earlier ones. Across processes, there are none.

**Deeper Explanation.** The mutex gives a total order per bucket, so the supply bound holds exactly. Across instances, each has its own state, so the aggregate is between `limit` and `N x limit`. With a shared store, you get the store's consistency; replicas and failover can briefly give back spent tokens.

**Tradeoffs.** Stronger consistency costs latency and availability.

### Q17. What metrics would you expose?

**Short Answer.** Allowed and rejected counters, wait-duration histogram, active-users gauge, all labeled by limiter name only.

**Deeper Explanation.** The `Observer` interface reports outcomes without depending on a vendor; `PerUser.Len()` feeds the gauge. Alert on rejection ratio, on `Len()` near `MaxUsers`, and on wait times near deadlines.

**Tradeoffs.** No per-user breakdown in metrics; use sampled logs or top-K reports for that.

### Q18. Why shouldn't user ID be a Prometheus label?

**Short Answer.** Each distinct label value creates a new time series, so cardinality equals the number of users.

**Deeper Explanation.** Series multiply by other labels and histogram buckets, exhausting memory in the app and the monitoring stack, and an attacker who can mint identities controls that size. The `Observer` interface doesn't receive the key precisely so this is hard to do by accident.

**Tradeoffs.** You lose per-user dashboards and compensate with sampled logs, traces, tier labels or top-K.

### Q19. How would you benchmark this?

**Short Answer.** Benchmark each contention scenario separately, with `-benchmem`, repeated runs, and several `-cpu` values, on the target hardware.

**Deeper Explanation.** I have single-goroutine, all-goroutines-one-bucket, many-keys, hot-key and with-global benchmarks. Use `-count=10` and `benchstat` to tell real differences from noise, and confirm zero allocations on the hot path. A machine with few cores can't show contention, so numbers from it don't generalize.

**Tradeoffs.** Microbenchmarks isolate the limiter but not your service's real mix; validate with a load test.

### Q20. Where is the bottleneck?

**Short Answer.** The mutex on the hottest bucket, usually the global limiter, then the per-user table's shared lock.

**Deeper Explanation.** Every request passes through the global mutex when one is configured, capping parallelism no matter how many users exist. Per-user buckets spread contention. Confirm with the mutex profile rather than guessing.

**Tradeoffs.** Fixing it (sharding, leases) trades away exactness.

### Q21. How would you reduce lock contention?

**Short Answer.** First measure; then shard the hot bucket or the table, or switch to a lock-free representation.

**Deeper Explanation.** Options: GCRA with a single atomic word; K sub-buckets each with `rate/K`; sharded map; leasing token blocks to workers; moving enforcement to the edge. All trade exactness or simplicity for throughput.

**Tradeoffs.** The mutex version is the only one whose correctness argument fits in a paragraph.

### Q22. What happens if system time changes?

**Short Answer.** Nothing with the default clock, because it uses monotonic readings.

**Deeper Explanation.** `time.Now()` carries a monotonic reading that `Sub` uses, so wall-clock changes don't affect `elapsed`. A custom clock that goes backwards credits nothing and re-anchors `last` (`TestLimiter_ClockGoingBackwards`); a huge forward jump earns at most `capacity` because of the clamp.

**Tradeoffs.** Re-anchoring forfeits whatever would have accrued during the anomalous step, which errs on the side of fewer tokens. The alternative, freezing the bucket until the old timestamp is reached again, could block a client for as long as the clock jumped back.

### Q23. How does context cancellation work?

**Short Answer.** `Wait` loops: try to take a token, else sleep for the computed shortfall or until the context ends.

**Deeper Explanation.** A `select` on `ctx.Done()` and a timer wakes it on cancellation. If the required wait already exceeds the deadline, it returns immediately with an error wrapping `context.DeadlineExceeded` instead of sleeping for nothing. At most one timer per call, stopped on every exit; no goroutines are started, so nothing leaks (`TestWait_NoGoroutineLeak`).

**Tradeoffs.** No FIFO fairness; waiters retry and can lose to others.

### Q24. How would you calculate Retry-After?

**Short Answer.** Time until the bucket holds enough tokens: `(n - tokens) / rate`, rounded up to whole seconds.

**Deeper Explanation.** `Take` returns the exact duration, rounded up to the nanosecond in the limiter and up to the second in the header so a client obeying it doesn't retry early (`TestProperty_RetryAfterIsSufficient`). With two layers it's the larger of the two waits. It's a hint, not a reservation.

**Tradeoffs.** It leaks timing to attackers, and with thousands of clients told "retry in 1 second" you get a synchronized herd; clients should add jitter.

### Q25. What would you change for a global API?

**Short Answer.** Per-region limits with periodic rebalancing, rather than one synchronous global bucket.

**Deeper Explanation.** Cross-region round trips on every request are too slow. Give each region a share of the global limit, reconcile asynchronously, and accept bounded overshoot. Keep local limiters as the first line, move identity-aware limits to regional stores, and apply coarse global protection at the edge.

**Tradeoffs.** Accuracy versus latency and availability; the overshoot bound should be stated and accepted explicitly.

### Q26. Why float64 tokens?

**Short Answer.** Refill is fractional; floats keep the fractions and allow fractional rates.

**Deeper Explanation.** Rounding error is about 1e-16 relative and is resolved conservatively (the retry arithmetic rounds up). Multiplying before dividing makes common cases exact. I validated the implementation against an exact `big.Rat` model within 1e-6.

**Tradeoffs.** Integer nanotokens or GCRA are exact and lock-free-friendly but quantize rates and complicate the code.

### Q27. What if global passes but the user's limiter rejects?

**Short Answer.** Nothing should be consumed from the global bucket; I check both and then consume both, atomically.

**Deeper Explanation.** The naive `global.Allow() && user.Allow()` spends a global token on a rejected request, so an abusive user drains global capacity. Reversing the order leaks the other way. Refunds create windows of spurious rejection. `takeTwo` holds both locks, decides, then changes both or neither, with a fixed lock order. The tests prove 100 abusive requests leave the global bucket at exactly 8 of 10.

**Tradeoffs.** Global mutex held slightly longer; lock order is a rule future code must respect.

### Q28. Why does Cleanup only delete full buckets?

**Short Answer.** A full bucket is identical to a new one, so deleting it changes nothing.

**Deeper Explanation.** A fixed TTL would be lossy for slow rates: a user who spent their budget ten minutes ago at one token per hour would get a fresh full bucket. Deleting only full buckets is exact, and retention is at most `capacity/rate`.

**Tradeoffs.** Keys with slow refill stay in memory a long time; `MaxUsers` caps the damage.

### Q29. Why doesn't Wait reserve tokens ahead of time?

**Short Answer.** Reserving makes the balance negative and needs refunds on cancellation; retrying keeps `0 <= tokens` and is simpler.

**Deeper Explanation.** Libraries like `golang.org/x/time/rate` reserve and allow a debt. Here, waiters sleep for the computed shortfall and retry. A concurrent `Allow` can take the token first, in which case the waiter recomputes.

**Tradeoffs.** No ordering and possible starvation; bound waits with a deadline. A FIFO queue is the extension if fairness matters.

### Q30. How do you rate limit by IP safely?

**Short Answer.** Use the TCP peer address, treat IPv6 as /64, be generous, and never trust forwarded headers you don't control.

**Deeper Explanation.** NAT, corporate networks, mobile carriers and proxies put many people behind one address; IPv6 gives one person 2^64 addresses. `X-Forwarded-For` is client-controlled on the left, so `KeyFromForwardedFor(n)` uses the entry `n` positions from the right for `n` proxies you operate. Prefer authenticated identity and use IP as a coarse backstop.

**Tradeoffs.** Generous IP limits are weak protection; strict ones hurt shared-address users.

### Q31. What are the weaknesses of your implementation?

**Short Answer.** It's local to one process, `Wait` isn't fair, the table sweep pauses readers on very large maps, and `float64` has small rounding error.

**Tradeoffs.** Each was a conscious simplicity-for-scope trade, and the documents say so.
