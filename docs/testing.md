# Testing

```
go test ./...                        # unit, property and concurrency tests
go test -race ./...                  # the same, under the race detector
go test -bench=. ./...               # tests plus benchmarks
go test -run '^$' -bench . -benchmem ./limiter   # benchmarks only, with allocations
make check                           # gofmt, vet, test, test -race, benchmark smoke run
```

The suite lives in `limiter/*_test.go`, in the package itself so tests can inspect invariants (`l.tokens`) directly. It runs in a few seconds and is free of sleeps except where real time is the thing under test.

## 1. Unit tests

| File | Covers |
|---|---|
| `token_bucket_test.go` | config validation, initially full, consume, empty rejects, refill, fractional tokens, cap on refill, burst then sustained, `AllowN` (partial, exact capacity, invalid `n`), `Take` retry hints, clock going backwards, overflow cases, and the documented walk-through |
| `wait_test.go` | `Wait`/`WaitN`: immediate, blocking, invalid arguments, cancellation, deadline fail-fast, goroutine leak, many waiters respecting the rate |
| `per_user_test.go` | isolation, global limit, no token leakage, concurrent accounting, cleanup, `MaxUsers`, `RunCleanup` |
| `middleware_test.go` | 200/429/400, `Retry-After` rounding, key extraction incl. IPv6 and `X-Forwarded-For` |
| `observer_test.go` | exactly-once notification, `Wait` accounting, `Counters` |

Test names state the behavior (`TestLimiter_EmptyBucketRejects`, `TestPerUser_NoTokenLeakageBetweenLayers`), so a failure reads as a sentence.

## 2. Deterministic time

The refill logic depends on time, and tests that call `time.Sleep` are slow and flaky. `Config.Clock` lets tests inject a clock they control:

```go
clk := newFakeClock()                        // starts at a fixed instant
l := mustLimiter(t, Config{Rate: 10, Capacity: 10, Clock: clk})
drain(l)
clk.Advance(100 * time.Millisecond)          // exactly one token earned
// l.Tokens() == 1, exactly
```

Advancing by whole milliseconds at integral rates gives exact results (see the multiply-before-divide note in [token-bucket.md](token-bucket.md)), so most tests assert equality rather than tolerances. The clock interface is a single method; nothing else in the design needed abstracting.

`Wait` sleeps with real timers, so its tests use the real clock with fast rates (a token every few milliseconds). Those tests assert only bounds that are guaranteed, such as "a wait of one token at 100/s did not return before 5 ms" and "50 waiters at 1000/s cannot finish before 48 ms", since timers never fire early, and never tight upper bounds that could flake on a loaded CI machine.

## 3. Concurrency tests

Thread safety is tested by stress plus exact accounting, not by hoping the scheduler produces a bad interleaving.

- **Frozen clock, exact count** (`TestConcurrent_AllowFrozenClockAdmitsExactlyCapacity`): 64 goroutines make 6,400 attempts against 1,000 tokens. With no refill, the number of successes must be exactly 1,000. More means double spending; fewer means a lost update.
- **Conservation** (`TestConcurrent_AllowNConservesTokens`): goroutines request random amounts; `granted + remaining` must equal the starting capacity exactly.
- **Bounded budget with a real clock** (`TestConcurrent_AllowRealClockStaysWithinBudget`): admitted requests must not exceed `capacity + rate x elapsed`, and must not be implausibly far below it.
- **Layered accounting** (`TestPerUser_ConcurrentGlobalAndUserAccounting`): 50 users, 200 goroutines; successes equal the global capacity exactly, and tokens spent across users equal tokens spent by the global bucket, so no layer leaks.
- **Table, cleanup and Len together** (`TestPerUser_ConcurrentRequestsWithCleanup`): requests, insertions, sweeps running in a `RunCleanup` goroutine, and `Len` calls at the same time, with `MaxUsers` enforced.

## 4. The race detector

`go test -race` compiles the code with instrumentation that reports unsynchronized concurrent memory access. Run it on every change: the CI workflow and `make check` do. It requires cgo and a C toolchain on some platforms, and makes tests several times slower, which is why it is a separate command rather than the default.

What it finds: data races, such as reading `l.tokens` outside the lock. What it cannot find: logic races where every access is synchronized but the algorithm is wrong, such as check-then-act with atomics ([concurrency.md](concurrency.md), section 3.1). The exact-count tests above exist to cover that gap; neither technique alone demonstrates thread safety.

Useful flags for hunting flakiness: `-count=20` repeats tests, `-shuffle=on` randomizes order, `GOMAXPROCS=1` forces interleaving through the scheduler alone. The suite passes all of these.

## 5. Property and invariant tests (`property_test.go`)

These tests state what must be true for *any* sequence of operations, then check it against thousands of randomized sequences (fixed seeds, so failures reproduce).

| Test | Property |
|---|---|
| `TestProperty_MatchesExactModel` | 300 random parameter sets x 400 random operations (idle gaps from zero to 48 hours, requests from 1 to `capacity + 1`). Every decision is compared with an **exact rational-arithmetic model** (`math/big.Rat`) of `tokens = min(C, tokens + elapsed x r)`. The implementation may differ from the model only within 1e-6 tokens (float rounding); outside that band the decision must match. Also asserts `0 <= tokens <= capacity` after every step. |
| `TestProperty_NeverExceedsSupply` | total granted never exceeds `capacity + rate x elapsed` |
| `TestProperty_RetryAfterIsSufficient` | waiting exactly the advertised `RetryAfter` always makes the request succeed |
| `TestPerUser_CleanupIsLossless` | a table swept at random moments makes the same decisions as one never swept, with a global limiter |

The model-based test is the strongest single check in the repository: it would catch off-by-one refill, missing clamps, lost fractions, and sign errors that line coverage would not.

A subtle point the model test taught during development: after a request rejected up front (`n > capacity`) the limiter's raw `tokens` field is legitimately stale, since refill is lazy. The test therefore compares `Tokens()`, which applies pending refill, not the raw field.

## 6. Benchmarks (`bench_test.go`)

| Benchmark | Measures |
|---|---|
| `BenchmarkLimiter_Allow` | uncontended cost of one decision (includes one `time.Now`) |
| `BenchmarkLimiter_AllowParallel` | every goroutine on one bucket: worst-case mutex contention, admit path |
| `BenchmarkLimiter_AllowParallel_Rejecting` | the same on the reject path |
| `BenchmarkPerUser_Sequential` | map lookup plus decision over 4,096 keys, no contention |
| `BenchmarkPerUser_AllowParallel` | goroutines spread across keys: table read lock plus separate bucket mutexes |
| `BenchmarkPerUser_AllowParallel_HotKey` | all goroutines on one key through the table |
| `BenchmarkPerUser_AllowParallel_WithGlobal` | adds the shared global limiter, reintroducing one contended mutex |

Reading them:

- Buckets are configured with huge capacity and rate so the admit path is measured; the reject-path benchmark says so in its name.
- The real clock is used, so `time.Now` is part of every number; that is what a caller pays.
- `-benchmem` reports allocations. The hot path is expected to allocate nothing.
- Results depend on hardware, `GOMAXPROCS` and what else the machine is doing. Compare runs on the same machine, repeat with `-count=10`, and use a statistics tool such as `benchstat` (an external tool, not a dependency of this repository) before concluding one variant is faster. The repository makes no performance claims, and none should be inferred from running it on a machine with few cores, where "parallel" benchmarks cannot show contention at all.
- Try `-cpu 1,2,4,8` to see how each benchmark scales with parallelism.

## 7. Coverage and what it means

`go test -cover ./...` reports the percentage of statements executed. It says which code is *never run*; it does not say the code is *correct*. The invariant tests, the exact model and the exact-count concurrency tests are what establish correctness. Use coverage to find untested branches, not as a goal.

## 8. Toolchains

The module declares Go 1.22. The suite and examples have been run on Go 1.22 and on Go 1.24 (including `-race`); CI runs the declared minimum and the latest stable release.
