# The Token Bucket Algorithm

This document explains the algorithm as implemented in `limiter/token_bucket.go`: the model, the math, the representation choices and their consequences, and how it compares to other algorithms. Every numeric example in the worked walk-through is replayed by `TestDocumentedWalkthrough`, so it cannot drift from the code.

## 1. The model

A bucket has three quantities:

| Symbol | Name | Meaning | In the code |
|---|---|---|---|
| `C` | capacity | maximum tokens the bucket can hold | `Config.Capacity` |
| `r` | refill rate | tokens added per second | `Config.Rate` |
| `T` | current tokens | request budget available right now | `Limiter.tokens` |

A request for `n` tokens succeeds if `T >= n`, in which case `T` decreases by `n`. Otherwise it is rejected (or, for `Wait`, delayed) and `T` is unchanged.

Two independent limits fall out of this:

- **Burst**: bounded by `C`. A full bucket can serve `C` requests instantly.
- **Sustained rate**: bounded by `r`. Once the stored tokens are spent, throughput cannot exceed `r` per second.

With `C = 100` and `r = 10`, a client may send 100 requests in one instant, then 10 per second thereafter. The two parameters are chosen independently: `r` protects the resource, `C` decides how much short-term variation to tolerate.

### The invariant

```
0 <= T <= C          at all times that the lock is not held
```

The lower bound holds because tokens are only deducted after checking `T >= n`, atomically with the deduction. The upper bound holds because every refill clamps: `T = min(C, T + earned)`.

Why it matters:

- `T > C` would silently turn the burst limit into a larger number. A bucket left idle overnight would accumulate a large credit and then admit a huge spike, which is exactly what the limiter exists to prevent. The clamp also makes long idle periods harmless: whether the bucket was idle for a second or a year, the result is "full".
- `T < 0` would mean the limiter has admitted work it had no budget for (double spending). It would also break the retry arithmetic, which assumes a non-negative deficit.

`TestProperty_MatchesExactModel` checks the invariant after every step of 120,000 randomized operations.

## 2. Lazy refill

The bucket does not run a timer. Each operation computes what it would have earned since the last one:

```
elapsed = now - last
earned  = elapsed * r
T       = min(C, T + earned)
last    = now
```

This is the `refillLocked` method. The alternatives and why they lose:

| Approach | Cost | Problem |
|---|---|---|
| Goroutine + ticker per bucket | one goroutine and one timer per bucket, forever | Millions of users means millions of goroutines. Needs explicit shutdown or it leaks. Tick granularity quantizes the rate. A slow scheduler delays refills, so tokens arrive late and in clumps. |
| Shared sweeper that refills every bucket | O(buckets) work per tick regardless of traffic | Touches idle buckets for nothing; contends with requests on every bucket. |
| **Lazy refill** | O(1) arithmetic per request | None structural. Idle buckets cost nothing; there is no goroutine to manage. |

Lazy refill also gives the useful property that state only changes when someone looks at it, which makes the bucket trivially deterministic under a fake clock.

## 3. Worked example

`rate = 5 tokens/s`, `capacity = 10`, bucket starts full.

| Time | Event | Tokens before (after refill) | Result | Tokens after |
|---|---|---|---|---|
| 0.0 s | 10 requests at once | 10 | all 10 allowed | 0 |
| 0.0 s | 11th request | 0 | rejected | 0 |
| 0.2 s | 1 request | 0 + 0.2 s x 5 = 1.0 | allowed | 0 |
| 0.3 s | 1 request | 0 + 0.1 s x 5 = 0.5 | rejected (0.5 < 1) | 0.5 |
| 0.4 s | 1 request | 0.5 + 0.5 = 1.0 | allowed | 0 |
| 1.4 s | `AllowN(3)` | 0 + 1.0 s x 5 = 5 | allowed | 2 |
| 1.4 s | `AllowN(3)` again | 2 | rejected, `RetryAfter` = (3 - 2) / 5 = 200 ms | 2 |
| 11.4 s | `Allow()` | 2 + 10 s x 5 = 52, clamped to 10 | allowed | 9 |

Things to notice:

- The rejected request at 0.3 s did not consume or discard the 0.5 tokens. Fractions persist, which is what lets a client pacing itself at exactly the refill rate succeed every time.
- `AllowN` is all-or-nothing: the failed `AllowN(3)` at 1.4 s leaves the 2 tokens in place.
- The last row shows the clamp: 52 would be wrong, because the bucket can hold 10.
- `RetryAfter` is the deficit divided by the rate: `(n - T) / r`.

## 4. The mathematical bound

Let the bucket start full. Over any interval of length `Δ` seconds, the total tokens granted is at most:

```
granted(Δ) <= C + r * Δ
```

The supply argument: tokens exist only as the initial `C` plus what refill has produced, `r * Δ` at most (clamping only removes supply, never adds). Since `T >= 0`, nothing can be granted beyond supply.

This is the guarantee a caller can rely on, and the one `TestConcurrent_AllowRealClockStaysWithinBudget` and `TestProperty_NeverExceedsSupply` assert. Note what it does not say: it bounds the *total* over an interval, not the *instantaneous* rate. Within the interval the whole budget may be spent in one instant.

## 5. Representation and precision

### Choice: `float64` tokens

The implementation stores `tokens` as a `float64`, time as `time.Time`, and computes `earned = float64(elapsed) * rate / 1e9`.

**Why floating point.** Refill is inherently fractional. At 10 tokens/s, a request 30 ms after the last has earned 0.3 tokens, and that 0.3 must not be discarded, or a steady client at exactly the limit would be rejected half the time. `float64` represents fractions directly, allows fractional rates (`0.5` tokens/s), and keeps the code short enough to audit.

**What it costs: rounding.**

- Values like 0.1 are not exactly representable, so repeated additions accumulate error of roughly 1e-16 relative per operation.
- The error can put a bucket a hair *below* a token count it mathematically has (for example 0.9999999999999999 where the exact answer is 1). The bucket then rejects a request by a few femtotokens. The retry arithmetic rounds *up* (`math.Ceil` on nanoseconds, minimum 1 ns), so the suggested wait always suffices; `TestProperty_RetryAfterIsSufficient` checks this across random rates.
- The opposite error (admitting when exactly short) is bounded the same way. `TestProperty_MatchesExactModel` compares the limiter against exact rational arithmetic (`math/big.Rat`) and allows disagreement only within 1e-6 tokens, where either answer is acceptable.
- **Multiply before dividing.** `float64(elapsed) * rate / 1e9` instead of `elapsed * (rate / 1e9)`. For integral rates and whole-millisecond durations the product is an exact integer, so the common cases (100 ms at 10/s is exactly 1) carry no rounding at all.
- **Absorbed increments.** A float near `T` has spacing of about `T * 2.2e-16`; an increment smaller than half that vanishes. With calls at least tens of nanoseconds apart, that requires `T` on the order of 1e8 times the rate, i.e. capacities far beyond practical limits. It is noted rather than ignored because it is the failure mode that would appear first if someone configured an enormous bucket with a tiny rate.

### Alternatives considered

| Representation | Advantage | Disadvantage |
|---|---|---|
| Integer tokens (whole tokens only) | Exact, no rounding | Drops fractions. At 1 token/s, refilling only whole tokens makes timing coarse; you must also track the leftover time yourself. |
| Fixed-point integer "nanotokens" (`tokens * 1e9` in an `int64`) | Exact arithmetic, deterministic across platforms | `rate` must become an integer nanotokens-per-second figure, so fractional rates are quantized. Overflow when `capacity * 1e9 > 9.2e18`, i.e. capacity above about 9.2e9, so it needs a checked bound. |
| **GCRA** (store one "theoretical arrival time" as an integer timestamp) | One integer of state; no accumulated float error; can be updated with a single compare-and-swap | The emission interval `1/rate` is rounded to integer nanoseconds, so rates above 1e9/s are not representable; the "tokens" concept is less visible, which hurts teaching. |
| `math/big.Rat` | Exact | Allocates; far too slow for a hot path. Used only in the test model. |

`float64` was chosen because the error is tiny, bounded, and resolved conservatively, and because the code stays readable. A production limiter aiming at lock-free updates or exact cross-platform determinism would likely pick GCRA or fixed-point.

### Time

- **Monotonic clocks.** The default `Clock` returns `time.Now()`. A `time.Time` from `time.Now` carries a monotonic reading, and `Time.Sub` uses it, so NTP steps or manual wall-clock changes cannot make `elapsed` jump.
- **Custom clocks.** A `Clock` that returns times without a monotonic reading (tests, or a clock built from `time.Unix`) can go backwards. `refillLocked` then credits nothing and re-anchors `last` to the new time, so the bucket neither mints tokens nor freezes until the old time is reached again (`TestLimiter_ClockGoingBackwards`). A large *forward* jump is harmless because the clamp caps the credit at `C`.
- **Resolution.** `time.Duration` is an `int64` of nanoseconds, more precise than any practical rate needs. `Duration` spans about 292 years, and `Time.Sub` saturates rather than wrapping, so "idle for a very long time" produces a huge but well-defined `elapsed`, which the clamp turns into "full" (`TestLimiter_RefillNeverExceedsCapacity` idles for the maximum duration).
- **Reading the clock inside the lock.** If a goroutine read the time first and locked afterwards, a slower goroutine could apply an older timestamp after a newer one, making `elapsed` negative or, worse, letting the same interval be credited twice. See [concurrency.md](concurrency.md).

### Overflow and underflow

- `float64(elapsed) * rate` can reach `+Inf` for a finite but enormous `rate`. `math.Min(capacity, tokens + Inf)` is `capacity`, so the result is still correct (`TestLimiter_HugeRateDoesNotOverflow`). `NaN` cannot arise because `Validate` rejects `NaN` and infinite parameters and `elapsed` is finite.
- The retry computation converts seconds to a `time.Duration`. For tiny rates the result would overflow `int64`, so it saturates at `math.MaxInt64` (`TestLimiter_RetryAfterSaturates`).
- `tokens` cannot underflow below zero because consumption is guarded by `tokens >= need`.
- `n` is an `int`; `float64(n)` is exact for any `int` up to 2^53.

## 6. Comparison with other algorithms

| | Token bucket | Fixed window | Sliding window (log) | Sliding window (counter) | Leaky bucket (meter) | Leaky bucket (queue) |
|---|---|---|---|---|---|---|
| State per key | 2 numbers | 2 numbers | one timestamp per request | 3 numbers | 2 numbers | a queue |
| Allows bursts | yes, up to `C` | yes, up to the window limit, and up to 2x across a boundary | no beyond the limit in any window | approximately | no (smooths) | no (smooths) |
| Boundary artifact | none | yes | none | small approximation error | none | none |
| Output shape | bursty up to `C` | bursty | bursty within limit | bursty within limit | constant rate | constant rate |
| Memory | O(1) | O(1) | O(limit) | O(1) | O(1) | O(queue depth) |
| Rejects or delays | rejects (or waits) | rejects | rejects | rejects | rejects | delays |

**Fixed window.** "100 requests per minute" implemented as a counter reset each minute. A client can send 100 requests at 12:00:59 and 100 more at 12:01:00: 200 requests in two seconds while never exceeding the limit in either window. The token bucket has no boundary: the burst is capped at `C` no matter when it happens, and refill is continuous. Fixed window is simpler and cheaper and is fine when the limit is coarse (daily quotas) and boundary effects are tolerable.

**Sliding window log.** Stores a timestamp per request and counts those in the last `W` seconds. Exact, with no boundary effect, but memory is O(limit) per key and each request scans or prunes. For a limit of 10,000/min per user it is impractical. Suitable when exactness matters and limits are small.

**Sliding window counter.** Interpolates between the current and previous fixed window counts. O(1) memory and a good approximation, but it is an estimate, and the model is "requests per window", so it offers no separate burst parameter.

**Leaky bucket.** Two distinct things share the name. As a *meter* it is nearly the same mathematics as a token bucket viewed in reverse (a counter that drains at a fixed rate and rejects when full), with the same average-rate and burst-size behavior. As a *queue* it buffers requests and releases them at a constant rate, producing perfectly smooth output and adding latency. Choose the queueing form when the downstream cannot tolerate any burst (traffic shaping to a fixed-rate link). Choose the token bucket when a burst up to a known size is acceptable or desirable and rejection is preferable to queueing.

## 7. When the token bucket is appropriate

- APIs with bursty but bounded clients, where the contract is "on average `r` per second, up to `C` at once".
- Protecting a downstream with its own burst tolerance: a database that handles 500 queries/s sustained but can absorb a short spike.
- Authentication endpoints: a small `C` (for example 5) and a slow `r` let a legitimate user mistype a password a few times while making credential stuffing slow. See the security notes in [architecture.md](architecture.md).
- Per-tenant fairness in a shared service, combined with a global bucket ([architecture.md](architecture.md)).

## 8. Limitations

- **Local scope.** One process's buckets know nothing of another's. See [distributed-rate-limiting.md](distributed-rate-limiting.md).
- **Bursts are a feature and a hazard.** A full bucket admits `C` requests at once. If the downstream cannot take that, lower `C` or use the queueing leaky bucket.
- **Counts requests, not cost.** Equal tokens per request ignore that some requests are more expensive. `AllowN` lets a caller charge more for expensive operations, but choosing the price is the application's job.
- **Rejection, not queueing.** `Allow` answers immediately. `Wait` delays, but without fairness guarantees ([concurrency.md](concurrency.md)).
- **No memory of history beyond the current level.** The bucket cannot answer "how many requests did this user send in the last hour".
- **Restart resets state.** A new process starts with full buckets.
