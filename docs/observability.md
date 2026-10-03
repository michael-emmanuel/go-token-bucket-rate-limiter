# Observability

The limiter reports outcomes through a small interface and keeps no dependency on any metrics library. This document describes what to measure, how to connect it to a metrics system, and why per-user labels must be avoided.

## 1. The interface

```go
type Observer interface {
    Allowed(n int)                   // a request for n tokens succeeded
    Rejected(n int)                  // a request for n tokens was denied
    Waited(d time.Duration, err error) // a Wait/WaitN call blocked for d
}
```

Set `Config.Observer` on a `Limiter`, or `PerUserConfig.Observer` on a `PerUser`. Contract, enforced by tests in `observer_test.go`:

- One `Allowed` or `Rejected` per call, including successful and failed `Wait`s; none for `n < 1`.
- `PerUser` reports once per request even when a global limiter is involved; the inner buckets are silent, so counts are not doubled.
- Methods run after the limiter's locks are released, on the caller's goroutine. They must be safe for concurrent use and cheap: they add to request latency, and a blocking observer blocks requests.
- There is no key parameter. That is deliberate (section 4).

`Counters` is a ready-made in-memory implementation built on `sync/atomic`, used in the tests and the HTTP example (`/metrics`).

## 2. Recommended metrics

| Metric | Type | Source | Labels |
|---|---|---|---|
| `rate_limit_allowed_total` | counter | `Observer.Allowed` | `limiter` (a fixed name such as `api` or `login`) |
| `rate_limit_rejected_total` | counter | `Observer.Rejected` | `limiter` |
| `rate_limit_wait_duration_seconds` | histogram | `Observer.Waited` | `limiter`, `outcome` (`ok`, `error`) |
| `rate_limit_active_users` | gauge | `PerUser.Len()` | `limiter` |
| `rate_limit_tokens_total` (optional) | counter | token arguments of `Allowed`/`Rejected` | `limiter`, `outcome` |

What to alert and look at:

- **Rejection ratio**: `rejected / (allowed + rejected)`. A sudden rise means an abusive client or a limit set too low; a constant zero for a limit that should bite suggests misconfiguration (for example a capacity far above the real traffic).
- **Active users versus `MaxUsers`**: approaching the cap means new users are about to be refused.
- **Wait duration p99 versus your deadline**: waits close to the deadline mean the limiter is saturated.
- **Cleanup**: `Len()` trending upward without bound means cleanup is not running.

## 3. Adapter sketch

An adapter to a Prometheus-style client, illustrative and not compiled in this repository (it would add a dependency):

```go
type promObserver struct {
    allowed, rejected prometheus.Counter
    wait              prometheus.Observer // histogram
}

func (o promObserver) Allowed(int)  { o.allowed.Inc() }
func (o promObserver) Rejected(int) { o.rejected.Inc() }
func (o promObserver) Waited(d time.Duration, err error) { o.wait.Observe(d.Seconds()) }

// Active users: a gauge computed at scrape time, no per-request cost.
prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "rate_limit_active_users"},
    func() float64 { return float64(users.Len()) })
```

Counters and gauge functions keep the hot path to an atomic add. The adapter lives in the application, so the limiter package stays free of vendor code and of the vendor's versioning.

## 4. Cardinality: why user ID must not be a label

In a label-based metrics system (Prometheus and its relatives) **every distinct combination of label values is a separate time series**. Each series costs memory in the client process, in the scraper and in storage, adds to scrape payload size, and slows queries.

A label `user_id` makes the series count equal to the number of users, multiplied by the other label values and, for histograms, by the number of buckets:

```
series = users x routes x outcomes x histogram_buckets
       = 1,000,000 x 10 x 2 x 12  =  240,000,000
```

Consequences: memory exhaustion in the application (the exporter holds every series), an exploding monitoring bill, slow or failing dashboards, and a **self-inflicted denial of service**, because an attacker who can invent identities (random API keys, rotating IPs) now controls how many series your monitoring stack must hold. The rate limiter's own per-user table is bounded and cleaned up; a metrics registry typically is not, so series for long-gone users linger.

Rules:

1. Labels must have small, fixed value sets: limiter name, route template (not the raw path), outcome, tier.
2. Never label by user ID, API key, IP address, raw URL, or error string.
3. Put the unbounded dimension somewhere built for it:
   - **Logs**: log rejections with the key, *sampled or rate limited themselves* so a flood of rejections does not flood the log pipeline.
   - **Traces**: attach the decision to the request span.
   - **Top-K reports**: periodically compute the heaviest keys in-process and log or expose the top few (bounded by K).
   - **Tiers**: if per-customer visibility is needed and there are few large customers, label by tier or by an allow-listed set of enterprise tenant IDs.

This is also why `Observer` receives no key: the interface makes the mistake hard to write.

## 5. Logging rejections

The limiter does not log; the middleware returns 429 and nothing more. A sensible application policy:

- Log the first rejection per key per interval at info level with the key, route and retry hint.
- Count the rest in the counters above.
- Never log API keys in clear text; log a short hash or the key's owner ID.

## 6. What is not measured

Decisions are cheap and there is no per-call latency metric for `Allow`. If a profile ever shows the limiter mutex to matter, the benchmarks in `bench_test.go` (see [testing.md](testing.md)) and Go's mutex profiler (`runtime.SetMutexProfileFraction`, then `go tool pprof` on the mutex profile) are the tools to use, rather than always-on instrumentation.
