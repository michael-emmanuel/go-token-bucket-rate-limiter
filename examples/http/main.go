// Command http is a small service protected by the rate limiting middleware.
//
// Run the server:
//
//	go run ./examples/http
//	for i in $(seq 1 8); do curl -s -o /dev/null -w '%{http_code}\n' localhost:8080/; done
//
// Or run a self-contained demonstration that starts the server in-process,
// sends requests, and exits:
//
//	go run ./examples/http -demo
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/example/token-bucket-rate-limiter/limiter"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	demo := flag.Bool("demo", false, "start the server on a random port, send requests, and exit")
	flag.Parse()

	metrics := &limiter.Counters{}
	users, err := limiter.NewPerUser(limiter.PerUserConfig{
		// 5 requests in a burst, then 1 per second, per client.
		Limit:    limiter.Config{Rate: 1, Capacity: 5},
		MaxUsers: 100_000,
		Observer: metrics,
	})
	if err != nil {
		log.Fatal(err)
	}

	// Identity: an API key if the client presents one, otherwise its peer
	// address. A real service would authenticate the key before limiting on it
	// (see docs/architecture.md); this example trusts it for brevity. No
	// X-Forwarded-For is consulted because this server is not behind a proxy.
	identity := limiter.FirstKey(
		limiter.KeyFromHeader("X-API-Key", "key:"),
		limiter.KeyFromRemoteAddr(),
	)

	mux := http.NewServeMux()
	mux.Handle("/", limiter.Middleware(users, identity)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "hello")
	})))
	// Metrics and health checks sit outside the limiter on purpose.
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		s := metrics.Snapshot()
		_ = json.NewEncoder(w).Encode(map[string]any{
			"rate_limit_allowed_total":  s.AllowedRequests,
			"rate_limit_rejected_total": s.RejectedRequests,
			"active_users":              users.Len(),
		})
	})

	// The cleanup goroutine is owned here, and stops when the process exits.
	// A service with graceful shutdown would pass a cancellable context.
	ctx := newShutdownContext()
	go func() { _ = users.RunCleanup(ctx, time.Minute) }()

	if *demo {
		runDemo(mux)
		return
	}
	log.Printf("listening on %s", *addr)
	srv := &http.Server{Addr: *addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	log.Fatal(srv.ListenAndServe())
}

func runDemo(handler http.Handler) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	srv := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = srv.Serve(ln) }()
	base := "http://" + ln.Addr().String()

	get := func(label, apiKey string) {
		req, _ := http.NewRequest(http.MethodGet, base+"/", nil)
		if apiKey != "" {
			req.Header.Set("X-API-Key", apiKey)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Fatal(err)
		}
		resp.Body.Close()
		fmt.Printf("%-28s -> %d  Retry-After=%q\n", label, resp.StatusCode, resp.Header.Get("Retry-After"))
	}

	for i := 1; i <= 7; i++ {
		get(fmt.Sprintf("anonymous request %d", i), "")
	}
	get("api key 'alice' (own bucket)", "alice")

	resp, err := http.Get(base + "/metrics")
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()
	fmt.Print("metrics: ")
	_, _ = os.Stdout.ReadFrom(resp.Body)
}
