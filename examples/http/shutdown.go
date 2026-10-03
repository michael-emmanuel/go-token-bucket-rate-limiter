package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

// newShutdownContext returns a context cancelled by SIGINT or SIGTERM. The
// stop function is intentionally not returned: the context lives for the
// process lifetime.
func newShutdownContext() context.Context {
	ctx, _ := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	return ctx
}
