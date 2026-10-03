GO ?= go
PKGS := ./...

.PHONY: all test test-race benchmark vet fmt fmt-check cover examples check clean help

all: check

help: ## List targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | sed -e 's/:.*## /\t/'

test: ## Run all tests
	$(GO) test $(PKGS)

test-race: ## Run all tests under the race detector
	$(GO) test -race $(PKGS)

benchmark: ## Run benchmarks (no tests)
	$(GO) test -run '^$$' -bench . -benchmem $(PKGS)

vet: ## Run go vet
	$(GO) vet $(PKGS)

fmt: ## Format all Go files in place
	gofmt -w .

fmt-check: ## Fail if any file is not gofmt-formatted
	@out="$$(gofmt -l .)"; \
	if [ -n "$$out" ]; then echo "gofmt needed for:"; echo "$$out"; exit 1; fi

cover: ## Run tests with a coverage summary
	$(GO) test -cover $(PKGS)

examples: ## Run the self-terminating examples
	$(GO) run ./examples/basic
	$(GO) run ./examples/burst
	$(GO) run ./examples/peruser
	$(GO) run ./examples/wait
	$(GO) run ./examples/http -demo

check: fmt-check vet test test-race ## Everything CI runs
	$(GO) test -run '^$$' -bench . -benchtime=1x $(PKGS) > /dev/null

clean: ## Remove test caches
	$(GO) clean -testcache
