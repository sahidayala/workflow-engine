.DEFAULT_GOAL := help

# ── Build ─────────────────────────────────────────────────────────────────────

.PHONY: build
build: ## Build both binaries (api + worker)
	go build -o bin/api  ./cmd/api
	go build -o bin/worker ./cmd/worker

.PHONY: build-api
build-api: ## Build the API server binary
	go build -o bin/api ./cmd/api

.PHONY: build-worker
build-worker: ## Build the worker binary
	go build -o bin/worker ./cmd/worker

# ── Run ───────────────────────────────────────────────────────────────────────

.PHONY: run
run: ## Run the API server (requires DATABASE_URL)
	go run ./cmd/api

.PHONY: run-worker
run-worker: ## Run the worker process (requires DATABASE_URL)
	go run ./cmd/worker

# ── Test ──────────────────────────────────────────────────────────────────────

.PHONY: test
test: ## Run all tests
	go test ./...

.PHONY: test-verbose
test-verbose: ## Run all tests with verbose output
	go test -v ./...

.PHONY: test-race
test-race: ## Run tests with the race detector
	go test -race ./...

# ── Lint & Vet ────────────────────────────────────────────────────────────────

.PHONY: vet
vet: ## Run go vet
	go vet ./...

.PHONY: lint
lint: ## Run golangci-lint (install: brew install golangci-lint)
	golangci-lint run ./...

.PHONY: tidy
tidy: ## Tidy go.mod and go.sum
	go mod tidy

.PHONY: tidy-check
tidy-check: ## Verify go.mod and go.sum are up-to-date (CI use)
	go mod tidy
	git diff --exit-code go.mod go.sum

# ── Swagger ───────────────────────────────────────────────────────────────────

.PHONY: swagger
swagger: ## Regenerate OpenAPI spec (requires swag: go install github.com/swaggo/swag/cmd/swag@latest)
	go generate ./cmd/api/...

# ── Database ──────────────────────────────────────────────────────────────────

.PHONY: db-create
db-create: ## Create the workflow-engine database
	psql "$(DATABASE_URL)" -c 'CREATE DATABASE "workflow-engine";' || true

.PHONY: migrate
migrate: ## Apply all SQL migrations (requires DATABASE_URL)
	./internal/infrastructure/db/migrate-up.sh

.PHONY: migrate-down
migrate-down: ## Revert all SQL migrations (destructive — dev only)
	@echo "Applying down migrations in reverse order..."
	@for f in $$(find internal/infrastructure/db/migrations -name '*.down.sql' | sort -r); do \
		echo "==> $$f"; \
		psql "$(DATABASE_URL)" -v ON_ERROR_STOP=1 -f "$$f"; \
	done

# ── Docker ────────────────────────────────────────────────────────────────────

.PHONY: docker-build
docker-build: ## Build both Docker images
	docker build --build-arg SERVICE=api    -t workflow-engine-api:dev    .
	docker build --build-arg SERVICE=worker -t workflow-engine-worker:dev .

.PHONY: up
up: ## Start PostgreSQL via docker compose (standalone)
	docker compose -f docker-compose.dev.yml up -d

.PHONY: down
down: ## Stop and remove containers (standalone)
	docker compose -f docker-compose.dev.yml down

# ── Setup ─────────────────────────────────────────────────────────────────────

.PHONY: setup
setup: ## One-shot local dev setup: install deps → create DB → migrate
	@echo "→ Downloading Go modules..."
	go mod download
	@echo "→ Creating database (if needed)..."
	psql "$(DATABASE_URL)" -c '' 2>/dev/null || \
		psql "$$(echo $(DATABASE_URL) | sed 's|/workflow-engine|/postgres|')" \
		     -c 'CREATE DATABASE "workflow-engine";'
	@echo "→ Running migrations..."
	$(MAKE) migrate
	@echo "✓ Setup complete. Run: make run"

.PHONY: clean
clean: ## Remove build artifacts
	rm -rf bin/

# ── Help ──────────────────────────────────────────────────────────────────────

.PHONY: help
help: ## Print this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'
