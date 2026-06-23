# Workflow Engine

A self-hosted, PostgreSQL-backed workflow orchestration engine for executing
durable multi-step processes with automatic retries, exponential backoff, and
inter-step output chaining.

[![CI](https://github.com/SheykoWk/workflow-engine/actions/workflows/ci.yml/badge.svg)](https://github.com/SheykoWk/workflow-engine/actions/workflows/ci.yml)
[![Go Version](https://img.shields.io/badge/go-1.25-blue)](go.mod)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## Why This Exists

Modern distributed systems need multi-step workflow orchestration — payment
pipelines, onboarding flows, integration retries, approval chains. Teams
typically solve this inside application code, which leads to duplicated retry
logic, inconsistent failure recovery, and no centralised visibility.

This engine provides a reusable orchestration layer that requires only
**PostgreSQL** — no Redis, no message broker required for the core path.

---

## Key Features

- **Durable state** — workflow and step state is persisted in PostgreSQL and
  survives crashes or restarts.
- **Retry with exponential backoff** — per-step retry policy with configurable
  max attempts, base backoff, and ±20 % jitter; capped at 24 h.
- **Inter-step output chaining** — downstream steps reference upstream outputs
  via `{{steps.N.output.path.to.field}}` in their config.
- **Concurrent workers** — `FOR UPDATE SKIP LOCKED` prevents duplicate
  execution when multiple worker replicas run against the same database.
- **Multi-tenant isolation** — every resource is scoped to a project; all
  queries filter by `project_id`.
- **Event-driven triggers** *(optional)* — register event-type → workflow
  mappings; the Kafka consumer triggers runs on matching integration events.
- **Lifecycle events** *(optional)* — publish `workflow.run.started`,
  `step.run.succeeded`, etc. to an external event store for audit/replay.
- **Two binaries** — `cmd/api` serves HTTP and can embed the executor;
  `cmd/worker` runs the executor only for independent horizontal scaling.

---

## Architecture Overview

```
  Client
    │ Bearer wf_prefix.secret
    ▼
┌──────────────────────────────────────────────────────────┐
│                    API Server (cmd/api)                   │
│  Auth Middleware → HTTP Handlers → WorkflowService        │
│                                   Executor (goroutine)   │
└──────────────────────────────────┬───────────────────────┘
                                   │
                          ┌────────▼─────────┐
                          │    PostgreSQL     │
                          │  projects         │
                          │  api_keys         │
                          │  workflows        │
                          │  workflow_steps   │
                          │  workflow_runs    │
                          │  step_runs        │
                          └────────▲─────────┘
                                   │ poll (FOR UPDATE SKIP LOCKED)
┌──────────────────────────────────┴───────────────────────┐
│                  Worker (cmd/worker)  [optional]          │
└──────────────────────────────────────────────────────────┘
```

The codebase follows **Hexagonal Architecture** (ports & adapters):

| Layer | Package | Responsibility |
| --- | --- | --- |
| Entrypoints | `cmd/api`, `cmd/worker` | Wiring, server lifecycle |
| HTTP Adapter | `internal/interfaces/http` | Parse requests, render JSON |
| Use Cases | `internal/app` | Business logic, validation |
| Executor | `internal/app/executor` | Poll and execute step runs |
| Domain | `internal/domain` | Core types, status enums |
| DB Adapter | `internal/infrastructure/db` | SQL queries, repositories |

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the full execution flow,
state machine diagram, and scaling model.

---

## Quick Start

> Prerequisites: Go 1.21+, PostgreSQL 14+, `psql` on `$PATH`.

```bash
# 1. Clone
git clone https://github.com/SheykoWk/workflow-engine.git
cd workflow-engine

# 2. Configure
cp .env.example .env
# Edit .env and set DATABASE_URL

# 3. Create database and run migrations
make up       # starts postgres via docker compose (or use your own)
make migrate

# 4. Start the API server
make run
# → http://localhost:8080
# → http://localhost:8080/swagger/index.html
```

In a second terminal, start the worker:

```bash
make run-worker
```

---

## Running Locally

### Prerequisites

| Tool | Version | Purpose |
| --- | --- | --- |
| Go | 1.21+ | Build and run |
| PostgreSQL | 14+ | State store |
| Docker + Compose | Any | Spin up local postgres (optional) |

### Step-by-step

**1. Start PostgreSQL**

Using the bundled compose file (recommended for local dev):

```bash
make up
```

Or use an existing PostgreSQL instance and set `DATABASE_URL` in `.env`.

**2. Apply migrations**

```bash
make migrate
```

This runs all `.up.sql` files in `internal/infrastructure/db/migrations/`.

**3. Start the API server**

```bash
make run
```

**4. Create a project and get an API key**

```bash
curl -s -X POST http://localhost:8080/projects \
  -H 'Content-Type: application/json' \
  -d '{"name":"my-project"}' | jq .
```

Response:

```json
{
  "project": { "id": "...", "name": "my-project", "slug": "my-project-abc123" },
  "api_key":  { "key": "wf_AbCdEfGhI.xYz..." }
}
```

Save the `key` — it is shown only once.

**5. Create a workflow**

```bash
export KEY="wf_AbCdEfGhI.xYz..."

curl -s -X POST http://localhost:8080/workflows \
  -H "Authorization: Bearer $KEY" \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "hello-world",
    "steps": [
      {
        "name": "wait-1s",
        "type": "delay",
        "config": { "seconds": 1 }
      },
      {
        "name": "call-api",
        "type": "http_request",
        "config": {
          "method": "GET",
          "url": "https://httpbin.org/get",
          "retry": { "max_attempts": 3, "backoff_seconds": 2 }
        }
      }
    ]
  }' | jq .
```

**6. Trigger a run**

```bash
export WF_ID="<id from above>"

curl -s -X POST "http://localhost:8080/workflows/$WF_ID/runs" \
  -H "Authorization: Bearer $KEY" | jq .
```

**7. Poll the run status**

```bash
export RUN_ID="<run_id from above>"

curl -s "http://localhost:8080/workflows/runs/$RUN_ID" \
  -H "Authorization: Bearer $KEY" | jq '.status, .stepRuns[].status'
```

---

## Configuration

| Variable | Required | Default | Description |
| --- | --- | --- | --- |
| `DATABASE_URL` | **Yes** | — | PostgreSQL DSN. Format: `postgresql://user:pass@host:port/db?sslmode=disable` |
| `HTTP_ADDR` | No | `:8080` | TCP address the API server listens on |
| `ENABLE_SWAGGER` | No | `false` | Mount Swagger UI at `/swagger/` |
| `ALLOWED_ORIGINS` | No | `""` | Comma-separated CORS allowed origins |
| `EVENT_STREAMING_BASE_URL` | No | — | Base URL of the Atlas Event Streaming API. When set, lifecycle events are published. |
| `EVENT_STREAMING_API_TOKEN` | No | — | API token for the event streaming service |
| `KAFKA_BROKERS` | No | — | Comma-separated Kafka broker addresses. Required for event-driven triggers. |
| `KAFKA_EVENTS_TOPIC` | No | — | Kafka topic for integration events |
| `KAFKA_CONSUMER_GROUP` | No | `workflow-engine` | Kafka consumer group ID |
| `DEMO_MODE` | No | `false` | Pre-seed a fixed project and API key (dev/demo only) |
| `DEMO_API_KEY` | No | — | API key for the demo project |
| `DEMO_PROJECT_ID` | No | — | Fixed UUID for the demo project |

---

## API Reference

| Method | Path | Auth | Description |
| --- | --- | --- | --- |
| `GET` | `/health` | No | Liveness probe |
| `POST` | `/projects` | No | Create project + return default API key |
| `GET` | `/projects` | Yes | Get authenticated project |
| `POST` | `/workflows` | Yes | Create workflow definition with steps |
| `GET` | `/workflows` | Yes | List workflow definitions |
| `POST` | `/workflows/{id}/runs` | Yes | Trigger a workflow run |
| `GET` | `/workflows/runs` | Yes | List all runs for the project |
| `GET` | `/workflows/runs/{id}` | Yes | Get run detail with per-step timeline |
| `GET` | `/swagger/*` | No | Interactive Swagger UI |

Full OpenAPI spec: [docs/swagger.yaml](docs/swagger.yaml)

### Step Types

**`delay`** — pause execution for N seconds.

```json
{ "seconds": 10 }
```

**`http_request`** — make an HTTP call; supports output chaining.

```json
{
  "method": "POST",
  "url": "https://api.example.com/webhook",
  "headers": { "X-API-Key": "secret" },
  "body": { "user_id": "{{steps.0.output.body.id}}" },
  "retry": { "max_attempts": 3, "backoff_seconds": 5 }
}
```

The response is captured as `{ "status_code": 200, "body": ... }` and
available to downstream steps via `{{steps.N.output.status_code}}` or
`{{steps.N.output.body.<field>}}`.

---

## Development

```bash
make build        # compile both binaries to ./bin/
make test         # go test ./...
make test-race    # test with race detector
make vet          # go vet ./...
make lint         # golangci-lint (install: brew install golangci-lint)
make tidy         # go mod tidy
make swagger      # regenerate OpenAPI spec
make help         # list all targets
```

### Adding a New Step Type

1. Add a case in `executeStep` — `internal/app/executor/executor.go`.
2. Implement `run<Type>` in a new file `internal/app/executor/<type>.go`.
3. Update the `type` enum in Swagger annotations on `CreateWorkflowStepRequest`.
4. Add tests.

---

## Roadmap

- [ ] `SELECT FOR UPDATE` with advisory lock for very high-concurrency scenarios
- [ ] Parallel step execution within a single run
- [ ] Conditional branching step type (`condition`)
- [ ] Script execution step type (`script`)
- [ ] Saga / compensation handlers for rollback flows
- [ ] Prometheus metrics (`/metrics`)
- [ ] OpenTelemetry distributed tracing
- [ ] Rate limiting on the HTTP API
- [ ] Unit and integration test suite

See [CHANGELOG.md](CHANGELOG.md) for what is already implemented.

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

---

## Security

See [SECURITY.md](SECURITY.md) for the security model and how to report
vulnerabilities.

---

## License

MIT — see [LICENSE](LICENSE).
