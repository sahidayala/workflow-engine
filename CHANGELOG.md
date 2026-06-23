# Changelog

All notable changes to the Workflow Engine are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Planned
- `SELECT FOR UPDATE SKIP LOCKED` with advisory lock for high-concurrency workers
- Parallel step execution within a single workflow run
- Saga / compensation handlers for rollback flows
- Conditional branching step type (`condition`)
- Script execution step type (`script`)
- Prometheus metrics endpoint (`/metrics`)
- OpenTelemetry distributed tracing
- Rate limiting on the HTTP API
- Audit log for administrative actions

---

## [0.1.0] — 2024-06-23

Initial public release of the Workflow Engine.

### Added

**Core execution engine**
- In-process executor with poll loop (`FOR UPDATE SKIP LOCKED`) that dequeues
  and executes pending `step_run` rows without concurrent races.
- Adaptive idle backoff: starts at 1 s, backs off up to 30 s when the queue
  is empty, resets immediately when work is found.
- Jitter on every poll interval to prevent thundering-herd when multiple worker
  replicas run in parallel.

**Step types**
- `http_request` — configurable HTTP calls (GET, POST, PUT, DELETE) with custom
  headers, request body, and a 10 s timeout. Response is captured as
  `{status_code, body}` JSON for downstream chaining.
- `delay` — cancellable timed wait; respects context cancellation for clean
  shutdown.

**Retry engine**
- Per-step retry policy: `max_attempts` and `backoff_seconds` in step config.
- Exponential backoff: `base * 2^(attempt-1)` with ±20 % uniform jitter, capped
  at 24 h.
- Non-retriable classification: 4xx HTTP errors (except 429) and unknown step
  types skip retry immediately.

**Inter-step output chaining**
- Template syntax `{{steps.N.output.path.to.field}}` resolved at execution time
  from the `output` column of previously succeeded step runs.

**API**
- `POST /projects` — create project and default API key (key returned once).
- `GET  /projects` — get authenticated project.
- `POST /workflows` — create workflow definition with ordered steps.
- `POST /workflows/{id}/runs` — trigger a workflow run.
- `GET  /workflows` — list workflow definitions.
- `GET  /workflows/runs` — list all runs for the project.
- `GET  /workflows/runs/{id}` — get run detail with per-step timeline.
- `GET  /health` — liveness probe.
- `GET  /swagger/*` — interactive Swagger UI.

**Multi-tenancy**
- Project-scoped isolation: every query is filtered by `project_id`.
- Bearer API key auth: `wf_<prefix>.<secret>`. Secret stored as bcrypt hash
  only; prefix stored for fast lookup.

**Kafka consumer (optional)**
- Event-driven workflow triggers: register an `event_trigger` mapping an event
  type to a workflow; the Kafka consumer fires the workflow on matching events.
- Idempotency guard via `processed_integration_events` table.

**Event publishing (optional)**
- Lifecycle events (`workflow.run.started`, `step.run.started`,
  `step.run.succeeded`, `step.run.failed`, `workflow.run.completed`,
  `workflow.run.failed`) published best-effort to the Atlas Event Streaming API.
  Event failures are logged and never block execution.

**Infrastructure**
- PostgreSQL persistence with pgx/v5.
- 9 SQL migrations (up/down pairs).
- Two independent binaries: `cmd/api` (HTTP server + in-process executor) and
  `cmd/worker` (standalone worker).
- Multi-stage Dockerfile (`golang:1.25-alpine` builder → `alpine:3.20` runtime).
- Structured JSON logging with `log/slog`.
- Graceful shutdown on `SIGINT` / `SIGTERM`.

### Known Limitations

- No distributed locking beyond `FOR UPDATE SKIP LOCKED` — concurrent worker
  replicas dequeue independently but do not coordinate globally.
- No parallel step execution — steps within a run execute sequentially.
- No saga / compensation — a failed run does not roll back earlier steps.
- No metrics or tracing — observability is log-only.
- Kafka integration is optional and not covered by automated tests.
- Zero unit/integration tests in this release — the testing infrastructure is
  the first priority for v0.2.0.

[0.1.0]: https://github.com/SheykoWk/workflow-engine/releases/tag/v0.1.0
[Unreleased]: https://github.com/SheykoWk/workflow-engine/compare/v0.1.0...HEAD
