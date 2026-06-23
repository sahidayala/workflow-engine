# Architecture

## Overview

The engine follows **hexagonal architecture** (ports & adapters). Dependencies always point inward — the domain has no external imports, and infrastructure is swappable.

```
┌─────────────────────────────────────────────────────────┐
│                     cmd/api / cmd/worker                │
│                    (entrypoints, wiring)                 │
└────────────────────────┬────────────────────────────────┘
                         │
         ┌───────────────┼───────────────┐
         ▼               ▼               ▼
┌──────────────┐  ┌────────────┐  ┌───────────────────┐
│  interfaces  │  │    app     │  │  infrastructure   │
│  http/       │  │  workflow  │  │  db/              │
│  (handlers,  │  │  executor/ │  │  (repositories,   │
│   router)    │  │            │  │   migrations)     │
└──────────────┘  └────────────┘  └───────────────────┘
                         │
                         ▼
                  ┌────────────┐
                  │   domain   │
                  │  (types,   │
                  │   enums)   │
                  └────────────┘
```

### Layer Responsibilities

| Layer | Package | Purpose |
|-------|---------|---------|
| Entrypoints | `cmd/api`, `cmd/worker` | Wire deps, start server/executor |
| HTTP Adapter | `internal/interfaces/http` | Parse requests, call app layer, render JSON |
| App / Use Cases | `internal/app` | Business logic, validation, orchestration |
| Executor | `internal/app/executor` | Poll and execute pending step_runs |
| Domain | `internal/domain` | Core types, status enums — no framework imports |
| DB Adapter | `internal/infrastructure/db` | SQL queries, transactions, repository impls |

---

## Multi-Tenancy

Every entity is scoped to a **Project**. A project is the tenant root.

```
Project
  └── APIKey (N)
  └── Workflow (N)
        └── WorkflowStep (N, ordered by step_index)
              └── WorkflowRun (N)
                    └── StepRun (N, one per step + attempt)
```

Auth resolves `Bearer wf_<prefix>.<secret>` → `project_id`, injected into the request context. All queries filter by project_id.

---

## Domain Model

### Project
Tenant root. Has a unique `slug` and one or more API keys.

### APIKey
Auth credential. The full key is shown once on creation and never stored — only the bcrypt hash and a lookup prefix are persisted.

**Key format:** `wf_<12-char-base64url-prefix>.<43-char-base64url-secret>`

### Workflow
A blueprint: an ordered list of steps. Immutable after creation (runs reference steps by ID).

### WorkflowStep
One node in a workflow blueprint. Fields:
- `step_index` — zero-based execution order
- `step_type` — executor discriminator (`delay`, `http_request`)
- `config` — JSONB payload, step-type specific

### WorkflowRun
One execution instance of a Workflow. Lifecycle:
```
pending → running → succeeded
                 └→ failed
```

### StepRun
One attempt at executing a WorkflowStep within a run. Supports retries via `attempt` counter.

Lifecycle:
```
pending → running → succeeded
                 └→ failed (retried → pending again if within max_attempts)
```

The constraint `UNIQUE (workflow_run_id, workflow_step_id, attempt)` makes retries idempotent at the DB level.

---

## Executor Flow

The executor is an in-process poll loop (1s interval). Both `cmd/api` and `cmd/worker` can run it.

```
loop every 1s:
  1. GetNextPendingStepRun
     → pending, next_run_at <= now()
     → no other step in the same run is currently running
     → all steps with lower step_index have succeeded
     → oldest eligible first

  2. Interpolate config
     → replace {{steps.N.output.path}} with outputs from succeeded steps

  3. MarkStepRunRunning
     → if workflow_run is still pending → MarkWorkflowRunRunning

  4. executeStep (delay | http_request)

  5a. Success → MarkStepRunSucceeded
      → if all steps succeeded → MarkWorkflowRunSucceeded

  5b. Failure → check retry policy
      → if attempts remain and error is retriable → RetryStepRun (backoff)
      → otherwise → MarkStepRunFailed + MarkWorkflowRunFailed
```

### Retry Policy

Configured per-step inside `config.retry`:

```json
{
  "retry": {
    "max_attempts": 3,
    "backoff_seconds": 5
  }
}
```

Delay formula: `base * 2^(attempt-1)` with ±20% jitter. Max cap: 24h.

Non-retriable errors: `context.Canceled`, HTTP 4xx (except 429), unknown step type, invalid config.

### Output Chaining

Step configs can reference outputs from earlier steps:

```
{{steps.0.output.body.id}}
{{steps.1.output.status_code}}
```

The interpolator replaces these before the step executes. Missing paths resolve to an empty string.

---

## Database Schema

### Key Design Decisions

- **`step_runs.attempt`** — retry audit trail; each retry is a separate row with incremented attempt. The UNIQUE constraint prevents duplicate execution.
- **`step_runs.next_run_at`** — backoff scheduling. NULL = run immediately; future timestamp = not before.
- **`workflow_steps.step_index`** — strict ordering. The executor enforces sequential execution by checking all lower-index steps are succeeded.
- **`api_keys.key_prefix`** — enables O(1) key lookup without full table scan. Only the prefix hits the DB; bcrypt verification happens in-process.

### Migrations

Located in `internal/infrastructure/db/migrations/`. Paired `.up.sql` / `.down.sql`. Applied with `migrate-up.sh` (idempotent via `IF NOT EXISTS`).

| # | Migration |
|---|-----------|
| 000001 | `projects`, `api_keys` |
| 000002 | `workflows`, `workflow_steps` |
| 000003 | `workflow_runs`, `step_runs` |
| 000004 | Unique index on `api_keys.key_prefix` |
| 000005 | `step_runs.next_run_at` (retry backoff scheduling) |
| 000006 | `step_runs.output` baseline (safe re-run) |
| 000007 | `projects.external_tenant_id` (Kafka trigger tenant mapping) |
| 000008 | `event_triggers` (event-type → workflow mapping) |
| 000009 | `processed_integration_events` (Kafka idempotency guard) |

---

## Auth Flow

```
Client                          API
  │                              │
  │  Authorization: Bearer wf_   │
  │  <prefix>.<secret>           │
  │──────────────────────────────▶│
  │                              │ 1. Extract prefix from token
  │                              │ 2. SELECT key_hash, project_id
  │                              │    FROM api_keys
  │                              │    WHERE key_prefix = $prefix
  │                              │    AND revoked_at IS NULL
  │                              │ 3. bcrypt.Compare(hash, fullKey)
  │                              │ 4. Inject project_id into ctx
  │                              │
  │◀──────────────────────────────│
```

Swagger UI accepts the bare key (`wf_xxx.yyy`) without `Bearer ` prefix — the middleware handles both formats.

---

## Binaries

### `cmd/api`
HTTP API server + embedded in-process executor. Suitable for local dev and single-node deployments.

### `cmd/worker`
Standalone worker — only runs the executor loop, no HTTP. For horizontal scaling: run multiple workers against the same database.
