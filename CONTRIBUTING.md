# Contributing

Thank you for considering a contribution to the Workflow Engine. This document
covers everything you need to know to get a change merged.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [How to Contribute](#how-to-contribute)
- [Development Workflow](#development-workflow)
- [Commit Style](#commit-style)
- [Pull Request Checklist](#pull-request-checklist)
- [Architecture Notes](#architecture-notes)

---

## Code of Conduct

Be respectful. Disagreements about technical decisions are expected and
welcome — personal attacks are not. We follow the standard open-source
community norms.

---

## Getting Started

1. **Fork** the repository and clone your fork.
2. Follow the [Quick Start](README.md#quick-start) to get a working local
   environment.
3. Run `go test ./...` — all tests must pass before you start.

---

## How to Contribute

### Bug Reports

Open a GitHub issue with:

- A clear title and description of the problem
- Steps to reproduce (minimal script or curl commands)
- Expected vs. actual behavior
- Go version and OS

### Feature Requests

Open a GitHub issue first to discuss the design before writing code. This avoids
wasted effort if the feature does not fit the current roadmap.

### Code Contributions

Small, focused pull requests are preferred over large refactors bundled with
feature work.

---

## Development Workflow

```bash
# 1. Set up the database (once)
make up       # starts postgres via docker compose
make migrate  # applies migrations

# 2. Run the server
make run

# 3. Run the worker in a separate terminal
make run-worker

# 4. Make your changes, then verify
make vet      # go vet
make test     # go test ./...
```

### Regenerating the OpenAPI spec

If you add or modify HTTP handlers, regenerate the spec:

```bash
make swagger
```

This requires `swag` to be installed:

```bash
go install github.com/swaggo/swag/cmd/swag@latest
```

---

## Commit Style

Use [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<scope>): <short description>

[optional body]
```

Common types: `feat`, `fix`, `docs`, `refactor`, `test`, `chore`.

Examples:

```
feat(executor): add script step type
fix(retry): cap exponential backoff at 24h
docs(readme): fix quick start instructions
test(executor): add retry jitter unit tests
```

---

## Pull Request Checklist

Before opening a PR, confirm:

- [ ] `make vet` passes with no warnings
- [ ] `make test` passes (add tests for new behaviour)
- [ ] `make tidy-check` passes (go.mod / go.sum are clean)
- [ ] New environment variables are documented in `.env.example` and `README.md`
- [ ] New SQL migrations are paired (`.up.sql` + `.down.sql`)
- [ ] New HTTP endpoints have Swagger annotations
- [ ] No secrets, credentials, or `.env` files are committed

---

## Architecture Notes

The codebase follows **Hexagonal Architecture (Ports & Adapters)**:

```
cmd/              entrypoints — wire everything together
internal/
  domain/         core types; no external dependencies
  app/            use-cases, service logic, executor
  auth/           API key generation and middleware
  interfaces/     HTTP adapter (handlers, router)
  infrastructure/ PostgreSQL adapter (repositories, migrations)
```

**Key invariants to preserve:**

- The `domain` package must never import from `infrastructure` or `interfaces`.
- All SQL queries must be parameterised — no string-interpolated queries.
- Every new step type needs a corresponding executor case in
  `internal/app/executor/executor.go`.
- Retry classification belongs in `isNonRetriable` — 4xx client errors (except
  429) are not retried.
- The `FOR UPDATE SKIP LOCKED` query in `GetNextPendingStepRun` is the
  concurrency safety mechanism — do not remove it.

### Adding a New Step Type

1. Add a case to `executeStep` in `internal/app/executor/executor.go`.
2. Add a `run<Type>` function in a new file `internal/app/executor/<type>.go`.
3. Register the type name in Swagger annotations on `CreateWorkflowStepRequest`.
4. Add a test for the happy path and at least one error case.
