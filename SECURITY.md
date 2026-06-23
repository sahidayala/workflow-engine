# Security Policy

## Supported Versions

The project is in early-stage alpha. Only the latest commit on `main` receives
security fixes.

| Version | Supported |
| ------- | --------- |
| main    | Yes       |

## Reporting a Vulnerability

**Do not open a public GitHub issue for security vulnerabilities.**

Please report security issues by emailing the maintainers directly. Include:

- A description of the vulnerability and its potential impact
- Steps to reproduce or a proof-of-concept
- Any suggested mitigations you have in mind

You will receive an acknowledgement within 48 hours and a resolution timeline
within 7 days. We will coordinate a disclosure date with you before publishing
any fix.

## Security Model

### API Key Authentication

API keys use the format `wf_<prefix>.<secret>` where:

- The **prefix** (9 random bytes, base64url) is stored in plaintext for fast
  lookup.
- The **secret** (32 random bytes, base64url) is hashed with bcrypt (cost 12)
  and never stored in plaintext.
- The full key is returned **once** at creation time and cannot be retrieved
  again.

### Multi-Tenancy

Every resource (workflow, run, step) is scoped to a `project_id` extracted
from the authenticated API key. Cross-tenant data access is prevented at the
query level — all SQL queries include a `project_id` predicate.

### Dependency Updates

Dependencies are managed with Go modules. Run `go list -m -u all` periodically
to identify outdated packages. Apply security patches promptly.

### Known Limitations

- TLS termination is expected to happen at the load-balancer / reverse-proxy
  layer. The server itself does not terminate TLS.
- Rate limiting is not yet implemented in the API server.
- No audit log for administrative actions (planned for a future milestone).
