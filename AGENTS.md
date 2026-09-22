# icanbwell - AI Agent Instructions

> **Scope:** Organization-wide baseline. Applies to all repositories in the icanbwell GitHub organization.
> **Owner:** Enterprise Architecture (@icanbwell/enterprise-architecture)
> **Precedence:** This file sets the floor. Repo-level instruction files (copilot-instructions.md, CLAUDE.md) may add stricter requirements or repo-specific context but must not weaken or contradict these directives. If there is a conflict, this baseline wins. Repo-level overrides may only tighten rules, never loosen them. Any true exception requires EA approval with documented rationale, scope, owner, JIRA ticket, and expiry date.
>
> **Context-budget note:** This file loads in full on every session, in every repo. Keep it to hard, always-applicable constraints. File-type-specific detail (OOAD/SOLID, testing, event contracts, operational patterns) lives in `.claude/rules/*.md` and loads only when Claude touches a matching file. Vendor/pattern-specific guidance lives in `.claude/skills/*` and loads only when invoked. Don't re-add prose here that a rule or skill already covers.

---

## Platform Identity

b.well is a cloud-native, multi-tenant, microservice-based, event-driven healthcare data platform under HIPAA, FHIR-native, exposed via a federated GraphQL gateway. Services are independently deployable, communicate asynchronously by default, and own their private datastores. The FHIR server is the system-of-record, accessed only via approved APIs and contracts. Cross-service workflows use sagas and event choreography, never distributed transactions or synchronous orchestration chains.

Code that violates tenant isolation, leaks PHI, bypasses the gateway, introduces unapproved technology, or creates tight coupling between services is incorrect regardless of whether it compiles and passes tests.

---

## Design-Time Quality Kit & Knowledge Substrate

The worst failures happen at **design time**, before code review can catch them. b.well maintains a tool-neutral **knowledge substrate** — the single source of truth for rules, patterns, and gradeable review rubrics — and a design-time kit that uses it. Reach for these instead of re-deriving (or reinventing) an approach:

- **Authoring a design?** Use the **`tech-design`** skill — it walks you through the rubric so the design passes EA review the first time.
- **Reviewing a design?** Use **`/tech-design-review`** — it grades a TDD/FDR against the rubric and returns concrete, cited gaps.
- **Rubrics** (`rubrics/`) — what "good" means, gradeably: `tech-design-rubric.md`, `fhir-feasibility-rubric.md` (conformance + IG conformance + resource-explosion feasibility), `api-design-rubric.md`.
- **Patterns** (`patterns/`) — named, blessed shapes to appeal to by name, not reinvent: `orchestrated-long-running-work`, `temporal-coalescing`, `event-key-and-partition-design`.
- **Decision guides** (`decision-guides/`) — e.g. `datastore-selection.md` (including *do not put run/FSM state on a FHIR `Task`*).
- **Standards** (`standards/`) — canonical rules, e.g. `events.md` (Kafka/event conventions; supersedes the inline `patient.updated`-style examples elsewhere in this file).
- **Reference architectures** (`reference-architectures/`) — annotated real exemplars (the DEQM orchestrator; a good API/SDK).

Overview + the stable-anchor citation convention: `docs/knowledge-substrate.md`. Cite substrate content by anchor (e.g. `standards/events.md#std-events-partition-key`), never by line number.

---

## Hard Non-Negotiables

- **Event-driven first.** Default to async via Kafka + CloudEvents. A sync service-to-service call needs a documented reason (immediate response required, data can't be pre-materialized). See the `sync-to-async` skill.
- **Choreography over orchestration.** Services react to events; no cross-domain god orchestrator, no request-reply chains disguised as async. Saga detail (compensation, idempotency, ordering): `.claude/rules/event-contracts.md`.
- **Service data ownership.** Never read from or write to another service's private datastore. Consume owned data via events or the owning service's public API.
- **Tenant isolation.** Mandatory on every persistence model and query path — correctness, not best-effort. If you can't confirm tenant filtering on a new data access path, flag it.
- **FHIR-native modeling.** Use standard FHIR resources before inventing schemas. New resource usage or structural changes need an FDR. Extensions are a last resort requiring review. See the `fhir-design` skill.
- **Federated gateway only.** Client-facing capabilities go through the federated graph — no point-to-point service APIs bypassing it. Public API/schema changes are additive-only and need a Tech Design Review. See the `api-design-guardian` skill.
- **No unapproved technology.** Check `policies/approved-tech.yaml` before adding any datastore, cache, queue, search engine, observability sink, vendor, or significant library. Not listed → Tech Design Review. Infra changes go through Terraform PRs, never manual console steps.
- **Eventually consistent by default.** Don't design cross-service reads/UX assuming immediate consistency. Strong consistency belongs inside a single bounded context.

Detailed Kafka usage patterns, schema evolution rules, and forward-compatibility requirements: `.claude/rules/event-contracts.md`.

---

## Code & Design Conventions

OOAD, SOLID, hexagonal-boundary, DRY/idempotency/minimal-diff, and modern-idiom (Java/Python/TypeScript) conventions apply to source files and load automatically via `.claude/rules/ooad-solid.md`.

Testing discipline (parameterized tests, AAA, mock-only-at-boundaries, contract tests, tenant-isolation-in-integration-tests) loads automatically for test files via `.claude/rules/testing.md`.

Timeouts/retries, circuit breakers, N+1/full-scan awareness, and observability requirements load automatically for service source files via `.claude/rules/operational.md`.

Follow whatever linter/formatter/static-analysis config exists in the repo — these rules cover architectural judgment linters can't catch, not style.

---

## Security

**PHI/PII:** Never in logs, test fixtures, example payloads, comments, commit messages, PR descriptions, or screenshots. Use synthetic/redacted data.

**Auth:** OAuth/OIDC only. No custom auth schemes, no hardcoded credentials, tokens, or secrets anywhere.

---

## Governing Processes

- **New technology, vendor, or pattern; public API changes; cross-team impact:** Tech Design Review with EA (JIRA ticket, type "Tech Design Review", linked design doc).
- **FHIR data modeling decisions:** FDR process — FDR Confluence page + FHIR SME approval.
- **Non-trivial repo-local implementation decisions** (library choice, caching strategy, new pattern in the repo): ADR in the repo's `adrs/` directory, MADR format (https://adr.github.io/madr/). Show the options considered, not just the conclusion.

Unsure if a change needs review? "Is it NEW?" — new technology, new vendor, or a pattern not previously used in this codebase → needs EA review.

---

## Agent Behavior

- **Plan before acting.** Before non-trivial changes, restate which constraints apply (tenancy, PHI, contracts, dependencies) and flag if it touches tenant isolation, PHI, public contracts, event schemas, or a new dependency.
- **Diagnose before escalating.** Read logs and check the obvious (right token/var, remote configured, did it actually fail vs. warn) before concluding something needs org-admin intervention or new infrastructure.
- **Respect system constraints.** If branch protection blocks a merge, CI is failing, or a process needs approval, don't repeatedly offer workarounds. If told an action is blocked, don't re-propose it with different wording.
- **Don't guess commands.** Use the repo's actual build/test/lint commands (Makefile, package.json, build.gradle, Pipfile). Say so if unclear.
- **Don't introduce dependencies casually.** Check `approved-tech.yaml` first; popularity isn't approval.
- **Reference governing artifacts.** Cite the Tech Design Doc/FDR/ADR/AsyncAPI spec in the PR when a change touches a public API, event contract, or cross-service behavior.
- **Flag what looks wrong.** Missing tenant filtering, PHI in fixtures, a vendor name baked into business logic, a sync call where an event belongs — flag it, don't silently work around it.
- **Code ownership.** No "Co-Authored-By", "Generated by", or other AI attribution in commits, PRs, or comments. The author on the commit is the owner.

---

## Branch Naming

Format: `XX-PROJ-123` (initials-JIRA project-ticket number), e.g. `WF-EA-2136`. No `feature/`, `fix/`, `bugfix/`, `hotfix/` prefixes — the JIRA key carries the context.

## Commit Messages

Every commit starts with a JIRA issue key, no conventional-commit prefixes: `EA-789 add FHIR resource validation`. Exceptions: automated dependency bumps (`Bump version`, `build(deps): ...`) and git operations (`Merge`, `Revert`, `Reapply`). Commits are validated automatically. No ticket for your work? Create one before committing.

---

## icanbwell Infrastructure

Atlassian: https://icanbwell.atlassian.net/ · Slack workspace: icanbwell · Common JIRA projects: EA, HP, PAY, RNGR.

Use the `atlassian:*` skills (ticket creation/triage, status reports, Confluence search) and `slack:*` skills (search, digests, announcements) for these operations rather than calling MCP tools ad hoc — they already encode the correct cloudId lookup, project conventions, and troubleshooting steps.

---

## Code Style

Follow whatever linter, formatter, and static analysis configuration exists in the repo. Don't duplicate what automated tooling already enforces — these instructions are for architectural and design decisions linters can't catch.