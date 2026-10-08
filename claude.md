# CLAUDE.md — ReschedulingEngine (Team Aurora, DMG)

Project instructions for AI coding agents (Claude Code, Cursor). Read this file
fully before any task. It is reviewed in PRs like code; when the AI repeats a
mistake, add a rule here.

---

## 1. What this service does
- Handles **customer-initiated rescheduling of jobs** at DMG.
- It sits on the **Zero Touch critical path**: every failed or duplicate
  reschedule turns into a manual touch (someone calls a customer or provider).
  Correctness beats speed of delivery here.

## 2. Architecture (target state)
```
POST /reschedule  /  POST /reschedule/batch
        │
        ▼
  API layer (thin: validate, authorise, map request)
        │
        ▼
  RescheduleService  ── orchestrates one job through its state machine
        │
        ├─ EligibilityPolicy      (one class per rule; Strategy pattern)
        ├─ SchedulerGateway       (V2 adapter; V1 adapter until retired)
        ├─ RescheduleRepository   (Postgres: reschedule_events + operations)
        └─ EventPublisher         (writes to outbox; relay publishes to Kafka)
                                        │
                                        ▼
                     Kafka `job.rescheduled` → NotificationService
```
- **Storage:** PostgreSQL. `reschedule_events` is the audit log.
  MongoDB is **not** approved; do not add MongoDB code.
- **Flags:** Statsig. Separate flags for batch and single-job paths.
- **Batch is asynchronous:** accept → return `202` + `batch_id` → workers
  process jobs with bounded concurrency → client polls per-job status.

### Job state machine
`ACCEPTED → ELIGIBLE → SCHEDULED → PUBLISHED`
or `→ FAILED(reason)` / `→ UNKNOWN(needs reconcile)` from any step.

## 3. Known issues — read before touching related code
| Ticket | Issue | What it means for your change |
|---|---|---|
| AURORA-1247 | `job.rescheduled` double-fires (last Sev-3) | Any code that publishes must be idempotent and dedupe-safe. Root cause not yet confirmed. |
| AURORA-1102 | ~15% of traffic still goes through OldSchedulerV1 via the legacy adapter | Never call either scheduler directly; go through `SchedulerGateway`. Both adapters must pass the same contract tests. |
| AURORA-1298 | Batch endpoint (PR #1842) needs rework | See section 7 for what the first version got wrong. |
| — | Idempotency was assumed to be "handled by the scheduler"; never verified | Do not rely on the scheduler for idempotency. We own it. |
| — | p99 latency is 920ms (SLO 800ms); EligibilityDSL alert fires often | Likely slow part is eligibility, not storage. Measure with traces before optimising. |
| — | Success rate 99.2% (SLO 99.5%) | Error budget is spent. Reliability work takes priority over new features. |

## 4. Code structure rules (SOLID, applied practically)
- **Single Responsibility:** the API layer does not call the database, scheduler
  or Kafka. `RescheduleService` coordinates; each dependency does one job.
- **Open/Closed:** a new eligibility rule is a **new** `EligibilityPolicy` class,
  never an edit inside an existing rule.
- **Liskov Substitution:** V1 and V2 scheduler adapters must behave identically
  for the same input — same results, same errors, same idempotency. Shared
  contract tests enforce this.
- **Interface Segregation:** small interfaces (gateway, repository, publisher).
  No "god client".
- **Dependency Inversion:** depend on interfaces, inject them. **Never create
  clients at import time** (e.g. `scheduler = NewSchedulerV2Client()` at module
  level).
- No abstraction without a second real use. No deep class hierarchies.

## 5. Correctness rules (non-negotiable)
1. **Idempotency:** every request carries an idempotency key; every job gets an
   operation ID. A repeated request returns the stored result; it does not
   reschedule or publish again.
2. **Eligibility:** every job — single or batch — passes `EligibilityPolicy`
   before calling the scheduler. Never skip it.
3. **Authorisation:** verify the caller owns every job in the request.
4. **Outbox:** never call Kafka directly. Write the state change and the event in
   **one Postgres transaction**; the relay publishes. Note: the outbox does not
   make the scheduler call transactional — idempotency and reconciliation are
   still required.
5. **Audit:** every reschedule (single or batch) writes to `reschedule_events`.
6. **Timeouts:** every gRPC call has a deadline.
7. **Errors — three different cases:**
   - Scheduler rejected → `FAILED(reason)`, do not retry.
   - Scheduler unavailable (transient) → retry with backoff and jitter, limited.
   - Timeout → `UNKNOWN`: the work may have happened. **Reconcile against the
     scheduler's state before any retry.** Never retry blindly.
8. **Partial failure:** one job failing never stops the batch. Every job ends
   with a status and a reason the client can see.
9. **Limits:** enforce the max batch size (1000) in the request model, not in a
   comment. Rate-limit calls to SchedulerV2.
10. **Events:** include `event_id`, `operation_id`, `batch_id`, `job_id`,
    old and new schedule, `schema_version`. Partition key = `job_id`.
11. Use a circuit breaker on the scheduler and a dead-letter queue for events
    that fail to publish.

## 6. Testing rules
- Unit tests use fakes for all interfaces — no live scheduler, DB or Kafka.
- Every behaviour change needs tests for the failure paths, not just success:

| Must test | Expectation |
|---|---|
| Ineligible job | `FAILED(ineligible)`, scheduler never called |
| Duplicate request (same key) | Same result returned, no second reschedule or event |
| Scheduler rejects | `FAILED(reason)`, no retry |
| Scheduler unavailable | Limited retries, then `FAILED` |
| Scheduler timeout | `UNKNOWN`, reconcile path runs |
| Partial batch | Other jobs complete; per-job statuses correct |
| Publish failure | Event stays in outbox; relay retries; no lost event |
| Batch over limit | Rejected with 400 |
| Caller doesn't own job | Rejected, nothing scheduled |
| Flag off | Endpoint not available |

- Contract tests: run against **both** scheduler adapters.
- Assert on statuses and side effects, not just on result counts.

## 7. Review checklist (what PR #1842 missed — check every PR for these)
- [ ] Eligibility checked for every job
- [ ] Idempotency key handled; repeat request is safe
- [ ] Deadline on every remote call; three error cases handled
- [ ] No blocking calls inside `async` routes
- [ ] Events go through the outbox; audit record written
- [ ] Size limit enforced in code; ownership checked
- [ ] No unused queries, undefined names (e.g. `db`), or cross-service table reads
- [ ] Dependencies injected; nothing created at import time
- [ ] Failure-path tests present (section 6)
- [ ] Metrics, logs and trace IDs per job
- [ ] PR links to an **approved** design; description has test and rollout plan

## 8. Observability
- Per job: structured log with `operation_id`, `batch_id`, `job_id`, state, reason.
- Metrics: success rate, duplicate-event rate, p99 (single-job and batch
  separately), batch queue backlog, scheduler error rate by type.
- Alerts on **SLO burn rate**, not on single threshold crossings. Every alert
  must have an owner and a runbook.

## 9. Rollout rules
- Ship behind a flag, bucketed **per customer** (same customer, same path).
- Move 5% → 25% → 100% only when success rate, duplicate rate, p99 and
  backlog stay within targets. Kill criteria are written down before rollout.
- Metrics and logs must exist **before** the flag is turned on.
- Small, frequent deploys. A change that can't be rolled back by a flag needs
  a written rollback plan.

## 10. How to work with me
- **Do without asking:** read code, run tests and linters, write tests, draft code.
- **Ask first:** changes touching more than 2 files (show a plan), schema
  changes, new dependencies, anything in section 5.
- **Challenge me.** If my approach is wrong, say so, explain why, propose an
  alternative. Do not just agree.
- Label review findings **BLOCKER / MAJOR / MINOR**, each with evidence and a fix.
- Report exactly which tests you ran and which you didn't. Never claim a test passed
  without running it.
- If you're unsure about an API or behaviour, say so. Don't guess.

## 11. Commands
> Adjust to the real repo.
- Tests: `make test`
- Lint + types: `make lint`
- Local stack: `make up`
- Run tests and lint before saying a task is done, and paste the result.

## 12. Hard limits (also enforced by a pre-tool hook)
- Never run `./deploy.sh` or any deploy command.
- Never touch production data or run destructive commands (drop, delete, force-push).
- Never read, write or print secrets or API keys. Secrets live in the vault.
  If you see a secret in code or config, stop and flag it.
