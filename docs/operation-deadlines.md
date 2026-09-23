# Bounded PostgreSQL transaction I/O (offline owner repair)

The runtime's pool creates DeadlineConnection instances. A session captures one
OperationBudget, intersects the inherited context, and retains it across pool
acquisition, BEGIN, every execute/cursor/savepoint wait, COMMIT and rollback.
Acquisition uses bounded slices for cancellation and never exceeds the earlier
acquire policy limit. Pool checkout has no unbounded query health-check callback.
The first budgeted BEGIN/execute detects a broken connection. Pool startup,
probes, schema discovery and provisioning are outside this protected-operation
contract; the runtime must already be started before M4 admission.

DeadlineConnection.wait drives psycopg's nonblocking libpq generator through a
selector with min(remaining, 50ms) waits. It checks the budget before dispatch and
before resuming each readiness event; cursor/savepoint methods use the same wait
hook. On failure it finishes the local transport without psycopg's additional
cancel-and-drain wait. No timeout worker continues an operation after return.
The existing pool's connection-maintenance workers do not receive transaction
commands. Expired cleanup discards instead of dispatching ROLLBACK. Returning a
closed connection cannot trigger pool rollback I/O. Normal rollback is itself
budgeted; a failed commit is never followed by an invented successful rollback.

A dropped transport after COMMIT cannot prove non-occurrence. An INERROR
transaction is discarded before commit to avoid mistaking PostgreSQL's implicit
ROLLBACK acknowledgement for a successful commit. Late positive acknowledgement
retains known-committed storage state while raising a nonretryable outcome error.
See Core's operation-deadlines contract for domain reconciliation rules.

This is cooperative synchronous nonblocking I/O control, not a hard realtime
scheduler or a CPU preemption mechanism. A suspended process can resume late;
late acknowledgement remains explicit. CPU compilation/decoding and local lock
scheduling cannot be preempted; budget is checked before further I/O/effects.
No server-side zero-effect-after-revocation or cross-system fence is claimed.

Qualified offline dependency recipe: psycopg/psycopg-binary 3.3.5 and psycopg-pool
3.3.1. Other supported-version ranges are not native qualification evidence.
Native transport behavior, TLS, actual backend rollback/commit durability and
pool replacement remain NOT RUN; follow the delivery's separate native plan.
