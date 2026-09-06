# Evidence conformance and upgrade

The PostgreSQL Adapter supports the released Evidence append and query contracts with Core 1.0.1, Semantics 2.0.0, Query 1.0.2 and Evidence 1.0.1. The package and lock file declare the tested dependency set. Core 1.0.1 is required so its operation replay cache partitions identical keys by the full scope.

## Guarantees and composition

Evidence query advertises the canonical `scope-isolation` guarantee. The compiler requires a tenant and exactly the configured scope keys, injects them as bound predicates, and rejects attempts to reference reserved scope columns as logical fields. Scope-leading keys allow identical logical IDs in different tenants and workspaces. Application authorization remains the caller's responsibility.

Required appends advertise `atomic-evidence` and must execute inside the existing `meridian.transaction(resource)` context. Source and audit Operations must resolve to the same Binding and owner; sharing a PostgreSQL endpoint alone is insufficient. Successful values are provisional until outer commit. Propagate a required append error or call the existing transaction handle's `set_rollback_only()` when catching it. The Adapter does not generate audit records or replay a group of Operations.

Append accepts one mapping or the released nonempty batch of up to 10,000 mappings. All mappings compile before execution, and a batch executes within one transaction/savepoint. Database and result-limit failures roll back the complete append and its replay claim. Results preserve input order; scalar append returns one mapping and batch append returns an ordered sequence. Existing field validation and SQL parameter binding apply to every row.

The operation's Evidence idempotency key, or its OperationContext key, selects a durable replay record. If neither is supplied, the released Evidence record identities (`evidenceId` / `checkpointKey`) provide the replay key when the Catalog marks the operation idempotent. Replay partitions include Binding, principal, tenant, full scope, Resource and Operation contract. Identical replay returns the committed original result; a changed request fingerprint conflicts. Replay storage commits or rolls back with the append, including inside a source/audit transaction, and survives runtime restart. This remains per-Operation replay, with no automatic transaction callback retry.

Live cursors are signed and bound to the original query, Schema, registry, scope and page size. Continuation tokens and the shrinking operation deadline are excluded from the cursor's query fingerprint; filters, projection, Resource, order and other limits are retained. Cursors from the prior implementation should be discarded during rollout. The public Query format and fingerprints are unchanged.

## Explicit migration

Before starting the upgraded runtime, deployment must regenerate capability and physical fingerprints and apply the new `SchemaCompiler` plan through the existing `MigrationExecutor` deployment hook. The plan adds Adapter-owned `__meridian_evidence_replay` storage in the same physical namespace/database. It stores a scope-partitioned key hash, request fingerprint and normalized result under a unique primary key. Runtime requires SELECT, INSERT and UPDATE on this table; startup checks its shape and primary key without creating it. The table name is reserved and cannot be used for a logical Resource.

Do not repin deployment fingerprints without applying and verifying the explicit migration. No startup DDL or production migration is performed by the runtime. Rollback to earlier Adapter code requires the corresponding earlier capability pins; the additive replay table can remain until deployment-owned retention permits removal.

## Evidence

`tests/integration/test_evidence_facade.py` runs installed public Catalogs and Core against real PostgreSQL/PostGIS, covering scoped replay, missing/spoofed context, cursor misuse, source/audit commit and rollback, cross-Binding rejection, batch ordering/failure, concurrent replay, restart, result limits and required migration checks. Test fixture SQL only provisions/inspects disposable deployment state. CI runs it on both supported PostgreSQL/PostGIS versions and repeats it from a built wheel in an isolated environment with normal dependency checks. Existing structured and cluster gates remain required.
