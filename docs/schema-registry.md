# Durable Schema metadata

PostgreSQL 2.3.0 implements the public Semantics 2.1 `SchemaRepository` contract.
Deployment supplies `PostgreSQLSchemaRepository` to `SchemaAPI` or
`SemanticsSchemaProvider`; consumers use their engine-neutral APIs. The repository
accepts an explicit connection factory, physical namespace, `OperationContext`
and a nonempty supported Catalog subset. This release supports `structured`
metadata only. Empty, duplicate, unknown and unsupported Catalogs fail closed.
Credentials, connection lifetime and tenant authorization remain deployment-owned.
The repository requires a tenant and partitions by the entire supplied scope.

`SchemaAPI.publish` persists the complete canonical `SchemaDocument`, including
namespaced JSON Schema extensions, its fingerprint, publication/deprecation
timestamps, explicit namespace/name boundaries and registry revision. A storage
envelope checksum also covers those address fields; it is distinct from the
public registry snapshot fingerprint. One checksummed JSONB row per tenant/scope
makes snapshots atomic. Writers lock that row; concurrent identical publication
has one original result, while changed same-version content conflicts. Expected
revision pins are checked before replay. Schema versions increase by semantic
version order; breaking changes require explicit `allow_breaking`.

`SchemaAPI.read(..., version=..., expected_fingerprint=...)` requires the exact
version and fingerprint. Reads reconstruct and verify every stored envelope and
the registry fingerprint. Missing migration, physical drift and corrupt data
fail closed. There is no implicit memory repository. A provider bundle
fingerprint, a Core SchemaDefinition fingerprint, the inner Semantics
SchemaDocument fingerprint and an individual ResourceDefinition fingerprint
identify different objects; do not compare them for equality.

## Deployment and migration

PostgreSQL 2.3.1 exposes the deployment settings parser through the public package
surface. Given a public Core `BindingConfig` loaded from released
Constructs-generated configuration, Platform can compile and apply its plan
without importing private Adapter modules:

```python
from meridian_storage.adapters.postgresql import (
    MigrationExecutor, PostgreSQLSettings, SchemaCompiler,
)

settings = PostgreSQLSettings.from_binding(binding)
plan = SchemaCompiler(settings).compile()
with migration_connections() as connection:
    evidence = MigrationExecutor(settings).apply(connection, plan)
```

Deployment then injects a repository into the engine-neutral Schema API. Its
connection factory supplies a context manager for an authoritative PostgreSQL
connection; these credentials and SQL-driver details stay in deployment wiring:

```python
from meridian_storage.adapters.postgresql import PostgreSQLSchemaRepository
from meridian_storage.semantics import SchemaAPI, SemanticsSchemaProvider

repository = PostgreSQLSchemaRepository(
    connection_factory=runtime_connections,
    physical_namespace=binding.physical_namespace,
    context=authorized_operation_context,
    catalogs=("structured",),
)
schemas = SchemaAPI(repository)
live_provider = SemanticsSchemaProvider(repository)
```

For an injected repository, Platform runs the public
`migrate_schema_repository(connection, physical_namespace=...)` hook once with
migration credentials. `verify_schema_repository` is its read-only readiness
check. Both hooks return/verify an immutable migration fingerprint. Migration
and publication join an enclosing transaction; returned writes remain
provisional until its commit. Replaying a migration verifies the existing shape
and marker. A failed initial migration rolls back atomically.

For `structured.publish_schema` through Core, the deployment-generated Binding
must pin the bootstrap `structured:meridian.registry` Resource with this
adapter-owned layout:

| Setting | Value |
| --- | --- |
| profile | `metadata-registry` |
| table | `__meridian_schema_registry` |
| fields / identity / indexes | empty arrays |
| relation | null |
| schemaFingerprint | bootstrap Core `registry_metadata` SchemaDefinition fingerprint |
| resourceFingerprint | individual structured registry Resource fingerprint |

`SchemaCompiler` includes metadata storage only when this explicit layout is
present. Platform applies that plan using `MigrationExecutor` before runtime
startup. Existing layouts keep their physical plan fingerprints. Regenerate
capability pins for the new durable publication guarantees. Runtime readiness
and physical verification inspect the marker, columns and primary keys without
DDL. Publication uses the same Adapter session transaction as its enclosing
Core transaction. Other data operations on this metadata Resource are rejected;
reads use the injected `SchemaAPI`. `create_resource` remains migration-only.
Business consumers never select an Engine or reach into an Adapter's private
connection methods. Generate runtime configuration through released Constructs.

The runtime writer needs schema USAGE, SELECT on
`__meridian_schema_registry_migration`, and SELECT/INSERT/UPDATE on
`__meridian_schema_registry`. A reader needs only USAGE and SELECT on both tables.
Neither needs CREATE, ALTER, DROP or DELETE. Application access must remain
behind the deployment's authorized context boundary; raw database credentials
are trusted infrastructure access, not tenant credentials. Statements and lock
waits are bounded by the repository timeout (default 30 seconds) and the supplied
context deadline. Core dispatch preserves the Binding's smaller timeout.

## Rollback and operational limits

The migration adds two tables and does not alter existing collection layouts or
activate a newly published Schema. Metadata publication executes DML only.
Roll back application code by stopping metadata writers and restoring the prior
package/configuration pins; retain these tables and their committed data.
Do not run destructive reverse DDL during ordinary rollback. Re-enable metadata
on the compatible release after the read-only migration check. Use Platform
database backup/restore for the registry, including its migration marker and
scope rows. Generic collection `LogicalTransfer` rejects this metadata Resource.
Schema snapshots can be inspected through the public repository; replay into a
new registry preserves Schema bytes but intentionally creates new publication
timestamps/revisions, so it is not a replacement for exact database restore.

Publication reads and rewrites the scope's full registry snapshot. Cost is
proportional to its Schema history, and writers within one tenant/scope serialize.
This is a metadata registry, not a high-volume record store; measure deployment
history sizes and set timeouts accordingly. No schema/history deletion API is
provided. Registry fingerprints detect accidental or partial corruption; a
database administrator with write access can replace both data and checksums.

## Validation

The real PostgreSQL integration suite covers immutable replay, concurrent
conflicts/CAS, exact/stale pins, numeric version order, explicit breaking changes,
deprecation, tenant/scope separation, rollback/savepoints, missing migration,
physical drift, stored corruption, fresh processes and roles without DDL or
DELETE privileges. Adapter SPI tests cover Catalog normalization, migration,
readiness, physical pins, same-session rollback and durable dispatch.
`scripts/schema_restart_acceptance.py` writes a complete receipt, then verifies
the same bytes and replay in a new process after the test database restarts.
CI repeats integration and restart checks from an installed wheel on PostgreSQL
16 and 17 and retains the existing cluster conformance gates.
