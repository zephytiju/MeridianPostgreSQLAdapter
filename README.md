<!-- SPDX-License-Identifier: Apache-2.0 -->

# Meridian PostgreSQL/PostGIS Adapter

`meridian-storage-postgresql` is the Meridian V1 adapter for PostgreSQL and
PostGIS. It implements the released Core Adapter SPI and Query translation
contract while keeping SQL, topology, credentials, connection pools, and
PostgreSQL-specific settings behind the adapter boundary.

The V1 package provides:

- relational, JSON document, authoritative key/value, and evidence-table
  storage using one physical table per logical Resource;
- tenant and scope injection into every read, write, primary key, and
  applicable index;
- conditional mutations, atomic claims with `FOR UPDATE SKIP LOCKED`, and one
  PostgreSQL `READ COMMITTED` transaction per Meridian transaction;
- joins, aggregates, signed live-keyset pagination, and WGS84 distance filters
  evaluated by PostGIS geography in meters;
- bounded recursive relation traversal over the Registry-resolved Relation
  Collection set—never database metadata or a universal edge table;
- deterministic migration plans, advisory locking, logical export/import,
  authenticated health probes, and read-only physical-fingerprint checks;
- local single-primary and primary-plus-two-standby conformance profiles.

## Installation

```bash
python -m pip install meridian-storage-postgresql
```

Core discovers the adapter through the `meridian_storage.adapters` entry-point
group under the stable id `postgresql`. Application code continues to use
mapping-first Expressions and serialized Operations; it does not import the
adapter, psycopg, or SQL builders.

## Structured put modes (2.0.0)

The released Structured Catalog defaults to `mode="if_absent"`: an existing
scoped identity raises `ConflictError`. Use `mode="update"` for an existing
record or explicit `mode="upsert"` to create or update. New PostgreSQL rows
start at `recordVersion == 1`.

```python
structured = meridian.catalog("structured")
created = meridian.execute(structured.put(resource=resource, data=data)).data
updated = meridian.execute(structured.put(
    resource=resource, data=changed_data, mode="update",
    expected_version=created["recordVersion"],
)).data
meridian.execute(structured.put(resource=resource, data=data, mode="upsert"))
```

A non-null expected version is invalid with `if_absent`, including zero.
For update/upsert, a supplied version requires an existing matching row; zero
never creates. Every path retains tenant and configured scope isolation.

This is a breaking change from implicit upsert. Migrate intentional updates
and upserts explicitly, refresh the Binding capability fingerprint, and use
the compatible package pins below. The adapter rejects old put Operations,
missing normalized modes, unsupported versions, and query plans in put input.
Other Operation versions and the Adapter SPI are unchanged.

Existing field semantics remain: unconditional updates/upserts assign all
mutable non-identity fields, including NULL for omitted nullable fields;
version-checked puts update the supplied mutable fields. Immutable-only
unconditional puts preserve their existing values and version. Return shape,
initial version, update increments, and conflict codes remain unchanged.
Core's existing bounded in-process idempotency recognizes same-key replay and
returns the original result; a distinct equal-data create conflicts. This
release adds no replay store or restart-durable replay guarantee for put.

## Binding settings

Platform/Vangu IaC renders `meridian.postgresql.settings.v1` into a Meridian
Binding. A condensed example is shown below; fingerprints must be canonical
`sha256:` values and real deployments pin every declared Resource.

```json
{
  "formatVersion": "meridian.postgresql.settings.v1",
  "scopeKeys": ["workspace"],
  "topology": {"expectedStandbys": 0},
  "resources": [
    {
      "ref": "structured:example.people",
      "table": "people",
      "profile": "relational",
      "schemaFingerprint": "sha256:<64 lowercase hex characters>",
      "resourceFingerprint": "sha256:<64 lowercase hex characters>",
      "fields": [
        {
          "name": "id",
          "column": "id",
          "logicalType": "uuid",
          "cardinality": "one",
          "nullable": false,
          "mutable": false
        }
      ],
      "identity": ["id"],
      "indexes": [],
      "relation": null
    }
  ]
}
```

The default profile is `postgresql-postgis-local-single-primary` with zero
standbys. The cluster profile is `postgresql-postgis-cluster` and requires at
least two streaming standbys. The adapter validates those properties; it does
not provision, promote, fail over, back up, or restore instances.

## Migrations and recovery boundary

Startup opens the pool and performs authenticated/read-only verification only.
It never acquires a migration lock and never executes DDL. Platform migration
jobs explicitly invoke `SchemaCompiler` and `MigrationExecutor`; the executor
uses a transaction-scoped advisory lock, compares the expected physical
fingerprint, and records a distinct deterministic plan fingerprint before
applying DDL. V1 supports initial creation and compatible additive nullable
columns. Destructive or otherwise incompatible evolution fails closed for a
new design and migration job rather than being inferred by the adapter.

`LogicalTransfer` provides bounded logical JSON-lines export/import. Physical
backup status, backup creation, restore, promotion, identities, ACLs, and
lifecycle remain Platform IaC authority. `RecoveryHook` describes that handoff
and intentionally has no administrative side effects.

The package also exposes a released `SemanticsAdapter` facade for schema
validation, activation planning/apply, Registry revision access, canonical
encoding/decoding, and logical transfer. It does not add public engine concepts
or bypass the Core SPI.

## Durable projection outbox (2.1.0)

Deployment composition can inject `PostgreSQLOutbox` into the released
`ProjectionRunner`. It implements the four owner-based `OutboxPort` methods
without changing the Core Adapter SPI, Writer, or Runner signatures. Business
code continues to call `TransactionalOutboxWriter` through Meridian.

```python
# Deployment/host composition; create_context is the existing AdapterCreateContext
# resolved by the platform from a pinned Binding and secret references.
from meridian_storage.adapters.postgresql import PostgreSQLAdapterFactory, PostgreSQLOutbox
from meridian_storage.projection import ProjectionRunner

adapter = PostgreSQLAdapterFactory().create(create_context)
adapter.open()
try:
    adapter.probe()  # read-only schema, capability and privilege checks
    outbox = PostgreSQLOutbox(
        adapter,
        resource="platform.outbox",
        spec=spec,                 # the same released ProjectionSpec as the runner
        context=worker_context,    # OperationContext: tenant and exact Binding scopes
        poison_threshold=5,
        max_batch_size=1000,
    )
    runner = ProjectionRunner(
        meridian=meridian, spec=spec, project=project,
        outbox=outbox, batch_size=100, lease_seconds=60,
    )
    with meridian.context(worker_context):
        runner.run_until_stopped(stop_event)
finally:
    adapter.close()
```

The host owns startup, termination, credentials, authorization, retries and
scheduling. No SQL or driver client is supplied to application projectors.
The injected adapter runtime must contain both the source and outbox Resource
mappings; target writes use the runner's Meridian facade and may resolve to a
different Binding. Source and intent commit through the Writer's single source
Binding transaction. Cross-Binding atomicity is not claimed.

The outbox Resource is a Structured relational Resource with identity `eventId`.
Its immutable Schema and Binding layout contain every `OutboxDataV1.to_mapping()`
field, each with `mutable=false` and scalar cardinality:

| Logical fields | Logical type | Nullable |
| --- | --- | --- |
| formatVersion, eventId, sourceCatalog, sourceResource, sourceSchema, mutationKind, digest | string | false |
| sourceIdentity, sourceVersion, payload, immutableReference | json | true |
| targetLabels, operationContext | json | false |
| occurredAt | utcTimestamp | false |

JSON identity/version columns preserve integer, string and null distinctions.
The layout uses safe adapter-owned physical column names (for example lowercase
logical names). `payload` and `immutableReference` are mutually exclusive per
the released intent contract. Use only the released Writer to append required
intent; insert-only duplicate conflicts leave both original intent and durable
progress unchanged. Layout fields cannot be mutable.

An explicit `SchemaCompiler` / `MigrationExecutor` deployment migration creates
`__meridian_outbox_state` and `__meridian_outbox_checkpoint` when an outbox layout
is configured. Apply the new plan and refresh the physical fingerprint before
startup. Existing databases with no outbox layout keep their previous DDL.
Runtime probes and provider construction only read and validate the metadata
shape, unique keys and privileges; they never create or repair tables. Grant the
runtime SELECT/INSERT/UPDATE on these metadata tables through IaC. Declare
scope-leading source/identity/version and occurrence/event indexes on large
outbox Resources through the normal Schema and Binding index configuration.

State and checkpoint keys include tenant, all configured scopes, outbox
Resource and projection name. Changing filters for the same projection retains
its leases and checkpoints. A different projection name consumes independently.
Claims filter the exact source Catalog, Resource, Schema and required target
labels. Only the earliest incomplete version for each source identity is
eligible. Numeric versions order numerically; opaque versions use occurrence
and event-id order. Independent identities may run concurrently. Quarantine
blocks later work for that identity and is never skipped automatically.

Claims lock a bounded batch with `FOR UPDATE SKIP LOCKED` and persist owner,
acquisition, expiry and attempt before returning. Concurrent activity can produce
a short batch; subsequent calls retain the remaining work. Expired claims become eligible
for reclaim. Release persists retry/quarantine with a redacted failure category;
exception messages and payloads are not stored. Completion checks exact source
version and current non-expired ownership, then advances the checkpoint by CAS
and marks completion in one transaction. Expiry during checkpoint persistence
rolls back both effects. Database time is used unless the caller explicitly
supplies the port's deterministic UTC `now` parameter.

The existing owner-only protocol **does not fence claim generations**. Hosts
must avoid overlapping reuse of an owner string. A target acknowledgement may
be replayed after a host crash; the projector/target must implement idempotent,
version-aware writes. `get(event_id)` and `checkpoint(partition_key)` are
adapter-owned deployment diagnostics, not new OutboxPort requirements.

## Development

```bash
uv sync --extra test
uv run ruff check .
uv run mypy
uv run pytest
uv build
uv run twine check dist/*
```

Genuine integration tests cover `postgis/postgis:16-3.4-alpine` and
`postgis/postgis:17-3.5-alpine`; they are skipped unless
`MERIDIAN_POSTGRESQL_TEST_DSN` is set. See
[`docs/conformance.md`](docs/conformance.md) for exact commands and evidence.

## Compatibility

Version 2.1.0 pins Core 1.0.1, Semantics 2.0.0, Query 1.0.2, and Projection 1.0.2.
The Adapter SPI remains 1.0.0; `structured.put` uses Operation contract 2.0.0. The locked design revisions and supported
PostgreSQL/PostGIS profiles are recorded in the wheel's `compatibility.json`.
Native PostgreSQL queries are intentionally excluded from V1.

## License

Apache License 2.0. See [`LICENSE`](LICENSE) and [`NOTICE`](NOTICE).
