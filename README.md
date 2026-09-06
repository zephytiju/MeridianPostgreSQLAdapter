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

Version 2.0.0 pins Core 1.0.1, Semantics 2.0.0, and Query 1.0.2.
The Adapter SPI remains 1.0.0; `structured.put` uses Operation contract 2.0.0. The locked design revisions and supported
PostgreSQL/PostGIS profiles are recorded in the wheel's `compatibility.json`.
Native PostgreSQL queries are intentionally excluded from V1.

## License

Apache License 2.0. See [`LICENSE`](LICENSE) and [`NOTICE`](NOTICE).
