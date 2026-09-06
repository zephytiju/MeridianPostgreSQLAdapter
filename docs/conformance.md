<!-- SPDX-License-Identifier: Apache-2.0 -->

# Deterministic conformance

The default test suite separates pure/unit contract tests from genuine engine
tests. No PostgreSQL behavior is accepted from mocks alone.

```bash
uv sync --extra test
uv run pytest tests/unit tests/contract tests/packaging
```

For PostgreSQL 16 + PostGIS 3.4:

```bash
docker run --rm --name meridian-postgis \
  -e POSTGRES_PASSWORD=meridian \
  -e POSTGRES_USER=meridian \
  -e POSTGRES_DB=meridian \
  -p 55432:5432 postgis/postgis:16-3.4-alpine

MERIDIAN_POSTGRESQL_TEST_DSN='postgresql://meridian:meridian@127.0.0.1:55432/meridian' \
  uv run pytest -m integration
```

For PostgreSQL 17 + PostGIS 3.5, use image
`postgis/postgis:17-3.5-alpine` and set
`MERIDIAN_POSTGRESQL_ENGINE_VERSION=17-postgis-3.5`. CI runs the complete
single-primary suite against both advertised version pins.

The integration suite applies the adapter-generated migration plan through the
explicit migration hook and then verifies types, JSON, CAS races, transactional
rollback, atomic claims, WGS84 boundary distances, live keysets, bounded
traversal, logical transfer, advisory-lock idempotence, probes, and physical
fingerprints.

The same suite exercises the released Semantics facade, including activation
plan/apply, Registry revision, canonical encoding/decoding, logical transfer,
and an additive nullable-column upgrade without dropping existing data.

Cluster tests are opt-in because they require one primary and at least two
streaming standbys. The repository includes a disposable genuine-cluster runner:

```bash
./scripts/run-cluster-conformance.sh
```

The CI cluster matrix runs that script with both advertised images. A local
version override uses `MERIDIAN_POSTGRESQL_IMAGE` together with the matching
`MERIDIAN_POSTGRESQL_ENGINE_VERSION`.

Alternatively, set `MERIDIAN_POSTGRESQL_CLUSTER_DSN` to the write endpoint and
`MERIDIAN_POSTGRESQL_CLUSTER_STANDBY_DSNS` to two comma-separated read endpoints,
then run `pytest -m cluster`. The suite applies the migration on the primary,
waits for both standbys to replay it, verifies the physical fingerprint, rejects
a standby as a write endpoint, and fails closed below the configured replica
minimum. Promotion, endpoint switching, backup creation, and restore remain
Platform IaC tests; the adapter exposes no authority to perform them.

Release builds pin the build backend and set a fixed `SOURCE_DATE_EPOCH`.
Independent isolated builds of the same revision must therefore produce
byte-identical wheel and sdist files; `twine check --strict` validates both.

## Structured put contract 2.0.0

`contracts/conformance/structured-put.v2.json` is copied byte-for-byte from
Semantics 2.0.0's published sdist. `provenance.json` records both artifact and
fixture digests. Contract tests validate all 11 normalized/fingerprint cases,
25 invalid expressions, capability negotiation, and single-statement SQL.

`tests/integration/test_put_modes.py` runs the 18 portable existence cases
through the installed Structured Catalog and Core public runtime on real
PostgreSQL. It also covers concurrent create winners, unconditional/CAS upsert
races, tenant plus two scope dimensions, existing field/version behavior,
immutable-only rows, original-result replay after a later mutation, mode-key
conflict, and unsupported contract rejection before mutation. The suite uses
a disposable migration-owned namespace and requires authenticated physical
fingerprint verification before Core startup.

## Durable outbox

`tests/integration/test_durable_outbox.py` imports the executable shared
`run_outbox_conformance` fixtures from the exact PyPI release
`meridian-storage-projection==1.0.2`. It supplies public Structured intent
seeding, durable inspection and fresh-runtime reopen callbacks. The portable
suite covers owner/expiry rejection, exact acknowledgements, checkpoint
revision, retry/quarantine, redaction, ordering and same-owner characterization.

Adapter-owned tests add four concurrent claimers, competing completions,
tenant/scope/Resource/projection isolation, numeric version ordering, real
Writer rollback and immutable duplicates, and Runner target replay. Separate
host processes exit abruptly after claim or target acknowledgement. A database
fault after checkpoint persistence proves atomic rollback of completion and
progress; a delayed checkpoint write proves lease expiry is checked at the
final transition. Uncommitted source/intent never appears to a claimant.
Missing metadata, changed columns and missing unique keys fail read-only
startup validation. No generation-token fencing is claimed.

The PostgreSQL 16/17 CI jobs run this suite both with the project environment
and with a clean installed candidate wheel. The subprocess crash host uses
that same installed interpreter; it never imports sibling repositories.
