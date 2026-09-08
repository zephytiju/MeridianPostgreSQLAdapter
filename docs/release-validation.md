<!-- SPDX-License-Identifier: Apache-2.0 -->

# Deployment-selected releases and authenticated contracts

The deployment chooses and locks server images and Python distributions
independently. `compatibility.json` and `ENGINE_VERSIONS` record historical
conformance recipes. They do not promise arbitrary future compatibility and
are not release membership predicates. Unlisted inputs can be constructed;
only real-engine acceptance establishes a verified combination.

## Complete adapter gate inventory

| Entry point / gate | Classification | Behavior |
| --- | --- | --- |
| Descriptor `supportedEngineVersions` values, compatibility recipe | Release metadata | Preserved for canonical V1 hashes and reproducible historical recipes. No membership check. |
| Factory / settings profile keys | Contract identity | Only local single-primary and cluster profiles; topology rules unchanged. |
| Factory Adapter id/SPI version | Contract | Exact PostgreSQL Adapter SPI 1.0.0 remains mandatory. |
| Factory dependency package pins | Deployment integrity | Optional selected locks for Core, Semantics, Query, Projection and this adapter compare to installed distribution metadata, not a compiled recipe. Unknown installed releases are not thereby verified. Core retains its own non-package compatibility keys. |
| Project dependency requirements | Package/API compatibility | Core 1.1 is required for the additive observed-release SPI and manifest repair. Compatible major ranges replace this package's exact historical recipe. The lockfile records the tested resolved build. |
| Factory capability fingerprint | Content integrity | Compare the canonical manifest for the deployment-selected profile/version with its supplied fingerprint. Never synthesize a compiled-release selection. |
| Probe protocol / extension / functions | Real contract/features | Wire protocol 3, installed PostGIS, authenticated geography distance, JSONB mutation, recursive traversal and migration-lock hashing execute read-only before readiness. Missing behavior fails specifically. |
| Probe numeric `engineVersion` (`16-postgis-3.4`, or exact patch versions) | Legacy deployment integrity | Preserve declared numeric PostgreSQL/PostGIS component expectations, comparing whole numeric components. A nonnumeric image label/digest carries selection only. |
| Probe observed release | Authenticated provenance | PostgreSQL `server_version` and `PostGIS_Lib_Version()` supply the separate `AdapterProbe.observed_engine_version`; never derive observation from selection. |
| Core `observedEngineVersion` pin | Deployment integrity | Core's public runtime/conformance runner compares an optional exact observed-release expectation with the new SPI observation. |
| Authentication / TLS / privileges | Security | Driver authentication, verified TLS/CA, CONNECT/schema USAGE and Resource privileges remain mandatory. |
| Primary / isolation / topology | Real runtime contract | Writable primary, READ COMMITTED, rollback-only probe and declared standby minimum remain enforced. |
| Physical / migration / durable outbox metadata | Schema and integrity | Fingerprints, columns, keys, migrations and durable provider checks remain fail closed. Runtime startup performs no DDL. |
| Operations / Evidence / transactions | Semantic contract | Existing put modes, same-Binding session ownership, exact required guarantees, atomic Evidence and source/outbox commit/rollback remain enforced. |
| Export/import, recovery hooks | Ownership and contract | Continue existing adapter APIs; provisioning, promotion and backup/restore execution remain IaC-owned. |

Managed and external Constructs both provide the same resolved runtime Binding.
The adapter has no provisioning-mode switch or provider of its own. Both
advertised topology profiles use the same factory, probe, schema and execution
checks; no helper retains a separate release gate.

## Provenance and migration

No descriptor/manifest/settings V1 wire fields or canonical hashing algorithm
change. Existing four historical manifest fingerprints remain byte-for-byte
stable. `engineVersion` in probe evidence remains a legacy selection field;
`selectedEngineVersion` labels it explicitly. `postgresqlVersion`,
`postgisVersion`, `observedEngineVersion` and the typed SPI observation are
independently authenticated server results. Nonnumeric selections can use
Core's exact `observedEngineVersion` expectation when release drift must fail.
A changed selected version still changes the manifest fingerprint and requires
an explicit deployment config update.

Full package hash verification and image digest verification belong to the
owning deployment installer/build. The factory's package-version comparisons
are supplemental drift checks, not a replacement for a locked installation.
Core's legacy `coreVersion=1.0.0` denotes its SPI and is never treated as a
Python package release. No dependency override or ignored `pip check` failure
qualifies as release acceptance.

## Verification

CI pins the exact PostgreSQL/PostGIS images for 16.4/3.4.3 and 17.11/3.5.7.
The latter uses the previously unlisted exact `17.11-postgis-3.5.7` selection
while the historical descriptor table stays unchanged. Each runs local and
primary-plus-two-streaming-standby acceptance. The cluster runner also executes
the full integration suite, including public Evidence, durable outbox, process
restart, exact acknowledgement, retry/quarantine and source/intent rollback.
The built wheel runs all integration tests in a separate installed environment.

`record_conformance.py` rejects missing/failed/skipped test evidence and records
selected images/versions, Docker registry digests, authenticated PostgreSQL and
PostGIS observations, installed package versions, candidate artifact hashes,
and JUnit counts. CI retains these JSON records with JUnit and coverage.
A standalone wheel interpreter can be passed as `MERIDIAN_POSTGRESQL_PYTHON`
to the same disposable cluster runner. This changes only the test interpreter,
not the required acceptance cases.

The wider release-closure conformance must additionally preserve immutable
Usage/Cost and projection latest/tombstone and both host-drain outcomes using
public owning-package artifacts. Adapter unit metadata tests do not establish
these cross-package results. Final delivery evidence must identify the exact
public release closure and its executed checks; an unresolved dependency
closure prevents completion.
