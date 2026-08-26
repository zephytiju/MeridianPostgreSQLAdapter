<!-- SPDX-License-Identifier: Apache-2.0 -->

# Architecture and authority boundaries

The adapter consumes the released Core, Semantics, and Query distributions. It
does not import sibling source trees. Consumers submit mapping-first Meridian
Expressions which Catalog providers normalize to serialized Operations. Core
selects the Binding and invokes the adapter SPI.

Internally, the adapter resolves a pinned Resource layout, injects tenant/scope,
compiles a typed SQL AST, and sends only bound parameters through psycopg. Query
results are normalized back to JSON-compatible Meridian data before leaving the
SPI. A signed live-keyset cursor binds plan, Schema, Registry, scope, order, and
page size.

The public facade also implements the released Semantics contract. Activation
planning separates the target physical schema fingerprint from the migration
plan fingerprint: the former describes the verified database shape, while the
latter identifies the exact ordered transition. Initial creation and additive
nullable columns are deterministic V1 transitions; incompatible changes are
rejected before DDL.

Each Relation Collection owns its own table. Its released relation profile
determines generated endpoint columns and indexes. Traversal receives a closed,
sorted Relation Collection set from the Registry and constructs a bounded
`UNION ALL` edge stream plus a recursive simple-path CTE. The adapter never
discovers relations from PostgreSQL metadata and never creates a universal edge
table.

Platform/Vangu IaC owns Engine selection, endpoint/service resolution,
provisioning, state, identities and ACLs, migration job invocation, backups,
restore, promotion, and lifecycle. The adapter validates those settings and
offers migration/transfer/probe hooks; those hooks do not confer administrative
authority.
