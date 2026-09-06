# SPDX-License-Identifier: Apache-2.0
"""Immutable Meridian V1 capability descriptors."""

from __future__ import annotations

from meridian_storage.query.adapter import QueryCapabilities
from meridian_storage.spi.capabilities import (
    AdapterDescriptor,
    CapabilityManifest,
    OperationCapability,
)

ADAPTER_ID = "postgresql"
ADAPTER_CONTRACT_VERSION = "1.0.0"
ENGINE_VERSIONS = {
    "postgresql-postgis-local-single-primary": ("16-postgis-3.4", "17-postgis-3.5"),
    "postgresql-postgis-cluster": ("16-postgis-3.4", "17-postgis-3.5"),
}

_METHODS = (
    "aggregate",
    "create_resource",
    "delete",
    "get",
    "patch",
    "publish_schema",
    "put",
    "query",
    "search",
    "traverse",
)


def _operation_capability(method: str) -> OperationCapability:
    guarantees = [
        "bound-parameters",
        "scope-injected",
        "single-binding",
        "strong-consistency",
    ]
    if method in {"delete", "patch", "put"}:
        guarantees.extend(("conditional-mutation", "read-committed"))
    if method == "traverse":
        guarantees.extend(("bounded-traversal", "relation-collections"))
    if method in {"create_resource", "publish_schema"}:
        guarantees.append("external-migration")
    return OperationCapability(
        operation_contract=f"meridian.structured.{method}",
        operation_versions=("2.0.0",) if method == "put" else ("1.0.0",),
        guarantees=tuple(guarantees),
        limits={
            "maxPageSize": 500,
            "maxRelationResources": 32,
            "maxTraversalDepth": 8,
            "maxMembershipNames": 10_000,
            "pageSize": 500,
        },
        cursor_behavior="signed-live-keyset" if method in {"query", "search"} else "none",
        migration_behavior="platform-iac-hook",
        health_probes=("authenticated", "physical", "readiness"),
        extensions={
            "org.meridian.postgresql/transaction": "read-committed",
            "org.meridian.postgresql/distance": "wgs84-geography-meters",
        },
    )


DESCRIPTOR = AdapterDescriptor(
    adapter_id=ADAPTER_ID,
    adapter_contract_version=ADAPTER_CONTRACT_VERSION,
    driver="psycopg-3",
    supported_engine_versions=ENGINE_VERSIONS,
    capabilities=(
        *(_operation_capability(method) for method in _METHODS),
        OperationCapability(
            operation_contract="meridian.evidence.append",
            operation_versions=("1.0.0",),
            guarantees=(
                "append-only",
                "atomic-evidence",
                "bound-parameters",
                "read-committed",
                "scope-injected",
                "scope-isolation",
                "transactional-with-structured",
            ),
            limits={"maxPageSize": 500},
            migration_behavior="platform-iac-hook",
            health_probes=("authenticated", "physical", "readiness"),
        ),
        OperationCapability(
            operation_contract="meridian.evidence.query",
            operation_versions=("1.0.0",),
            guarantees=(
                "bound-parameters",
                "scope-injected",
                "scope-isolation",
                "strong-consistency",
            ),
            limits={"maxPageSize": 500},
            cursor_behavior="signed-live-keyset",
            migration_behavior="platform-iac-hook",
            health_probes=("authenticated", "physical", "readiness"),
        ),
        OperationCapability(
            operation_contract="meridian.transaction",
            operation_versions=("1.0.0",),
            guarantees=("atomic", "no-dirty-reads", "read-committed"),
            limits={"maxOperations": 10_000},
            migration_behavior="external",
            health_probes=("authenticated", "readiness"),
        ),
    ),
)

QUERY_CAPABILITIES = QueryCapabilities(
    adapter_id=ADAPTER_ID,
    operations=(
        "aggregate",
        "get",
        "scan",
        "search",
        "traverse",
    ),
    native_semantics=("*",),
    operators=(
        "add",
        "aggregate.avg",
        "aggregate.count",
        "aggregate.max",
        "aggregate.min",
        "aggregate.sum",
        "and",
        "contains",
        "distance",
        "distanceWithin",
        "divide",
        "documentPath",
        "eq",
        "field",
        "fullText",
        "get",
        "gt",
        "gte",
        "in",
        "isNull",
        "literal",
        "lt",
        "lte",
        "modulo",
        "multiply",
        "ne",
        "negate",
        "not",
        "notIn",
        "or",
        "order",
        "point",
        "prefix",
        "scan",
        "search",
        "subtract",
        "timestampRange",
        "traverse",
    ),
    logical_types=(
        "boolean",
        "bytes",
        "date",
        "decimal",
        "duration",
        "enum",
        "float64",
        "int8",
        "int16",
        "int32",
        "int64",
        "json",
        "objectRef",
        "recordRef",
        "string",
        "utcTimestamp",
        "uuid",
        "wgs84Point",
    ),
    consistency_classes=("strong", "session"),
    guarantees=("single-binding", "stable-order", "scope-injected", "bound-parameters"),
    features=(
        "all-neighbors",
        "crs:EPSG:4326",
        "direction:any",
        "direction:inbound",
        "direction:outbound",
        "distance-unit:m",
        "explicit-relations",
        "live-keyset",
        "result:paths",
        "result:records",
        "result:relations",
        "simple-path",
    ),
    limits={
        "deadlineMs": 3_600_000,
        "facetBuckets": 0,
        "membershipNames": 10_000,
        "pageSize": 500,
        "relationResources": 32,
        "resultBytes": 16 * 1024 * 1024,
        "resultValues": 100_000,
        "returnedPaths": 10_000,
        "traversalDepth": 8,
        "visitedValues": 100_000,
    },
)


def manifest(engine_profile: str, engine_version: str) -> CapabilityManifest:
    return CapabilityManifest(
        descriptor=DESCRIPTOR,
        engine_profile=engine_profile,
        engine_version=engine_version,
        extensions={
            "org.meridian.postgresql/queryCapabilityFingerprint": QUERY_CAPABILITIES.fingerprint,
            "org.meridian.postgresql/postgisRequired": True,
        },
    )


__all__ = [
    "ADAPTER_CONTRACT_VERSION",
    "ADAPTER_ID",
    "DESCRIPTOR",
    "ENGINE_VERSIONS",
    "QUERY_CAPABILITIES",
    "manifest",
]
