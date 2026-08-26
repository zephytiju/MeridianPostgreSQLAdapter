# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import json
from importlib.metadata import entry_points
from pathlib import Path

from meridian_storage.query.ast import Field, distance_within, point
from meridian_storage.query.requirements import infer_requirements
from meridian_storage.query.wire import QueryOperation, QueryTarget
from meridian_storage.registry.resources import ResourceRef

from meridian_storage.adapters.postgresql import PostgreSQLAdapterFactory
from meridian_storage.adapters.postgresql.descriptor import DESCRIPTOR, QUERY_CAPABILITIES, manifest


def test_entry_point_discovers_exact_adapter() -> None:
    selected = entry_points(group="meridian_storage.adapters", name="postgresql")
    assert len(selected) == 1
    factory = next(iter(selected)).load()()
    assert isinstance(factory, PostgreSQLAdapterFactory)
    assert factory.adapter_id == "postgresql"


def test_manifest_is_deterministic_and_closed() -> None:
    first = manifest("postgresql-postgis-local-single-primary", "16-postgis-3.4")
    second = manifest("postgresql-postgis-local-single-primary", "16-postgis-3.4")
    assert first.fingerprint == second.fingerprint
    assert first.descriptor.fingerprint == DESCRIPTOR.fingerprint
    contracts = {item.operation_contract for item in DESCRIPTOR.capabilities}
    assert "meridian.transaction" in contracts
    assert "meridian.evidence.append" in contracts
    assert all("native" not in item for item in contracts)


def test_query_capability_satisfies_distance_requirement() -> None:
    operation = QueryOperation(
        catalog="structured",
        targets=(QueryTarget(ResourceRef.parse("structured:example.people")),),
        operation="scan",
        filter=distance_within(Field("location"), point(0, 0), 100),
    )
    requirements = infer_requirements(operation)
    failures = [
        reason
        for requirement in requirements.requirements
        for supported, reason in [
            QUERY_CAPABILITIES.supports(requirement, operation=operation.operation)
        ]
        if not supported
    ]
    assert failures == []


def test_checked_in_manifest_fingerprints_match_executable_contracts() -> None:
    root = Path(__file__).resolve().parents[2]
    ledger = json.loads(
        (root / "contracts/adapter-capability/fingerprints.v1.json").read_text()
    )
    assert ledger["adapterDescriptorFingerprint"] == DESCRIPTOR.fingerprint
    assert ledger["queryCapabilityFingerprint"] == QUERY_CAPABILITIES.fingerprint
    assert ledger["manifests"] == [
        {
            "engineProfile": profile,
            "engineVersion": version,
            "fingerprint": manifest(profile, version).fingerprint,
        }
        for profile, versions in DESCRIPTOR.supported_engine_versions.items()
        for version in versions
    ]
