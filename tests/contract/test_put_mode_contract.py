# SPDX-License-Identifier: Apache-2.0
"""Pinned mode fixture fidelity, fail-closed negotiation, and atomic SQL shape."""

import hashlib
import json
from pathlib import Path

import pytest
from meridian_storage.registry import CapabilityRequirement
from meridian_storage.semantics import StructuredCatalogProvider
from meridian_storage.semantics.errors import InvalidDefinition
from meridian_storage.spi.capabilities import capability_violations

from meridian_storage import Expression, ResourceRef
from meridian_storage.adapters.postgresql.descriptor import manifest
from meridian_storage.adapters.postgresql.query.dml import DMLCompiler

FIXTURE_PATH = Path(__file__).parents[2] / "contracts/conformance/structured-put.v2.json"
FIXTURE = json.loads(FIXTURE_PATH.read_text())


def test_fixture_is_the_exact_released_contract():
    provenance = json.loads(FIXTURE_PATH.with_name("provenance.json").read_text())
    assert hashlib.sha256(FIXTURE_PATH.read_bytes()).hexdigest() == provenance["fixtureSha256"]
    assert provenance["package"] == "meridian-storage-semantics"
    assert provenance["version"] == "2.0.0"


@pytest.mark.parametrize("case", FIXTURE["valid"], ids=lambda c: c["name"])
def test_released_normalization_and_capability_acceptance(case):
    operation = StructuredCatalogProvider().normalize(Expression.from_mapping(case["expression"]))
    assert operation.to_dict() == case["operation"]
    assert operation.request_fingerprint == case["requestFingerprint"]
    assert not capability_violations(
        manifest("postgresql-postgis-local-single-primary", "16-postgis-3.4"),
        operation.requirements,
    )


@pytest.mark.parametrize("case", FIXTURE["invalid"], ids=lambda c: c["name"])
def test_released_invalid_inputs_fail_closed(case):
    with pytest.raises(InvalidDefinition):
        StructuredCatalogProvider().normalize(Expression.from_mapping(case["expression"]))


@pytest.mark.parametrize("version", ["1.0.0", "3.0.0"])
def test_descriptor_never_advertises_unsupported_put_contracts(version):
    assert capability_violations(
        manifest("postgresql-postgis-local-single-primary", "16-postgis-3.4"),
        (CapabilityRequirement("meridian.structured.put", version),),
    )


@pytest.mark.parametrize(
    "mode,expected,prefix",
    [
        ("if_absent", None, "INSERT"),
        ("update", None, "UPDATE"),
        ("update", 0, "UPDATE"),
        ("upsert", None, "INSERT"),
        ("upsert", 1, "UPDATE"),
    ],
)
def test_put_compiles_to_one_scoped_mutation(settings, operation_context, mode, expected, prefix):
    command = DMLCompiler(settings).compile(
        "put",
        ResourceRef.parse("structured:example.people"),
        {
            "data": {"id": "00000000-0000-0000-0000-000000000001", "name": "bound-value"},
            "mode": mode,
            "expectedVersion": expected,
        },
        operation_context,
    )
    statement = command.statement.command.as_string(None)
    assert statement.startswith(prefix) and "SELECT" not in statement and ";" not in statement
    assert '"__tenant"' in statement and '"__scope_workspace"' in statement
    assert "RETURNING" in statement and "bound-value" not in statement
    assert ("ON CONFLICT" in statement) == (mode == "upsert" and expected is None)
    assert ("t.__record_version = %s" in statement) == (expected is not None)
    assert tuple(
        value for value in command.statement.parameters if value in ("tenant-a", "workspace-a")
    ) == ("tenant-a", "workspace-a")


@pytest.mark.parametrize(
    "input_value",
    [
        {},
        {"mode": None},
        {"mode": []},
        {"mode": "unknown"},
        {"mode": "if_absent", "expectedVersion": 0},
        {"mode": "update", "expectedVersion": True},
    ],
)
def test_adapter_rejects_invalid_mode_inputs_before_sql(settings, operation_context, input_value):
    with pytest.raises((ValueError, TypeError)):
        DMLCompiler(settings).compile(
            "put",
            ResourceRef.parse("structured:example.people"),
            {"data": {"id": "x", "name": "invalid"}, **input_value},
            operation_context,
        )
