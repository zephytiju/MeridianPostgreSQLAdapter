# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from dataclasses import replace
from unittest.mock import Mock

import pytest
from conftest import fp, read_compatible_binding
from test_operation_and_dml import PEOPLE, compiler, context, operation, request

from meridian_storage.adapters.postgresql._settings import PostgreSQLSettings, ReadCompatibility
from meridian_storage.adapters.postgresql.migration import LogicalTransfer, MigrationExecutor
from meridian_storage.adapters.postgresql.query.dml import DMLCompiler
from meridian_storage.adapters.postgresql.schema import SchemaCompiler
from meridian_storage.adapters.postgresql.semantics import PostgreSQLSemanticsAdapter


def mapping():
    # BindingConfig serializes frozen mappings/tuples back to public JSON.
    return read_compatible_binding().to_dict()["settings"]


def settings(raw=None):
    binding = read_compatible_binding()
    return PostgreSQLSettings.from_binding(
        binding if raw is None else replace(binding, settings=raw)
    )


def test_exact_proof_is_immutable_and_uses_core_fingerprints():
    value = settings()
    proof = value.read_compatibility[PEOPLE.canonical]
    assert value.read_only
    assert proof.reader_resource.fingerprint == value.layout(PEOPLE).resource_fingerprint
    assert proof.stored_resource.fingerprint != proof.reader_resource.fingerprint
    with pytest.raises(TypeError):
        value.read_compatibility["other"] = proof
    with pytest.raises(ValueError, match="read-only"):
        value.require_writable()


@pytest.mark.parametrize(
    "field,value",
    [
        ("ref", {"catalog": "structured", "namespace": "other", "name": "people"}),
        ("profile", "document"),
        ("schema", None),
        ("labels", {"different": "value"}),
        ("requiredScope", []),
        ("relatedResources", [{"catalog": "structured", "namespace": "example", "name": "other"}]),
        ("extensions", {"different": True}),
    ],
)
def test_non_put_definition_differences_fail(field, value):
    raw = mapping()
    raw["readCompatibility"]["resources"][0]["readerResource"][field] = value
    with pytest.raises(ValueError):
        settings(raw)


@pytest.mark.parametrize(
    "target,field,value",
    [
        ("storedResource", "operationVersion", "2.0.0"),
        ("readerResource", "operationVersion", "3.0.0"),
        ("readerResource", "guarantees", ["atomic"]),
        ("readerResource", "minimumLimits", {"rows": 10}),
    ],
)
def test_only_exact_put_version_transition_is_supported(target, field, value):
    raw = mapping()
    definition = raw["readCompatibility"]["resources"][0][target]
    next(r for r in definition["requirements"] if r["operationContract"].endswith(".put"))[
        field
    ] = value
    with pytest.raises(ValueError):
        settings(raw)


@pytest.mark.parametrize(
    "change", ["empty", "duplicate", "unknown", "version", "layout", "scope", "read", "canonical"]
)
def test_closed_coverage_layout_and_read_contract_pins(change):
    raw = mapping()
    proof = raw["readCompatibility"]
    if change == "empty":
        proof["resources"] = []
    elif change == "duplicate":
        proof["resources"] *= 2
    elif change == "unknown":
        proof["skipChecks"] = True
    elif change == "version":
        proof["formatVersion"] = "unknown"
    elif change == "layout":
        raw["resources"][0]["resourceFingerprint"] = fp("wrong")
    elif change == "scope":
        raw["scopeKeys"] = []
    elif change == "read":
        proof["resources"][0]["readerResource"]["requirements"][0]["operationVersion"] = "2.0.0"
    elif change == "canonical":
        proof["resources"][0]["storedResource"]["formatVersion"] = "unknown"
    with pytest.raises(ValueError):
        settings(raw)


def test_required_physical_fingerprint_cannot_be_omitted():
    with pytest.raises(ValueError, match="requiredPhysicalFingerprint"):
        PostgreSQLSettings.from_binding(
            replace(read_compatible_binding(), required_physical_fingerprint=None)
        )


def test_json_boolean_and_number_are_not_an_exact_definition_match():
    proof = mapping()["readCompatibility"]["resources"][0]
    proof["storedResource"]["extensions"] = {"example": 1}
    proof["readerResource"]["extensions"] = {"example": True}
    with pytest.raises(ValueError, match="permits only"):
        ReadCompatibility.from_mapping(proof, "test")


@pytest.mark.parametrize(
    "method", ["put", "patch", "delete", "append", "publish_schema", "create_resource"]
)
@pytest.mark.parametrize("declared_read_only", [True, False])
def test_mutations_rejected_even_with_false_read_only_claim(method, declared_read_only):
    value = replace(operation(method), read_only=declared_read_only)
    with pytest.raises(ValueError, match="read-only"):
        compiler(settings()).compile(request(value))


def test_reads_keep_scope_validation_and_write_flag_fails():
    selected = compiler(settings())
    get = operation("get", input_value={"where": {"name": "Ada"}})
    command = selected.compile(request(get))
    assert command.statement.parameters[:2] == ("tenant-a", "workspace-a")
    with pytest.raises(ValueError, match="read-only"):
        selected.compile(request(replace(get, read_only=False)))
    with pytest.raises(ValueError, match="scope"):
        selected.compile(request(get, operation_context=replace(context(), scope={})))


def test_control_plane_and_direct_dml_cannot_write_or_connect():
    selected = settings()
    connection = Mock()
    provider = Mock()
    semantics = PostgreSQLSemanticsAdapter(selected, provider)
    calls = [
        lambda: SchemaCompiler(selected).compile(),
        lambda: MigrationExecutor(selected).apply(connection, Mock()),
        lambda: LogicalTransfer(selected).import_rows(connection, [], tenant="t", scope={}),
        lambda: semantics.import_logical({}),
        lambda: semantics.apply_activation(Mock()),
        lambda: DMLCompiler(selected).compile("put", PEOPLE, {}, context()),
        lambda: DMLCompiler(selected).atomic_claim(
            PEOPLE, where={}, changes={}, limit=1, context=context()
        ),
    ]
    for call in calls:
        with pytest.raises(ValueError, match="read-only"):
            call()
    assert connection.mock_calls == []
    provider.assert_not_called()


def test_ordinary_binding_preserves_migration_and_write_behavior():
    raw = mapping()
    del raw["readCompatibility"]
    value = settings(raw)
    assert not value.read_only
    value.require_writable()
    assert SchemaCompiler(value).compile().statements
