# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace

import pytest
from conftest import make_binding, sample_settings_mapping
from meridian_storage.errors import CompatibilityError
from meridian_storage.spi.adapters import AdapterCreateContext, SecretValue

from meridian_storage.adapters.postgresql import PostgreSQLAdapterFactory
from meridian_storage.adapters.postgresql._settings import PostgreSQLSettings
from meridian_storage.adapters.postgresql.schema import SchemaCompiler


def test_settings_are_closed_and_cluster_requires_two_standbys() -> None:
    raw = sample_settings_mapping(expected_standbys=1)
    binding = SimpleNamespace(
        engine_profile="postgresql-postgis-cluster",
        settings=raw,
        physical_namespace="meridian_test",
        tls=SimpleNamespace(mode="server"),
    )
    with pytest.raises(ValueError, match="at least two"):
        PostgreSQLSettings.from_binding(binding)
    raw["unknown"] = True
    binding.engine_profile = "postgresql-postgis-local-single-primary"
    with pytest.raises(ValueError, match="unknown"):
        PostgreSQLSettings.from_binding(binding)


def test_schema_compilation_is_deterministic_and_relation_local(
    settings: PostgreSQLSettings,
) -> None:
    first = SchemaCompiler(settings).compile()
    second = SchemaCompiler(settings).compile()
    assert first.plan_fingerprint == second.plan_fingerprint
    assert first.physical_fingerprint == second.physical_fingerprint
    sql_text = "\n".join(statement.command.as_string(None) for statement in first.statements)
    assert "CREATE EXTENSION IF NOT EXISTS postgis" in sql_text
    assert "ALTER TABLE" in sql_text and "ADD COLUMN IF NOT EXISTS" in sql_text
    assert '"__tenant", "__scope_workspace", "id"' in sql_text
    assert '"friendships"' in sql_text
    assert '"__source_collection"' in sql_text
    assert '"friendships__source_endpoint"' in sql_text
    assert "universal" not in sql_text.casefold()
    assert 'CREATE TABLE IF NOT EXISTS "meridian_test"."friendships"' in sql_text

    changed = sample_settings_mapping()
    changed["resources"][0]["resourceFingerprint"] = (
        "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
    )
    changed_settings = PostgreSQLSettings.from_binding(
        SimpleNamespace(
            engine_profile="postgresql-postgis-local-single-primary",
            settings=changed,
            physical_namespace="meridian_test",
            tls=SimpleNamespace(mode="disabled"),
        )
    )
    changed_plan = SchemaCompiler(changed_settings).compile()
    assert changed_plan.physical_fingerprint == first.physical_fingerprint
    assert changed_plan.plan_fingerprint != first.plan_fingerprint


def test_resource_layout_rejects_duplicate_physical_tables() -> None:
    raw = sample_settings_mapping()
    resources = raw["resources"]
    assert isinstance(resources, list)
    resources[1]["table"] = resources[0]["table"]
    binding = SimpleNamespace(
        engine_profile="postgresql-postgis-local-single-primary",
        settings=raw,
        physical_namespace="meridian_test",
        tls=SimpleNamespace(mode="disabled"),
    )
    with pytest.raises(ValueError, match="unique physical tables"):
        PostgreSQLSettings.from_binding(binding)


def test_impossible_physical_layouts_fail_at_binding_parse() -> None:
    baseline = sample_settings_mapping()
    cases: list[tuple[dict[str, object], str]] = []

    unknown_type = deepcopy(baseline)
    unknown_type["resources"][0]["fields"][0]["logicalType"] = "unknown"
    cases.append((unknown_type, "logicalType"))

    incomplete_decimal = deepcopy(baseline)
    del incomplete_decimal["resources"][0]["fields"][5]["scale"]
    cases.append((incomplete_decimal, "precision and scale"))

    reserved_column = deepcopy(baseline)
    reserved_column["resources"][0]["fields"][0]["column"] = "__tenant"
    cases.append((reserved_column, "Adapter-owned column"))

    nullable_identity = deepcopy(baseline)
    nullable_identity["resources"][0]["fields"][0]["nullable"] = True
    cases.append((nullable_identity, "immutable non-null scalars"))

    invalid_unique = deepcopy(baseline)
    invalid_unique["resources"][0]["indexes"][0].update({"kind": "hash", "unique": True})
    cases.append((invalid_unique, "only for btree"))

    missing_relation = deepcopy(baseline)
    missing_relation["resources"][0]["profile"] = "relation"
    cases.append((missing_relation, "must agree"))

    for settings_value, message in cases:
        binding = SimpleNamespace(
            engine_profile="postgresql-postgis-local-single-primary",
            settings=settings_value,
            physical_namespace="meridian_test",
            tls=SimpleNamespace(mode="disabled"),
        )
        with pytest.raises(ValueError, match=message):
            PostgreSQLSettings.from_binding(binding)


def test_factory_rejects_unadvertised_engine_version() -> None:
    binding, user, password = make_binding("postgresql://localhost/meridian")
    binding = replace(binding, engine_version="18-postgis-3.6")
    context = AdapterCreateContext(
        binding=binding,
        identity=SecretValue(user.encode()),
        credential=SecretValue(password.encode()),
    )
    with pytest.raises(CompatibilityError, match="unsupported PostgreSQL/PostGIS Engine version"):
        PostgreSQLAdapterFactory().create(context)
