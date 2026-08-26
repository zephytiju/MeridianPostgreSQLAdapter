# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import hashlib
import os
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from typing import Any

import pytest
from meridian_storage.context import OperationContext
from meridian_storage.runtime.config import (
    BindingConfig,
    ClientPolicy,
    SecretReference,
    TLSPolicy,
)
from meridian_storage.semantics import SchemaDocument
from meridian_storage.spi.adapters import AdapterCreateContext, PhysicalResource, SecretValue
from psycopg import connect
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from meridian_storage.adapters.postgresql._settings import PostgreSQLSettings
from meridian_storage.adapters.postgresql.descriptor import manifest
from meridian_storage.adapters.postgresql.migration import MigrationExecutor
from meridian_storage.adapters.postgresql.schema import SchemaCompiler


def fp(label: str) -> str:
    return "sha256:" + hashlib.sha256(label.encode()).hexdigest()


def selected_engine_version() -> str:
    return os.environ.get(
        "MERIDIAN_POSTGRESQL_ENGINE_VERSION",
        "16-postgis-3.4",
    )


def people_schema() -> SchemaDocument:
    return SchemaDocument.from_definition(
        catalog="structured",
        namespace="example",
        name="person",
        version="1.0.0",
        definition={
            "semanticKind": "relational",
            "fields": [
                {
                    "name": "id",
                    "logicalType": "uuid",
                    "nullable": False,
                    "mutable": False,
                },
                {
                    "name": "name",
                    "logicalType": "string",
                    "nullable": False,
                    "mutable": True,
                },
                {
                    "name": "age",
                    "logicalType": "int32",
                    "nullable": True,
                    "mutable": True,
                },
                {
                    "name": "document",
                    "logicalType": "json",
                    "nullable": True,
                    "mutable": True,
                },
                {
                    "name": "location",
                    "logicalType": "wgs84Point",
                    "nullable": True,
                    "mutable": True,
                },
                {
                    "name": "balance",
                    "logicalType": {"kind": "decimal", "precision": 20, "scale": 4},
                    "nullable": True,
                    "mutable": True,
                },
                {
                    "name": "payload",
                    "logicalType": "bytes",
                    "nullable": True,
                    "mutable": True,
                },
            ],
            "identity": ["id"],
            "indexes": [
                {
                    "name": "name_idx",
                    "kind": "btree",
                    "fields": ["name"],
                    "unique": False,
                },
                {
                    "name": "name_search",
                    "kind": "full-text",
                    "fields": ["name"],
                    "unique": False,
                },
                {
                    "name": "location_geo",
                    "kind": "geospatial",
                    "fields": ["location"],
                    "unique": False,
                },
            ],
        },
    )


def sample_resources() -> list[dict[str, object]]:
    people = {
        "ref": "structured:example.people",
        "table": "people",
        "profile": "relational",
        "schemaFingerprint": people_schema().fingerprint,
        "resourceFingerprint": fp("people-resource-v1"),
        "fields": [
            {
                "name": "id",
                "column": "id",
                "logicalType": "uuid",
                "nullable": False,
                "mutable": False,
            },
            {
                "name": "name",
                "column": "name",
                "logicalType": "string",
                "nullable": False,
                "mutable": True,
            },
            {
                "name": "age",
                "column": "age",
                "logicalType": "int32",
                "nullable": True,
                "mutable": True,
            },
            {
                "name": "document",
                "column": "document",
                "logicalType": "json",
                "nullable": True,
                "mutable": True,
            },
            {
                "name": "location",
                "column": "location",
                "logicalType": "wgs84Point",
                "nullable": True,
                "mutable": True,
            },
            {
                "name": "balance",
                "column": "balance",
                "logicalType": "decimal",
                "precision": 20,
                "scale": 4,
                "nullable": True,
                "mutable": True,
            },
            {
                "name": "payload",
                "column": "payload",
                "logicalType": "bytes",
                "nullable": True,
                "mutable": True,
            },
        ],
        "identity": ["id"],
        "indexes": [
            {"name": "name_idx", "kind": "btree", "fields": ["name"], "unique": False},
            {
                "name": "name_search",
                "kind": "full-text",
                "fields": ["name"],
                "unique": False,
            },
            {
                "name": "location_geo",
                "kind": "geospatial",
                "fields": ["location"],
                "unique": False,
            },
        ],
        "relation": None,
    }
    work = {
        "ref": "structured:example.work",
        "table": "work_items",
        "profile": "key-value",
        "schemaFingerprint": fp("work-schema-v1"),
        "resourceFingerprint": fp("work-resource-v1"),
        "fields": [
            {
                "name": "id",
                "column": "id",
                "logicalType": "uuid",
                "nullable": False,
                "mutable": False,
            },
            {
                "name": "state",
                "column": "state",
                "logicalType": "string",
                "nullable": False,
                "mutable": True,
            },
            {
                "name": "owner",
                "column": "owner",
                "logicalType": "string",
                "nullable": True,
                "mutable": True,
            },
            {
                "name": "priority",
                "column": "priority",
                "logicalType": "int32",
                "nullable": False,
                "mutable": True,
            },
        ],
        "identity": ["id"],
        "indexes": [
            {
                "name": "claim_idx",
                "kind": "btree",
                "fields": ["state", "priority"],
                "unique": False,
            }
        ],
        "relation": None,
    }
    friendship = {
        "ref": "structured:example.friendships",
        "table": "friendships",
        "profile": "relation",
        "schemaFingerprint": fp("friendships-schema-v1"),
        "resourceFingerprint": fp("friendships-resource-v1"),
        "fields": [
            {
                "name": "id",
                "column": "id",
                "logicalType": "uuid",
                "nullable": False,
                "mutable": False,
            },
            {
                "name": "source",
                "column": "source_ref",
                "logicalType": "recordRef",
                "nullable": False,
                "mutable": False,
            },
            {
                "name": "target",
                "column": "target_ref",
                "logicalType": "recordRef",
                "nullable": False,
                "mutable": False,
            },
            {
                "name": "label",
                "column": "label",
                "logicalType": "string",
                "nullable": False,
                "mutable": True,
            },
        ],
        "identity": ["id"],
        "indexes": [],
        "relation": {
            "sourceField": "source",
            "targetField": "target",
            "directed": True,
            "sourceCollections": ["structured:example.people"],
            "targetCollections": ["structured:example.people"],
        },
    }
    evidence = {
        "ref": "evidence:example.outbox",
        "table": "outbox",
        "profile": "append-only-evidence",
        "schemaFingerprint": fp("outbox-schema-v1"),
        "resourceFingerprint": fp("outbox-resource-v1"),
        "fields": [
            {
                "name": "id",
                "column": "id",
                "logicalType": "uuid",
                "nullable": False,
                "mutable": False,
            },
            {
                "name": "kind",
                "column": "kind",
                "logicalType": "string",
                "nullable": False,
                "mutable": False,
            },
            {
                "name": "payload",
                "column": "payload",
                "logicalType": "json",
                "nullable": False,
                "mutable": False,
            },
        ],
        "identity": ["id"],
        "indexes": [{"name": "kind_idx", "kind": "btree", "fields": ["kind"], "unique": False}],
        "relation": None,
    }
    follows = deepcopy(friendship)
    follows.update(
        {
            "ref": "structured:example.follows",
            "table": "follows",
            "schemaFingerprint": fp("follows-schema-v1"),
            "resourceFingerprint": fp("follows-resource-v1"),
        }
    )
    return [people, work, friendship, follows, evidence]


def sample_settings_mapping(*, expected_standbys: int = 0) -> dict[str, object]:
    return {
        "formatVersion": "meridian.postgresql.settings.v1",
        "applicationName": "meridian-postgresql-tests",
        "scopeKeys": ["workspace"],
        "topology": {"expectedStandbys": expected_standbys},
        "resources": sample_resources(),
    }


@pytest.fixture
def settings() -> PostgreSQLSettings:
    binding = SimpleNamespace(
        engine_profile="postgresql-postgis-local-single-primary",
        settings=sample_settings_mapping(),
        physical_namespace="meridian_test",
        tls=SimpleNamespace(mode="disabled"),
    )
    return PostgreSQLSettings.from_binding(binding)


def make_binding(
    endpoint: str,
    *,
    engine_profile: str = "postgresql-postgis-local-single-primary",
    engine_version: str | None = None,
    expected_standbys: int = 0,
    required_physical_fingerprint: str | None = None,
) -> tuple[BindingConfig, str, str]:
    selected_version = engine_version or selected_engine_version()
    parsed = conninfo_to_dict(endpoint)
    user = parsed.pop("user", "meridian")
    password = parsed.pop("password", "meridian")
    clean_endpoint = make_conninfo("", **parsed)
    binding = BindingConfig(
        id="postgresql-test",
        adapter_id="postgresql",
        adapter_contract="1.0.0",
        engine_profile=engine_profile,
        engine_version=selected_version,
        endpoint=clean_endpoint,
        service_ref=None,
        physical_namespace="meridian_test",
        tls=TLSPolicy("disabled", None, None, None),
        identity_ref=SecretReference("test", "identity"),
        secret_ref=SecretReference("test", "credential"),
        client=ClientPolicy(
            min_size=1,
            max_size=4,
            acquire_timeout_ms=10_000,
            idle_timeout_ms=30_000,
            operation_timeout_ms=10_000,
            max_result_bytes=16 * 1024 * 1024,
            iterator_lifetime_ms=30_000,
        ),
        required_capability_fingerprint=manifest(
            engine_profile,
            selected_version,
        ).fingerprint,
        required_physical_fingerprint=required_physical_fingerprint,
        compatibility_pins={},
        settings=sample_settings_mapping(expected_standbys=expected_standbys),
        extensions={},
    )
    return binding, user, password


def make_create_context(endpoint: str) -> tuple[AdapterCreateContext, PostgreSQLSettings, Any]:
    binding, user, password = make_binding(endpoint)
    settings = PostgreSQLSettings.from_binding(binding)
    plan = SchemaCompiler(settings).compile()
    binding = replace(binding, required_physical_fingerprint=plan.physical_fingerprint)
    return (
        AdapterCreateContext(
            binding=binding,
            identity=SecretValue(user.encode()),
            credential=SecretValue(password.encode()),
        ),
        settings,
        plan,
    )


@pytest.fixture(scope="session")
def postgresql_dsn() -> str:
    value = os.environ.get("MERIDIAN_POSTGRESQL_TEST_DSN")
    if not value:
        pytest.skip("MERIDIAN_POSTGRESQL_TEST_DSN is not configured")
    return value


@pytest.fixture(scope="session")
def integration_context(
    postgresql_dsn: str,
) -> tuple[AdapterCreateContext, PostgreSQLSettings, Any]:
    context, settings, plan = make_create_context(postgresql_dsn)
    with connect(postgresql_dsn, autocommit=True) as connection:
        connection.execute('DROP SCHEMA IF EXISTS "meridian_test" CASCADE')
    with connect(postgresql_dsn) as connection:
        MigrationExecutor(settings).apply(connection, plan)
    return context, settings, plan


@pytest.fixture
def operation_context() -> OperationContext:
    return OperationContext(
        principal_ref="test:principal",
        request_id="request-1",
        tenant="tenant-a",
        scope={"workspace": "workspace-a"},
    )


def physical_resources(settings: PostgreSQLSettings) -> tuple[PhysicalResource, ...]:
    return tuple(
        PhysicalResource(
            resource_ref=layout.ref,
            resource_fingerprint=layout.resource_fingerprint,
            schema_fingerprint=layout.schema_fingerprint,
            profile=layout.profile,
        )
        for layout in settings.resources.values()
    )
