# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import replace
from typing import Any

import pytest
from conftest import people_schema
from meridian_storage.semantics import (
    ActivationPlan,
    CollectionDocument,
    ResourceReference,
    SemanticsAdapter,
)
from psycopg import Connection

from meridian_storage.adapters.postgresql.schema import SchemaCompiler
from meridian_storage.adapters.postgresql.semantics import PostgreSQLSemanticsAdapter


@contextmanager
def unavailable_connection() -> Iterator[Connection[Any]]:
    raise AssertionError("unit conformance must not open PostgreSQL")
    yield  # pragma: no cover


def test_released_semantics_spi_validates_plans_and_values(settings: Any) -> None:
    adapter = PostgreSQLSemanticsAdapter(settings, unavailable_connection)
    assert isinstance(adapter, SemanticsAdapter)
    schema = people_schema()
    collection = CollectionDocument(
        ResourceReference.parse("structured:example.people"),
        schema.ref,
        "relational",
    )

    adapter.validate_definition(schema, collection)
    plan = adapter.plan_activation(collection, None, schema)
    physical_fingerprint = SchemaCompiler(settings).compile().physical_fingerprint
    assert plan.metadata["physicalFingerprint"] == physical_fingerprint
    assert plan.requirements == (
        "platform-migration-job",
        "postgis",
        "transactional-ddl",
    )

    record = {
        "id": "00000000-0000-0000-0000-000000000001",
        "name": "Ada",
        "document": {"nested": [1, True]},
    }
    encoded = adapter.encode_value(schema, record)
    assert adapter.decode_value(schema, encoded) == encoded

    wrong_profile = replace(collection, semantic_profile="document")
    with pytest.raises(ValueError, match="semantic profile"):
        adapter.validate_definition(schema, wrong_profile)

    tampered = ActivationPlan(
        plan.resource_ref,
        plan.target_schema_ref,
        steps=plan.steps,
        requirements=plan.requirements,
        metadata={**plan.metadata, "statementCount": 0},
    )
    with pytest.raises(ValueError, match="differs"):
        adapter.apply_activation(tampered)


def test_logical_import_envelope_fails_closed(settings: Any) -> None:
    adapter = PostgreSQLSemanticsAdapter(settings, unavailable_connection)
    with pytest.raises(ValueError, match="unknown or missing"):
        adapter.import_logical({"formatVersion": "bad"})
    with pytest.raises(ValueError, match="unsupported"):
        adapter.import_logical(
            {
                "formatVersion": "bad",
                "registryRevision": "revision",
                "resources": [],
                "records": [],
            }
        )
