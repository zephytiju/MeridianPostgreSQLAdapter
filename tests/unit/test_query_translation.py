# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from datetime import UTC, datetime

import pytest
from conftest import fp, people_schema
from meridian_storage.query.adapter import TranslationContext
from meridian_storage.query.ast import Field, Projection, distance_within, point
from meridian_storage.query.cursor import CursorSigner
from meridian_storage.query.wire import Join, PageSpec, QueryOperation, QueryTarget, ResultSpec
from meridian_storage.registry.resources import ResourceRef

from meridian_storage.adapters.postgresql._settings import PostgreSQLSettings
from meridian_storage.adapters.postgresql.query import PostgreSQLQueryTranslator


def context() -> TranslationContext:
    return TranslationContext(
        binding_id="postgresql-test",
        plan_fingerprint=fp("plan"),
        registry_fingerprint=fp("registry"),
        schema_fingerprints={"structured:example.people": people_schema().fingerprint},
        scope_fingerprint=fp("scope"),
        deadline_ms=30_000,
    )


def test_distance_filter_is_postgis_geography_with_bound_meters(
    settings: PostgreSQLSettings,
) -> None:
    operation = QueryOperation(
        catalog="structured",
        targets=(QueryTarget(ResourceRef.parse("structured:example.people")),),
        operation="scan",
        filter=distance_within(Field("location"), point(-122.4194, 37.7749), "1250.5"),
    )
    compiled = PostgreSQLQueryTranslator(settings).compile(operation, context())
    command = compiled.command
    assert isinstance(command, dict) or hasattr(command, "get")
    sql_text = command["sql"]
    assert "ST_DWithin" in sql_text
    assert "ST_MakePoint" in sql_text
    assert "1250.5" not in sql_text
    assert "1250.5" in compiled.parameters.values()


def test_literals_never_enter_sql(settings: PostgreSQLSettings) -> None:
    payload = "Robert'); DROP TABLE people;--"
    operation = QueryOperation(
        catalog="structured",
        targets=(QueryTarget(ResourceRef.parse("structured:example.people")),),
        operation="scan",
        filter=Field("name").eq(payload),
    )
    compiled = PostgreSQLQueryTranslator(settings).compile(operation, context())
    assert payload not in compiled.command["sql"]
    assert payload in compiled.parameters.values()


def test_live_cursor_is_signed_and_scope_bound(settings: PostgreSQLSettings) -> None:
    def now() -> datetime:
        return datetime(2026, 1, 1, tzinfo=UTC)

    signer = CursorSigner({"v1": b"x" * 32}, active_key_id="v1", clock=now)
    translator = PostgreSQLQueryTranslator(settings, cursor_signer=signer)
    operation = QueryOperation(
        catalog="structured",
        targets=(QueryTarget(ResourceRef.parse("structured:example.people")),),
        operation="scan",
        order=(),
        page=PageSpec(2),
    )
    compiled = translator.compile(operation, context())
    cursor_fields = compiled.command["cursorFields"]
    rows = [
        {"id": "00000000-0000-0000-0000-000000000001", cursor_fields[0]: "a"},
        {"id": "00000000-0000-0000-0000-000000000002", cursor_fields[0]: "b"},
        {"id": "00000000-0000-0000-0000-000000000003", cursor_fields[0]: "c"},
    ]
    normalized = translator.normalize_result(compiled, rows)
    assert normalized.cursor is not None
    payload = signer.verify(normalized.cursor)
    assert payload.scope_fingerprint == fp("scope")
    assert payload.sort_tuple == ("b",)


def test_unbound_parameter_is_rejected(settings: PostgreSQLSettings) -> None:
    from meridian_storage.query.ast import Parameter

    operation = QueryOperation(
        catalog="structured",
        targets=(QueryTarget(ResourceRef.parse("structured:example.people")),),
        operation="scan",
        filter=Field("name").eq(Parameter("name", "string")),
    )
    with pytest.raises(ValueError, match="unbound query parameters"):
        PostgreSQLQueryTranslator(settings).compile(operation, context())


def test_every_join_is_scope_bound(settings: PostgreSQLSettings) -> None:
    people = ResourceRef.parse("structured:example.people")
    work = ResourceRef.parse("structured:example.work")
    operation = QueryOperation(
        catalog="structured",
        targets=(QueryTarget(people), QueryTarget(work)),
        operation="scan",
        result=ResultSpec(
            "records",
            (Projection(Field("state", resource=work.canonical), "state"),),
        ),
        joins=(
            Join(
                QueryTarget(work),
                Field("id", resource=people.canonical).eq(
                    Field("id", resource=work.canonical)
                ),
                "left",
            ),
        ),
    )
    compiled = PostgreSQLQueryTranslator(settings).compile(operation, context())
    sql_text = compiled.command["sql"]
    assert 'LEFT JOIN "meridian_test"."work_items" AS "t1" ON' in sql_text
    assert '"t1"."__tenant" = %s' in sql_text
    assert '"t1"."__scope_workspace" = %s' in sql_text
    scope_markers = [
        value for value in compiled.parameters.values() if value == {"$scope": "__tenant"}
    ]
    assert len(scope_markers) == 2


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"consistency": "eventual"}, "consistency"),
        ({"page": PageSpec(10, point_in_time=True)}, "point-in-time"),
        ({"result": ResultSpec("records", include_total=True)}, "total counts"),
    ],
)
def test_unadvertised_query_semantics_fail_closed(
    settings: PostgreSQLSettings,
    changes: dict[str, object],
    message: str,
) -> None:
    from dataclasses import replace

    operation = QueryOperation(
        catalog="structured",
        targets=(QueryTarget(ResourceRef.parse("structured:example.people")),),
        operation="scan",
    )
    with pytest.raises(ValueError, match=message):
        PostgreSQLQueryTranslator(settings).compile(replace(operation, **changes), context())
