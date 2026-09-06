# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest
from conftest import fp
from meridian_storage.context import OperationContext
from meridian_storage.query.wire import QueryOperation, QueryTarget
from meridian_storage.registry.resources import ResourceRef
from meridian_storage.runtime.operations import JsonValue
from meridian_storage.semantics import RecordReference, ResourceReference
from meridian_storage.spi.adapters import ExecutionRequest

from meridian_storage import Operation
from meridian_storage.adapters.postgresql._operation import OperationCompiler, QueryCommand
from meridian_storage.adapters.postgresql.query import PostgreSQLQueryTranslator
from meridian_storage.adapters.postgresql.query.dml import DMLCommand, DMLCompiler, jsonable

PEOPLE = ResourceRef.parse("structured:example.people")
WORK = ResourceRef.parse("structured:example.work")
FRIENDSHIPS = ResourceRef.parse("structured:example.friendships")
OUTBOX = ResourceRef.parse("evidence:example.outbox")


def context(*, deadline: datetime | None = None) -> OperationContext:
    return OperationContext(
        principal_ref="test",
        tenant="tenant-a",
        scope={"workspace": "workspace-a"},
        deadline=deadline,
    )


def operation(
    method: str,
    *,
    catalog: str = "structured",
    resources: tuple[ResourceRef, ...] = (PEOPLE,),
    input_value: dict[str, JsonValue] | None = None,
) -> Operation:
    return Operation(
        catalog=catalog,
        operation_contract=f"meridian.{catalog}.{method}",
        operation_version="2.0.0" if (catalog, method) == ("structured", "put") else "1.0.0",
        resources=resources,
        input=(
            {"mode": "if_absent", **(input_value or {})} if method == "put" else input_value or {}
        ),
        read_only=method in {"get", "query", "search", "aggregate", "traverse"},
        idempotent=method not in {"append"},
    )


def request(
    value: Operation,
    *,
    operation_context: OperationContext | None = None,
) -> ExecutionRequest:
    return ExecutionRequest(
        operation=value,
        context=operation_context or context(),
        request_id="request",
        execution_id="execution",
        binding_id="postgresql-test",
        registry_revision=1,
        registry_fingerprint=fp("registry"),
        attempt=1,
    )


def compiler(settings: object) -> OperationCompiler:
    translator = PostgreSQLQueryTranslator(settings)
    return OperationCompiler(settings, translator)


def test_operation_dispatch_covers_dml_and_query_surfaces(settings: object) -> None:
    selected = compiler(settings)
    commands = (
        operation(
            "put",
            input_value={"data": {"id": "00000000-0000-0000-0000-000000000001", "name": "Ada"}},
        ),
        operation("get", input_value={"where": {"id": "00000000-0000-0000-0000-000000000001"}}),
        operation("patch", input_value={"where": {"name": "Ada"}, "changes": {"age": 37}}),
        operation("delete", input_value={"where": {"name": "Ada"}}),
        operation(
            "append",
            catalog="evidence",
            resources=(OUTBOX,),
            input_value={
                "event": {
                    "id": "00000000-0000-0000-0000-000000000002",
                    "kind": "test",
                    "payload": {"ok": True},
                }
            },
        ),
    )
    assert all(isinstance(selected.compile(request(item)), DMLCommand) for item in commands)

    queries = (
        operation(
            "query",
            input_value={
                "where": {"age": {"gte": 18}},
                "select": ["id", "name"],
                "orderBy": [{"field": "name", "direction": "desc", "nulls": "first"}],
                "limit": 10,
            },
        ),
        operation(
            "search",
            input_value={
                "where": {"age": {"gte": 18}},
                "query": {"text": "Ada", "fields": ["name"]},
                "limit": 10,
            },
        ),
        operation(
            "aggregate",
            input_value={
                "groupBy": ["age"],
                "metrics": [
                    {"name": "people", "function": "count", "field": None, "distinct": False},
                    {"name": "total", "function": "sum", "field": "age", "distinct": True},
                ],
            },
        ),
        operation(
            "traverse",
            resources=(PEOPLE, FRIENDSHIPS),
            input_value={
                "start": RecordReference(
                    ResourceReference.parse(PEOPLE.canonical),
                    "00000000-0000-0000-0000-000000000001",
                ).to_dict(),
                "maxDepth": 2,
            },
        ),
    )
    compiled = [selected.compile(request(item)) for item in queries]
    assert all(isinstance(item, QueryCommand) for item in compiled)
    assert "GROUP BY" in compiled[2].compiled.command["sql"]
    assert "WITH RECURSIVE" in compiled[3].compiled.command["sql"]


def test_operation_dispatch_rejects_unadvertised_shapes(settings: object) -> None:
    selected = compiler(settings)
    object_ref = ResourceRef.parse("object:example.blobs")
    with pytest.raises(ValueError, match="structured and evidence"):
        selected.compile(
            request(
                operation(
                    "get",
                    catalog="object",
                    resources=(object_ref,),
                    input_value={"where": {"id": "x"}},
                )
            )
        )

    mismatched = operation("get", input_value={"where": {"id": "x"}})
    object.__setattr__(mismatched, "operation_contract", "meridian.evidence.query")
    with pytest.raises(ValueError, match="does not match"):
        selected.compile(request(mismatched))

    with pytest.raises(ValueError, match="Platform migration"):
        selected.compile(request(operation("create_resource")))
    with pytest.raises(ValueError, match="unsupported"):
        selected.compile(request(operation("unknown")))
    with pytest.raises(TypeError, match="queryPlan"):
        selected.compile(request(operation("query", input_value={"queryPlan": "bad"})))
    with pytest.raises(TypeError, match="where"):
        selected.compile(request(operation("query", input_value={"where": "bad"})))

    evidence_put = operation(
        "put",
        catalog="evidence",
        resources=(OUTBOX,),
        input_value={"data": {}},
    )
    with pytest.raises(ValueError, match="unsupported evidence"):
        selected.compile(request(evidence_put))

    cross_catalog = operation("put", input_value={"data": {}})
    object.__setattr__(cross_catalog, "resources", (OUTBOX,))
    with pytest.raises(ValueError, match="belong to its Catalog"):
        selected.compile(request(cross_catalog))

    wrong_version = operation("get", input_value={"where": {}})
    object.__setattr__(wrong_version, "operation_version", "2.0.0")
    with pytest.raises(ValueError, match=r"version 1\.0\.0"):
        selected.compile(request(wrong_version))

    plan = QueryOperation(
        catalog="structured",
        targets=(QueryTarget(PEOPLE),),
        operation="scan",
    )
    mismatched_resources = operation(
        "query",
        resources=(WORK,),
        input_value={"queryPlan": plan.to_dict()},
    )
    with pytest.raises(ValueError, match="Resources differ"):
        selected.compile(request(mismatched_resources))


@pytest.mark.parametrize(
    ("method", "input_value", "message"),
    [
        ("query", {"select": "id"}, "invalid type"),
        ("query", {"orderBy": [{"unknown": "id"}]}, "field/direction/nulls"),
        ("query", {"orderBy": [{"field": "missing"}]}, "unknown field"),
        ("search", {"query": "Ada", "facets": ["age"]}, "facets"),
        ("search", {"query": {"text": "Ada", "fields": "name"}}, "fields"),
        ("search", {"query": {"text": "Ada", "fields": ["age"]}}, "pinned full-text"),
        ("search", {"query": 42}, "text"),
        ("search", {"query": "Ada", "limit": True}, "limit"),
        ("aggregate", {"groupBy": "age", "metrics": []}, "must be arrays"),
        ("aggregate", {"groupBy": [], "metrics": [{"extra": True}]}, "closed V1"),
        (
            "aggregate",
            {"groupBy": [], "metrics": [{"name": 1, "function": "count"}]},
            "must be strings",
        ),
        (
            "aggregate",
            {
                "groupBy": [],
                "metrics": [{"name": "count", "function": "count", "distinct": "yes"}],
            },
            "distinct must be a boolean",
        ),
        ("traverse", {"start": "bad"}, "RecordReference"),
        (
            "traverse",
            {
                "start": RecordReference(
                    ResourceReference.parse(PEOPLE.canonical),
                    "00000000-0000-0000-0000-000000000001",
                ).to_dict(),
                "maxDepth": True,
            },
            "maxDepth",
        ),
    ],
)
def test_structured_operation_validation(
    settings: object,
    method: str,
    input_value: dict[str, JsonValue],
    message: str,
) -> None:
    resources = (PEOPLE, FRIENDSHIPS) if method == "traverse" else (PEOPLE,)
    with pytest.raises((TypeError, ValueError), match=message):
        compiler(settings).compile(
            request(operation(method, resources=resources, input_value=input_value))
        )


def test_search_requires_full_text_layout(settings: object) -> None:
    with pytest.raises(ValueError, match="full-text index"):
        compiler(settings).compile(
            request(operation("search", resources=(WORK,), input_value={"query": "ready"}))
        )


def test_dml_validation_and_conditional_paths(settings: object) -> None:
    dml = DMLCompiler(settings)
    ctx = context()
    people_id = "00000000-0000-0000-0000-000000000001"

    conditional_put = dml.compile(
        "put",
        PEOPLE,
        {"data": {"id": people_id, "name": "Ada"}, "mode": "update", "expectedVersion": 1},
        ctx,
    )
    assert conditional_put.conditional
    conditional_patch = dml.compile(
        "patch",
        PEOPLE,
        {"where": {"id": people_id}, "changes": {"age": 38}, "expectedVersion": 1},
        ctx,
    )
    assert conditional_patch.conditional
    conditional_delete = dml.compile(
        "delete",
        PEOPLE,
        {"where": {"id": people_id}, "expectedVersion": 2},
        ctx,
    )
    assert conditional_delete.conditional

    immutable_only = dml.compile(
        "put",
        OUTBOX,
        {"mode": "upsert", "data": {"id": people_id, "kind": "test", "payload": {"ok": True}}},
        ctx,
    )
    assert "__updated_at = t.__updated_at" in immutable_only.statement.command.as_string(None)

    invalid: tuple[tuple[str, ResourceRef, dict[str, object], str], ...] = (
        ("unknown", PEOPLE, {}, "does not implement"),
        ("put", PEOPLE, {"data": "bad"}, "data must be"),
        ("put", PEOPLE, {"data": {"id": people_id}}, "fields mismatch"),
        ("get", PEOPLE, {"where": "bad"}, "where must be"),
        ("patch", PEOPLE, {"where": {}, "changes": "bad"}, "must be objects"),
        ("delete", PEOPLE, {"where": "bad"}, "where must be"),
        ("append", OUTBOX, {"event": "bad"}, "data object"),
        (
            "append",
            OUTBOX,
            {"data": {"id": people_id, "kind": "test", "unknown": True}},
            "fields mismatch",
        ),
    )
    for method, resource, input_value, message in invalid:
        with pytest.raises((TypeError, ValueError), match=message):
            dml.compile(method, resource, {"mode": "if_absent", **input_value}, ctx)

    with pytest.raises(ValueError, match="between 1 and 500"):
        dml.atomic_claim(WORK, where={}, changes={"state": "x"}, limit=0, context=ctx)
    with pytest.raises(ValueError, match="cannot be empty"):
        dml._assignments(settings.layout(PEOPLE), {})
    with pytest.raises(ValueError, match="immutable"):
        dml._assignments(settings.layout(PEOPLE), {"id": people_id, "unknown": 1})


def test_dml_values_scope_and_json_normalization(settings: object) -> None:
    dml = DMLCompiler(settings)
    people = settings.layout(PEOPLE)
    with pytest.raises(ValueError, match="not nullable"):
        dml._value(people.field_map["name"], None)
    assert dml._value(people.field_map["age"], None)[1] == (None,)
    with pytest.raises(ValueError, match="longitude"):
        dml._value(people.field_map["location"], {"latitude": 1})
    with pytest.raises(ValueError, match="out of range"):
        dml._value(
            people.field_map["location"],
            {"longitude": 181, "latitude": 0},
        )
    with pytest.raises(TypeError, match="base64url"):
        dml._value(people.field_map["payload"], b"not-text")
    with pytest.raises(ValueError, match="invalid base64url"):
        dml._value(people.field_map["payload"], "not+base64")

    with pytest.raises(ValueError, match="scope mismatch"):
        dml.compile(
            "get",
            PEOPLE,
            {"where": {}},
            OperationContext(principal_ref="test", tenant=None, scope={}),
        )
    with pytest.raises(ValueError, match="scope mismatch"):
        dml.compile(
            "get",
            PEOPLE,
            {"where": {}},
            OperationContext(
                principal_ref="test",
                tenant="tenant-a",
                scope={"workspace": "a", "extra": "b"},
            ),
        )

    instant = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    assert jsonable(Decimal("1.20")) == "1.20"
    assert jsonable(b"\x00\xff") == "AP8"
    assert jsonable(instant) == "2026-01-01T12:00:00Z"
    assert jsonable(date(2026, 1, 2)) == "2026-01-02"
    assert jsonable(timedelta(seconds=1.5)) == "PT1.5S"
    assert jsonable({"value": [Decimal("2.0")]}) == {"value": ["2.0"]}
    assert jsonable(object()).startswith("<object object at")


def test_deadline_is_bounded_by_operation_context(settings: object) -> None:
    deadline = datetime.now(UTC) + timedelta(milliseconds=250)
    selected = compiler(settings)
    result = selected.compile(
        request(
            operation("query", input_value={"limit": 1}),
            operation_context=context(deadline=deadline),
        )
    )
    assert isinstance(result, QueryCommand)
