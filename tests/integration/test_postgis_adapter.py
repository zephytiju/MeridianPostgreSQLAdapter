# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import uuid
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from conftest import (
    fp,
    people_schema,
    physical_resources,
    sample_settings_mapping,
    selected_engine_version,
)
from meridian_storage.context import OperationContext, bind_context
from meridian_storage.errors import ConflictError, MeridianTimeoutError, ValidationError
from meridian_storage.query.ast import Field, Projection, distance_within, point
from meridian_storage.query.wire import (
    Join,
    PageSpec,
    QueryOperation,
    QueryTarget,
    ResultSpec,
    TraversalSpec,
)
from meridian_storage.registry.resources import ResourceRef
from meridian_storage.runtime.operations import Operation
from meridian_storage.semantics import (
    CollectionDocument,
    RecordReference,
    ResourceReference,
    StructuredCatalogProvider,
)
from meridian_storage.spi.adapters import ExecutionRequest
from meridian_storage.testing.adapter_conformance import (
    AdapterConformanceTarget,
    run_adapter_conformance,
)
from psycopg import connect, errors
from psycopg.rows import dict_row

from meridian_storage.adapters.postgresql import PostgreSQLAdapterFactory
from meridian_storage.adapters.postgresql._settings import PostgreSQLSettings
from meridian_storage.adapters.postgresql.migration import LogicalTransfer, MigrationExecutor
from meridian_storage.adapters.postgresql.probe import ProbeService
from meridian_storage.adapters.postgresql.query.dml import DMLCompiler
from meridian_storage.adapters.postgresql.schema import SchemaCompiler

pytestmark = pytest.mark.integration


def request(operation: Operation, context: OperationContext, suffix: str = "1") -> ExecutionRequest:
    return ExecutionRequest(
        operation=operation,
        context=context,
        request_id=context.request_id or f"request-{suffix}",
        execution_id=f"execution-{suffix}",
        binding_id="postgresql-test",
        registry_revision=1,
        registry_fingerprint=fp("registry"),
        attempt=1,
    )


def execute(
    runtime: Any, operation: Operation, context: OperationContext, suffix: str = "1"
) -> Any:
    session = runtime.open_session(transactional=False)
    try:
        return session.execute(request(operation, context, suffix)).data
    finally:
        session.close()


@pytest.fixture
def runtime(integration_context: tuple[Any, Any, Any], postgresql_dsn: str) -> Any:
    create_context, settings, _ = integration_context
    with connect(postgresql_dsn, autocommit=True) as connection:
        for layout in settings.resources.values():
            connection.execute(f'TRUNCATE TABLE "{settings.physical_schema}"."{layout.table}"')
    result = PostgreSQLAdapterFactory().create(create_context)
    result.open()
    try:
        yield result
    finally:
        result.close()


def provider() -> tuple[StructuredCatalogProvider, Any]:
    result = StructuredCatalogProvider()
    return result, result.create_surface()


def test_crud_json_types_cas_and_search(
    runtime: Any,
    operation_context: OperationContext,
) -> None:
    catalog, surface = provider()
    person_id = str(uuid.uuid4())
    encoded = "AAEC_w"
    put = catalog.normalize(
        surface.put(
            resource="example.people",
            data={
                "id": person_id,
                "name": "Ada Lovelace",
                "age": 36,
                "document": {"role": "mathematician", "tags": ["analysis"]},
                "location": {"longitude": -0.1276, "latitude": 51.5072},
                "balance": "123.4500",
                "payload": encoded,
            },
        )
    )
    created = execute(runtime, put, operation_context)
    assert created["recordVersion"] == 1
    assert created["payload"] == encoded
    assert created["balance"] == "123.4500"
    assert created["document"]["role"] == "mathematician"
    assert abs(created["location"]["longitude"] + 0.1276) < 1e-6

    get = catalog.normalize(surface.get(resource="example.people", where={"id": person_id}))
    fetched = execute(runtime, get, operation_context, "get")
    assert fetched["id"] == person_id
    typed_filter = QueryOperation(
        catalog="structured",
        targets=(QueryTarget(ResourceRef.parse("structured:example.people")),),
        operation="scan",
        filter=Field("document")
        .contains({"role": "mathematician"})
        .and_(Field("payload").eq(encoded)),
        page=PageSpec(10),
    ).to_core_operation()
    typed_rows = execute(runtime, typed_filter, operation_context, "typed-filter")
    assert [row["id"] for row in typed_rows["items"]] == [person_id]

    updated = execute(
        runtime,
        catalog.normalize(
            surface.put(
                resource="example.people",
                data={
                    "id": person_id,
                    "name": "Ada King",
                    "age": 37,
                    "document": {"role": "mathematician"},
                    "location": {"longitude": -0.1276, "latitude": 51.5072},
                    "balance": "123.4500",
                    "payload": encoded,
                },
                expected_version=1,
            )
        ),
        operation_context,
        "cas-success",
    )
    assert updated["recordVersion"] == 2
    with pytest.raises(ConflictError):
        execute(
            runtime,
            catalog.normalize(
                surface.patch(
                    resource="example.people",
                    where={"id": person_id},
                    changes={"age": 38},
                    expected_version=1,
                )
            ),
            operation_context,
            "cas-stale",
        )

    search = catalog.normalize(surface.search(resource="example.people", query="Ada", limit=10))
    result = execute(runtime, search, operation_context, "search")
    assert [row["id"] for row in result["items"]] == [person_id]


def test_join_scope_and_binding_identity_are_enforced(
    runtime: Any,
    operation_context: OperationContext,
) -> None:
    catalog, surface = provider()
    shared_id = str(uuid.uuid4())
    execute(
        runtime,
        catalog.normalize(
            surface.put(
                resource="example.people",
                data={"id": shared_id, "name": "Tenant A"},
            )
        ),
        operation_context,
        "join-person",
    )
    other_tenant = replace(operation_context, tenant="tenant-b")
    execute(
        runtime,
        catalog.normalize(
            surface.put(
                resource="example.work",
                data={"id": shared_id, "state": "private", "priority": 1},
            )
        ),
        other_tenant,
        "join-work",
    )

    people = ResourceRef.parse("structured:example.people")
    work = ResourceRef.parse("structured:example.work")
    query = QueryOperation(
        catalog="structured",
        targets=(QueryTarget(people), QueryTarget(work)),
        operation="scan",
        result=ResultSpec(
            "records",
            (
                Projection(Field("id", resource=people.canonical), "id"),
                Projection(Field("state", resource=work.canonical), "joinedState"),
            ),
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
        page=PageSpec(10),
    ).to_core_operation()
    result = execute(runtime, query, operation_context, "join-query")
    assert tuple(dict(item) for item in result["items"]) == (
        {"id": shared_id, "joinedState": None},
    )

    wrong_binding = replace(request(query, operation_context, "wrong-binding"), binding_id="other")
    session = runtime.open_session(transactional=False)
    try:
        with pytest.raises(ValidationError, match="different Binding"):
            session.execute(wrong_binding)
    finally:
        session.close()


def test_postgis_distance_boundary_and_live_keyset(
    runtime: Any,
    operation_context: OperationContext,
) -> None:
    catalog, surface = provider()
    ids = [str(uuid.uuid4()) for _ in range(4)]
    points = [
        (-122.4194, 37.7749),
        (-122.4195, 37.7750),
        (-118.2437, 34.0522),
        (-73.9857, 40.7484),
    ]
    for index, (person_id, coordinates) in enumerate(zip(ids, points, strict=True)):
        execute(
            runtime,
            catalog.normalize(
                surface.put(
                    resource="example.people",
                    data={
                        "id": person_id,
                        "name": f"Person {index}",
                        "location": {
                            "longitude": coordinates[0],
                            "latitude": coordinates[1],
                        },
                    },
                )
            ),
            operation_context,
            f"put-{index}",
        )
    resource = ResourceRef.parse("structured:example.people")
    distance_query = QueryOperation(
        catalog="structured",
        targets=(QueryTarget(resource),),
        operation="scan",
        filter=distance_within(Field("location"), point(-122.4194, 37.7749), 30),
        order=(),
        page=PageSpec(10),
    ).to_core_operation()
    nearby = execute(runtime, distance_query, operation_context, "distance")
    assert {row["id"] for row in nearby["items"]} == {ids[0], ids[1]}

    first = execute(
        runtime,
        catalog.normalize(
            surface.query(
                resource="example.people",
                order_by=[{"field": "id", "direction": "asc", "nulls": "last"}],
                limit=2,
            )
        ),
        operation_context,
        "page-1",
    )
    assert len(first["items"]) == 2 and first["cursor"]
    second = execute(
        runtime,
        catalog.normalize(
            surface.query(
                resource="example.people",
                order_by=[{"field": "id", "direction": "asc", "nulls": "last"}],
                limit=2,
                cursor=first["cursor"],
            )
        ),
        operation_context,
        "page-2",
    )
    assert len(second["items"]) == 2
    assert {row["id"] for row in first["items"]}.isdisjoint({row["id"] for row in second["items"]})


def test_relation_traversal_is_bounded_and_collection_owned(
    runtime: Any,
    operation_context: OperationContext,
) -> None:
    catalog, surface = provider()
    first_id, second_id, third_id = (
        str(uuid.uuid4()),
        str(uuid.uuid4()),
        str(uuid.uuid4()),
    )
    for person_id, name in (
        (first_id, "First"),
        (second_id, "Second"),
        (third_id, "Third"),
    ):
        execute(
            runtime,
            catalog.normalize(
                surface.put(
                    resource="example.people",
                    data={"id": person_id, "name": name},
                )
            ),
            operation_context,
            person_id[-4:],
        )
    collection = {
        "catalog": "structured",
        "namespace": "example",
        "name": "people",
    }
    execute(
        runtime,
        catalog.normalize(
            surface.put(
                resource="example.friendships",
                data={
                    "id": str(uuid.uuid4()),
                    "source": {"collectionRef": collection, "recordId": first_id},
                    "target": {"collectionRef": collection, "recordId": second_id},
                    "label": "friend",
                },
            )
        ),
        operation_context,
        "edge",
    )
    execute(
        runtime,
        catalog.normalize(
            surface.put(
                resource="example.follows",
                data={
                    "id": str(uuid.uuid4()),
                    "source": {"collectionRef": collection, "recordId": second_id},
                    "target": {"collectionRef": collection, "recordId": third_id},
                    "label": "follows",
                },
            )
        ),
        operation_context,
        "second-edge",
    )
    relation_collections = (
        ResourceRef.parse("structured:example.friendships"),
        ResourceRef.parse("structured:example.follows"),
    )

    def traversal_operation(*, all_neighbors: bool) -> Operation:
        return QueryOperation(
            catalog="structured",
            targets=(QueryTarget(ResourceRef.parse("structured:example.people")),),
            operation="traverse",
            result=ResultSpec("paths"),
            traversal=TraversalSpec(
                start=RecordReference(
                    ResourceReference.parse("structured:example.people"), first_id
                ),
                relation_collections=relation_collections,
                all_neighbors=all_neighbors,
                direction="outbound",
                max_depth=3,
                result_shape="paths",
                registry_fingerprint=fp("registry") if all_neighbors else None,
            ),
            page=PageSpec(50),
        ).to_core_operation()

    expected = [(second_id, 1), (third_id, 2)]
    result = execute(
        runtime,
        traversal_operation(all_neighbors=False),
        operation_context,
        "traverse",
    )
    assert [(row["recordId"], row["depth"]) for row in result["items"]] == expected
    resolved = execute(
        runtime,
        traversal_operation(all_neighbors=True),
        operation_context,
        "all-neighbors",
    )
    assert [(row["recordId"], row["depth"]) for row in resolved["items"]] == expected


def test_atomic_claim_skips_locked_rows(
    runtime: Any,
    integration_context: tuple[Any, Any, Any],
    postgresql_dsn: str,
    operation_context: OperationContext,
) -> None:
    _, surface = provider()
    catalog = StructuredCatalogProvider()
    ids = [str(uuid.uuid4()), str(uuid.uuid4())]
    for index, work_id in enumerate(ids):
        execute(
            runtime,
            catalog.normalize(
                surface.put(
                    resource="example.work",
                    data={
                        "id": work_id,
                        "state": "ready",
                        "priority": index,
                    },
                )
            ),
            operation_context,
            f"work-{index}",
        )
    _, settings, _ = integration_context
    compiler = DMLCompiler(settings)
    command = compiler.atomic_claim(
        ResourceRef.parse("structured:example.work"),
        where={"state": "ready"},
        changes={"state": "claimed", "owner": "worker"},
        limit=1,
        context=operation_context,
    )
    first = connect(postgresql_dsn, row_factory=dict_row)
    second = connect(postgresql_dsn, row_factory=dict_row)
    try:
        first_rows = first.execute(
            command.statement.command, command.statement.parameters
        ).fetchall()
        second_rows = second.execute(
            command.statement.command, command.statement.parameters
        ).fetchall()
        assert len(first_rows) == len(second_rows) == 1
        assert first_rows[0]["id"] != second_rows[0]["id"]
        first.commit()
        second.commit()
    finally:
        first.close()
        second.close()


def test_server_cancels_statement_at_operation_deadline(
    runtime: Any,
    postgresql_dsn: str,
    operation_context: OperationContext,
) -> None:
    catalog, surface = provider()
    blocked_query = catalog.normalize(
        surface.get(
            resource="example.people",
            where={"id": "00000000-0000-0000-0000-000000000001"},
        )
    )
    deadline_context = replace(
        operation_context,
        request_id="deadline",
        deadline=datetime.now(UTC) + timedelta(milliseconds=250),
    )
    with connect(postgresql_dsn) as blocker:
        blocker.execute('LOCK TABLE "meridian_test"."people" IN ACCESS EXCLUSIVE MODE')
        with pytest.raises(MeridianTimeoutError):
            execute(runtime, blocked_query, deadline_context, "deadline")


def test_transactional_outbox_commit_and_rollback(
    runtime: Any,
    operation_context: OperationContext,
) -> None:
    catalog, surface = provider()
    people_ref = ResourceRef.parse("structured:example.people")
    outbox_ref = ResourceRef.parse("evidence:example.outbox")

    def append(event_id: str, person_id: str) -> Operation:
        return Operation(
            catalog="evidence",
            operation_contract="meridian.evidence.append",
            operation_version="1.0.0",
            resources=(outbox_ref,),
            input={
                "resource": outbox_ref.to_dict(),
                "data": {
                    "id": event_id,
                    "kind": "person.created",
                    "payload": {"personId": person_id},
                },
            },
            read_only=False,
            idempotent=False,
        )

    rolled_person, rolled_event = str(uuid.uuid4()), str(uuid.uuid4())
    session = runtime.open_session(transactional=True)
    session.begin()
    try:
        session.execute(
            request(
                catalog.normalize(
                    surface.put(
                        resource=people_ref.to_dict(),
                        data={"id": rolled_person, "name": "Rolled back"},
                    )
                ),
                operation_context,
                "tx-person-rollback",
            )
        )
        session.execute(
            request(append(rolled_event, rolled_person), operation_context, "tx-event-rollback")
        )
        session.rollback()
    finally:
        session.close()
    missing = execute(
        runtime,
        catalog.normalize(surface.get(resource=people_ref.to_dict(), where={"id": rolled_person})),
        operation_context,
        "missing",
    )
    assert missing is None

    committed_person, committed_event = str(uuid.uuid4()), str(uuid.uuid4())
    session = runtime.open_session(transactional=True)
    session.begin()
    try:
        session.execute(
            request(
                catalog.normalize(
                    surface.put(
                        resource=people_ref.to_dict(),
                        data={"id": committed_person, "name": "Committed"},
                    )
                ),
                operation_context,
                "tx-person-commit",
            )
        )
        session.execute(
            request(
                append(committed_event, committed_person),
                operation_context,
                "tx-event-commit",
            )
        )
        session.commit()
    finally:
        session.close()
    evidence_query = Operation(
        catalog="evidence",
        operation_contract="meridian.evidence.query",
        operation_version="1.0.0",
        resources=(outbox_ref,),
        input={
            "resource": outbox_ref.to_dict(),
            "where": {"id": committed_event},
            "select": [],
            "orderBy": [],
            "limit": 10,
        },
        read_only=True,
        idempotent=True,
    )
    events = execute(runtime, evidence_query, operation_context, "evidence-query")
    assert events["items"][0]["payload"]["personId"] == committed_person


def test_migration_probe_physical_transfer_and_core_conformance(
    runtime: Any,
    integration_context: tuple[Any, Any, Any],
    postgresql_dsn: str,
    operation_context: OperationContext,
) -> None:
    create_context, settings, plan = integration_context
    lock_key = f"meridian:{settings.physical_schema}:migration"
    with connect(postgresql_dsn) as holder, connect(postgresql_dsn, autocommit=True) as contender:
        holder.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (lock_key,))
        contender.execute("SET lock_timeout = '100ms'")
        with pytest.raises(errors.LockNotAvailable):
            MigrationExecutor(settings).apply(contender, plan)
        holder.rollback()
    with connect(postgresql_dsn) as connection:
        evidence = MigrationExecutor(settings).apply(connection, plan)
        assert not evidence.applied
    probe = runtime.probe()
    assert probe.evidence["role"] == "primary"
    verification = runtime.verify_physical(physical_resources(settings))
    assert verification.fingerprint == plan.physical_fingerprint
    assert verification.evidence["stateFingerprint"].startswith("sha256:")
    with (
        connect(postgresql_dsn, row_factory=dict_row) as connection,
        connection.transaction(force_rollback=True),
    ):
        connection.execute(
            'ALTER TABLE "meridian_test"."people" ALTER COLUMN "age" TYPE bigint'
        )
        with pytest.raises(RuntimeError, match="column drift"):
            ProbeService(settings, engine_version=selected_engine_version()).verify_physical(
                connection,
                physical_resources(settings),
            )

    schema = people_schema()
    collection = CollectionDocument(
        ResourceReference.parse("structured:example.people"),
        schema.ref,
        "relational",
    )
    semantics = runtime.semantics
    semantics.validate_definition(schema, collection)
    activation = semantics.plan_activation(collection, None, schema)
    activation_result = semantics.apply_activation(activation)
    assert activation_result.plan_fingerprint == activation.fingerprint
    assert activation_result.physical_fingerprint == plan.physical_fingerprint
    assert activation_result.registry_revision == semantics.read_registry_revision()

    catalog, surface = provider()
    person_id = str(uuid.uuid4())
    execute(
        runtime,
        catalog.normalize(
            surface.put(
                resource="example.people",
                data={
                    "id": person_id,
                    "name": "Transfer",
                    "document": {"portable": True},
                    "location": {"longitude": 12.5, "latitude": 41.9},
                    "payload": "AQID",
                },
            )
        ),
        operation_context,
        "transfer-source",
    )
    transfer = LogicalTransfer(settings)
    with connect(postgresql_dsn) as connection:
        lines = list(
            transfer.export_rows(
                connection,
                ["structured:example.people"],
                tenant="tenant-a",
                scope={"workspace": "workspace-a"},
            )
        )
        assert len(lines) == 1
        assert (
            transfer.import_rows(
                connection,
                lines,
                tenant="tenant-a",
                scope={"workspace": "workspace-b"},
            )
            == 1
        )

    with bind_context(operation_context):
        exported = semantics.export_logical(
            (ResourceReference.parse("structured:example.people"),)
        )
    assert exported["registryRevision"] == semantics.read_registry_revision()
    assert len(exported["records"]) == 1
    destination_context = replace(
        operation_context,
        request_id="logical-import",
        scope={"workspace": "workspace-c"},
    )
    with bind_context(destination_context):
        semantics.import_logical(exported)

    get = catalog.normalize(surface.get(resource="example.people", where={"id": person_id}))
    report = run_adapter_conformance(
        AdapterConformanceTarget(
            factory=PostgreSQLAdapterFactory(),
            create_context=create_context,
            resources=physical_resources(settings),
            operation=get,
            context=operation_context,
            assert_result=lambda result: (
                None
                if isinstance(result.data, Mapping) and result.data["id"] == person_id
                else (_ for _ in ()).throw(AssertionError("unexpected conformance result"))
            ),
        )
    )
    assert "transaction-commit-rollback" in report.checks


def test_additive_migration_updates_existing_physical_layout(
    postgresql_dsn: str,
) -> None:
    raw_target = sample_settings_mapping()
    raw_initial = deepcopy(raw_target)
    people = raw_initial["resources"][0]
    people["fields"] = [field for field in people["fields"] if field["name"] != "age"]
    people["schemaFingerprint"] = fp("people-schema-before-age")
    people["resourceFingerprint"] = fp("people-resource-before-age")

    def settings_from(value: dict[str, object]) -> PostgreSQLSettings:
        binding = type(
            "Binding",
            (),
            {
                "engine_profile": "postgresql-postgis-local-single-primary",
                "settings": value,
                "physical_namespace": "meridian_migration_test",
                "tls": type("TLS", (), {"mode": "disabled"})(),
            },
        )()
        return PostgreSQLSettings.from_binding(binding)

    initial = settings_from(raw_initial)
    target = settings_from(raw_target)
    initial_plan = SchemaCompiler(initial).compile()
    target_plan = SchemaCompiler(target).compile()
    assert initial_plan.physical_fingerprint != target_plan.physical_fingerprint

    with (
        connect(postgresql_dsn) as connection,
        connection.transaction(force_rollback=True),
    ):
        assert MigrationExecutor(initial).apply(connection, initial_plan).applied
        upgraded = MigrationExecutor(target).apply(
            connection,
            target_plan,
            expected_physical_fingerprint=initial_plan.physical_fingerprint,
        )
        assert upgraded.applied
        column = connection.execute(
            "SELECT data_type, is_nullable FROM information_schema.columns "
            "WHERE table_schema = 'meridian_migration_test' "
            "AND table_name = 'people' AND column_name = 'age'"
        ).fetchone()
        assert column == ("integer", "YES")
        verified = ProbeService(target, engine_version=selected_engine_version()).verify_physical(
            connection,
            physical_resources(target),
        )
        assert verified.fingerprint == target_plan.physical_fingerprint
