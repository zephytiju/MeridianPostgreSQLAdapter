# SPDX-License-Identifier: Apache-2.0
"""Released Catalog/Core APIs against a migrated real PostgreSQL Binding."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import ExitStack
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import pytest
from conftest import make_binding
from meridian_storage.errors import ConflictError, MeridianError
from meridian_storage.evidence.catalogs import EvidenceCatalogProvider
from meridian_storage.registry.resources import (
    NamespaceDefinition,
    ResourceBundle,
    ResourceDefinition,
    ResourceRef,
    SchemaDefinition,
    SchemaRef,
)
from meridian_storage.runtime.config import RuntimeConfig
from meridian_storage.semantics.catalogs import StructuredCatalogProvider
from meridian_storage.spi.adapters import SecretValue
from psycopg import connect, sql

from meridian_storage import Meridian, OperationContext
from meridian_storage.adapters.postgresql._settings import PostgreSQLSettings
from meridian_storage.adapters.postgresql.migration import MigrationExecutor
from meridian_storage.adapters.postgresql.schema import SchemaCompiler

pytestmark = pytest.mark.integration


def ctx(
    tenant: str = "tenant-a", workspace: str = "workspace-a", **kwargs: Any
) -> OperationContext:
    return OperationContext(
        principal_ref="test:app", tenant=tenant, scope={"workspace": workspace}, **kwargs
    )


def event(identity: str | None = None, **values: Any) -> dict[str, Any]:
    return {"id": identity or str(uuid4()), "kind": "audit", "payload": {"value": 1}, **values}


@pytest.fixture
def facade(postgresql_dsn: str) -> Iterator[Any]:
    binding, user, password = make_binding(postgresql_dsn)
    namespace = "ev_" + uuid4().hex[:12]
    layouts = [
        deepcopy(x)
        for x in binding.settings["resources"]
        if x["ref"] in {"structured:example.people", "evidence:example.outbox"}
    ]
    schemas = []
    resources = []
    for layout in layouts:
        ref = ResourceRef.parse(layout["ref"])
        if ref.catalog == "evidence":
            layout["fields"].extend(
                [
                    {
                        "name": name,
                        "column": name.lower(),
                        "logicalType": "string",
                        "nullable": True,
                        "mutable": False,
                    }
                    for name in ("evidenceId", "checkpointKey")
                ]
            )
        schema = SchemaDefinition(
            SchemaRef(ref.catalog, ref.namespace, ref.name, "1.0.0"), {"fields": layout["fields"]}
        )
        resource = ResourceDefinition(
            ref, layout["profile"], schema=schema.ref, required_scope=("workspace",)
        )
        layout.update(
            schemaFingerprint=schema.fingerprint, resourceFingerprint=resource.fingerprint
        )
        schemas.append(schema)
        resources.append(resource)
    binding = replace(
        binding, physical_namespace=namespace, settings=dict(binding.settings, resources=layouts)
    )
    settings = PostgreSQLSettings.from_binding(binding)
    plan = SchemaCompiler(settings).compile()
    binding = replace(binding, required_physical_fingerprint=plan.physical_fingerprint)
    with connect(postgresql_dsn) as connection:
        MigrationExecutor(settings).apply(connection, plan)
    bundle = ResourceBundle(
        "test.schemas",
        "1.0.0",
        "1.0.0",
        namespaces=(
            NamespaceDefinition("evidence", "example"),
            NamespaceDefinition("structured", "example"),
        ),
        schemas=tuple(schemas),
        resources=tuple(resources),
    )

    class Schemas:
        provider_id = bundle.provider_id
        provider_contract_version = "1.0.0"

        def load(self) -> ResourceBundle:
            return bundle

    class Secrets:
        def resolve(self, ref: Any) -> SecretValue:
            return SecretValue((user if ref.reference == "identity" else password).encode())

    providers = [EvidenceCatalogProvider(), StructuredCatalogProvider()]
    config = {
        "formatVersion": "meridian-config.v1",
        "profile": "conformance",
        "catalogs": {
            "providers": [
                {
                    "name": p.catalog_name,
                    "package": "meridian-storage-"
                    + ("evidence" if p.catalog_name == "evidence" else "semantics"),
                    "contract": p.manifest().catalog_contract_version,
                    "requiredFingerprint": p.manifest().fingerprint,
                }
                for p in providers
            ],
            "extensions": {},
        },
        "resources": {
            "pins": [
                {
                    "ref": r.ref.to_dict(),
                    "providerId": bundle.provider_id,
                    "requiredFingerprint": r.fingerprint,
                }
                for r in resources
            ],
            "extensions": {},
        },
        "schemas": {
            "providers": [
                {
                    "id": bundle.provider_id,
                    "package": "test-schemas",
                    "contract": "1.0.0",
                    "requiredFingerprint": bundle.fingerprint,
                }
            ],
            "live": {"enabled": False, "required": False, "providerId": None},
            "extensions": {},
        },
        "bindings": [binding.to_dict()],
        "placements": [
            {
                "id": "primary",
                "selector": {
                    "resources": [r.ref.to_dict() for r in resources],
                    "catalog": None,
                    "labels": {},
                },
                "bindingId": binding.id,
                "extensions": {},
            }
        ],
        "validation": {
            "strict": True,
            "requirePhysicalFingerprints": True,
            "defaultOperationTimeoutMs": 10000,
            "idempotencyCacheEntries": 64,
            "retry": {"maxAttempts": 1, "baseDelayMs": 0, "maxDelayMs": 0, "jitterRatio": 0},
        },
    }
    with ExitStack() as stack:

        def start(
            *,
            cross_binding: bool = False,
            replay_fault: str | None = None,
            result_limit: int | None = None,
            append_delay: bool = False,
        ) -> Meridian:
            selected = deepcopy(config)
            if append_delay:
                with connect(postgresql_dsn) as connection:
                    connection.execute(
                        sql.SQL(
                            "CREATE FUNCTION {}.delay_append() RETURNS trigger LANGUAGE plpgsql "
                            "AS $$ BEGIN PERFORM pg_sleep(0.2); RETURN NEW; END $$"
                        ).format(sql.Identifier(namespace))
                    )
                    connection.execute(
                        sql.SQL(
                            "CREATE TRIGGER delay_append BEFORE INSERT ON {}.outbox "
                            "FOR EACH ROW EXECUTE FUNCTION {}.delay_append()"
                        ).format(sql.Identifier(namespace), sql.Identifier(namespace))
                    )
            if replay_fault is not None:
                with connect(postgresql_dsn) as connection:
                    table = sql.Identifier(namespace, "__meridian_evidence_replay")
                    if replay_fault == "missing":
                        connection.execute(sql.SQL("DROP TABLE {}").format(table))
                    else:
                        connection.execute(
                            sql.SQL("ALTER TABLE {} DROP CONSTRAINT {}").format(
                                table, sql.Identifier("__meridian_evidence_replay_pkey")
                            )
                        )
            if result_limit is not None:
                selected["bindings"][0]["client"]["maxResultBytes"] = result_limit
            if cross_binding:
                other = dict(binding.to_dict(), id="second")
                selected["bindings"].append(other)
                selected["placements"] = [
                    {
                        "id": "placement-" + r.ref.catalog,
                        "selector": {"resources": [r.ref.to_dict()], "catalog": None, "labels": {}},
                        "bindingId": "second" if r.ref.catalog == "evidence" else binding.id,
                        "extensions": {},
                    }
                    for r in resources
                ]
            runtime = Meridian(
                RuntimeConfig.from_mapping(selected),
                schema_providers=[Schemas()],
                secret_resolver=Secrets(),
            )
            runtime.start()
            stack.callback(runtime.close)
            return runtime

        def seed() -> str:
            identity = str(uuid4())
            with connect(postgresql_dsn) as connection:
                connection.execute(
                    sql.SQL(
                        "INSERT INTO {}.people (__tenant, __scope_workspace, id, name) "
                        "VALUES (%s, %s, %s, %s)"
                    ).format(sql.Identifier(namespace)),
                    ("tenant-a", "workspace-a", identity, "before"),
                )
            return identity

        yield start, seed
    with connect(postgresql_dsn) as connection:
        connection.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(namespace)))


def query(runtime: Meridian, context: OperationContext, **kw: Any) -> Any:
    with runtime.context(context):
        return runtime.execute(
            runtime.catalog("evidence").query(resource="example.outbox", **kw)
        ).data


def test_scope_reads_and_core_replay_are_isolated(facade: Any) -> None:
    start, _ = facade
    runtime = start()
    evidence = runtime.catalog("evidence")
    data = event()
    contexts = [
        ctx(idempotency_key="same-key"),
        ctx(workspace="workspace-b", idempotency_key="same-key"),
        ctx(tenant="tenant-b", idempotency_key="same-key"),
    ]
    for context in contexts:
        with runtime.context(context):
            expression = evidence.append(
                resource="example.outbox", data=data, idempotency_key="same"
            )
            first = runtime.execute(expression).data
            assert runtime.execute(expression).data == first
    for context in contexts:
        result = query(runtime, context, where={"id": data["id"]})
        assert len(result["items"]) == 1
        assert result["items"][0]["id"] == data["id"]


def test_missing_and_spoofed_scope_fail(facade: Any) -> None:
    runtime = facade[0]()
    evidence = runtime.catalog("evidence")
    with pytest.raises(MeridianError):
        runtime.execute(evidence.query(resource="example.outbox"))
    invalid = [
        OperationContext(principal_ref="app"),
        OperationContext(principal_ref="app", tenant="tenant-a"),
        OperationContext(principal_ref="app", scope={"workspace": "workspace-a"}),
        OperationContext(
            principal_ref="app",
            tenant="tenant-a",
            scope={"workspace": "workspace-a", "spoof": "other"},
        ),
    ]
    for context in invalid:
        with pytest.raises(MeridianError):
            query(runtime, context)
        with runtime.context(context), pytest.raises(MeridianError):
            runtime.execute(evidence.append(resource="example.outbox", data=event()))
    with pytest.raises(MeridianError):
        query(runtime, ctx(), where={"__scope_workspace": "workspace-b"})
    with runtime.context(ctx()), pytest.raises(MeridianError):
        runtime.execute(evidence.append(resource="example.outbox", data=event(__tenant="spoof")))


def test_batch_replay_survives_runtime_restart_and_conflicts(facade: Any) -> None:
    start, _ = facade
    runtime = start()
    data = [event(), event()]
    with runtime.context(ctx()):
        expression = runtime.catalog("evidence").append(
            resource="example.outbox", data=data, idempotency_key="batch"
        )
        first = runtime.execute(expression).data
        assert [r["id"] for r in first] == [r["id"] for r in data]
        assert runtime.execute(expression).data == first
    runtime.close()
    runtime = start()
    with runtime.context(ctx()):
        assert runtime.execute(expression).data == first
        with pytest.raises(ConflictError):
            runtime.execute(
                runtime.catalog("evidence").append(
                    resource="example.outbox", data=[event()], idempotency_key="batch"
                )
            )
    assert len(query(runtime, ctx())["items"]) == 2


def test_batch_database_failure_and_validation_leave_no_partial_data(facade: Any) -> None:
    runtime = facade[0]()
    evidence = runtime.catalog("evidence")
    good = event()
    with runtime.context(ctx()):
        # First row is valid and executes; second fails in the database.
        with pytest.raises(MeridianError):
            runtime.execute(
                evidence.append(
                    resource="example.outbox",
                    data=[good, event(id="not-uuid")],
                    idempotency_key="retry",
                )
            )
        assert not query(runtime, ctx())["items"]
        # Failed replay claim rolls back with the data, so this retry may proceed.
        runtime.execute(
            evidence.append(resource="example.outbox", data=[good], idempotency_key="retry")
        )
        with pytest.raises(MeridianError):
            runtime.execute(
                evidence.append(resource="example.outbox", data=[event(), {"kind": "missing"}])
            )
    assert len(query(runtime, ctx())["items"]) == 1


@pytest.mark.parametrize(
    "failure", [None, "database", "validation", "rollback-only", "cross-binding"]
)
def test_source_audit_atomicity_and_binding_boundary(facade: Any, failure: str | None) -> None:
    start, seed = facade
    identity = seed()
    runtime = start(cross_binding=failure == "cross-binding")
    structured, evidence = runtime.catalog("structured"), runtime.catalog("evidence")
    data = (
        event(id="not-uuid")
        if failure == "database"
        else {"kind": "missing"}
        if failure == "validation"
        else event()
    )

    def execute() -> None:
        with runtime.context(ctx()), runtime.transaction("structured:example.people") as tx:
            runtime.execute(
                structured.patch(
                    resource="example.people", where={"id": identity}, changes={"name": "after"}
                )
            )
            if failure == "rollback-only":
                try:
                    runtime.execute(
                        evidence.append(
                            resource="example.outbox", data={"kind": "missing"}, require_atomic=True
                        )
                    )
                except MeridianError:
                    tx.set_rollback_only()
            else:
                runtime.execute(
                    evidence.append(resource="example.outbox", data=data, require_atomic=True)
                )

    if failure is not None and failure != "rollback-only":
        with pytest.raises(MeridianError) as caught:
            execute()
        if failure == "cross-binding":
            assert caught.value.code == "MERIDIAN_TRANSACTION_SCOPE"
    else:
        execute()
    with runtime.context(ctx()):
        row = runtime.execute(
            structured.get(resource="example.people", where={"id": identity})
        ).data
    assert row["name"] == ("after" if failure is None else "before")
    assert len(query(runtime, ctx())["items"]) == (1 if failure is None else 0)


def test_cursor_rejects_changed_scope_filter_and_tampering(facade: Any) -> None:
    runtime = facade[0]()
    with runtime.context(ctx()):
        runtime.execute(
            runtime.catalog("evidence").append(
                resource="example.outbox", data=[event(), event(), event()]
            )
        )
    first = query(runtime, ctx(), limit=1)
    cursor = first["cursor"]
    assert cursor
    second = query(runtime, ctx(), limit=1, cursor=cursor)
    assert first["items"][0]["id"] != second["items"][0]["id"]
    for context in (ctx(tenant="other"), ctx(workspace="other")):
        with pytest.raises(MeridianError):
            query(runtime, context, limit=1, cursor=cursor)
    with pytest.raises(MeridianError):
        query(runtime, ctx(), limit=1, cursor=cursor, where={"kind": "changed"})
    with pytest.raises(MeridianError):
        query(runtime, ctx(), limit=1, cursor="tampered." + cursor)


def test_explicit_atomic_requirement_needs_a_transaction(facade: Any) -> None:
    runtime = facade[0]()
    with runtime.context(ctx()), pytest.raises(MeridianError):
        runtime.execute(
            runtime.catalog("evidence").append(
                resource="example.outbox", data=event(), require_atomic=True
            )
        )
    assert not query(runtime, ctx())["items"]


@pytest.mark.parametrize("identity_field", ["evidenceId", "checkpointKey"])
def test_record_identity_replay_and_conflict(facade: Any, identity_field: str) -> None:
    runtime = facade[0]()
    evidence = runtime.catalog("evidence")
    data = [event(**{identity_field: "stable"})]
    with runtime.context(ctx()):
        expression = evidence.append(resource="example.outbox", data=data)
        first = runtime.execute(expression).data
        assert runtime.execute(expression).data == first
        with pytest.raises(ConflictError):
            runtime.execute(
                evidence.append(resource="example.outbox", data=[dict(data[0], kind="changed")])
            )
    assert len(query(runtime, ctx())["items"]) == 1


def test_concurrent_batch_replay_commits_once(facade: Any) -> None:
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier

    runtime = facade[0]()
    barrier = Barrier(2)
    expression = runtime.catalog("evidence").append(
        resource="example.outbox", data=[event(), event()], idempotency_key="race"
    )

    def append() -> Any:
        with runtime.context(ctx()):
            barrier.wait(timeout=5)
            return runtime.execute(expression).data

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(append) for _ in range(2)]
        results = [future.result(timeout=15) for future in futures]
    assert results[0] == results[1]
    assert len(query(runtime, ctx())["items"]) == 2


def test_caught_batch_failure_is_atomic_and_outer_rollback_stays_explicit(facade: Any) -> None:
    start, seed = facade
    identity = seed()
    runtime = start()
    with runtime.context(ctx()), runtime.transaction("structured:example.people"):
        runtime.execute(
            runtime.catalog("structured").patch(
                resource="example.people", where={"id": identity}, changes={"name": "after"}
            )
        )
        with pytest.raises(MeridianError):
            runtime.execute(
                runtime.catalog("evidence").append(
                    resource="example.outbox",
                    data=[event(), event(id="invalid")],
                    require_atomic=True,
                )
            )
    # No auto-audit or new outer rollback semantics: callers must propagate a
    # required failure or explicitly set rollback-only. The batch itself rolls back.
    with runtime.context(ctx()):
        row = runtime.execute(
            runtime.catalog("structured").get(resource="example.people", where={"id": identity})
        ).data
    assert row["name"] == "after"
    assert not query(runtime, ctx())["items"]


@pytest.mark.parametrize("fault", ["missing", "primary-key"])
def test_startup_rejects_missing_or_invalid_replay_migration(facade: Any, fault: str) -> None:
    with pytest.raises(MeridianError):
        facade[0](replay_fault=fault)


def test_result_limit_rolls_back_append_and_replay_claim(facade: Any) -> None:
    start, _ = facade
    runtime = start(result_limit=1024)
    with runtime.context(ctx()), pytest.raises(MeridianError):
        runtime.execute(
            runtime.catalog("evidence").append(
                resource="example.outbox",
                data=[event(payload={"value": "x" * 2000})],
                idempotency_key="limit",
            )
        )
    assert not query(runtime, ctx())["items"]
    with runtime.context(ctx()):
        runtime.execute(
            runtime.catalog("evidence").append(
                resource="example.outbox", data=[event()], idempotency_key="limit"
            )
        )
    assert len(query(runtime, ctx())["items"]) == 1


def test_outer_rollback_discards_successful_append_replay_claim(facade: Any) -> None:
    runtime = facade[0]()
    expression = runtime.catalog("evidence").append(
        resource="example.outbox", data=[event()], require_atomic=True, idempotency_key="rollback"
    )
    with runtime.context(ctx()):
        with pytest.raises(RuntimeError), runtime.transaction("evidence:example.outbox"):
            runtime.execute(expression)
            raise RuntimeError("force caller rollback")
        assert not query(runtime, ctx())["items"]
        with runtime.transaction("evidence:example.outbox"):
            runtime.execute(expression)
    assert len(query(runtime, ctx())["items"]) == 1


def test_source_and_audit_stay_provisional_until_outer_commit(facade: Any) -> None:
    from concurrent.futures import ThreadPoolExecutor

    start, seed = facade
    identity = seed()
    runtime = start()
    structured = runtime.catalog("structured")

    def observe() -> tuple[str, int]:
        with runtime.context(ctx()):
            row = runtime.execute(
                structured.get(resource="example.people", where={"id": identity})
            ).data
            return row["name"], len(query(runtime, ctx())["items"])

    with ThreadPoolExecutor(max_workers=1) as executor:
        with runtime.context(ctx()), runtime.transaction("structured:example.people"):
            runtime.execute(
                structured.patch(
                    resource="example.people", where={"id": identity}, changes={"name": "after"}
                )
            )
            runtime.execute(
                runtime.catalog("evidence").append(
                    resource="example.outbox", data=event(), require_atomic=True
                )
            )
            assert executor.submit(observe).result(timeout=10) == ("before", 0)
        assert executor.submit(observe).result(timeout=10) == ("after", 1)


def test_batch_shares_one_deadline_and_rolls_back_replay_claim(facade: Any) -> None:
    runtime = facade[0](append_delay=True)
    data = [event(), event(), event()]
    expression = runtime.catalog("evidence").append(
        resource="example.outbox", data=data, idempotency_key="deadline-batch"
    )
    with (
        runtime.context(ctx(deadline=datetime.now(UTC) + timedelta(milliseconds=350))),
        pytest.raises(MeridianError) as raised,
    ):
        runtime.execute(expression)
    assert raised.value.code == "MERIDIAN_DEADLINE_EXCEEDED"
    assert not query(runtime, ctx())["items"]
    # A fresh operation can claim the same key after the timed-out batch rolled back.
    with runtime.context(ctx()):
        assert len(runtime.execute(expression).data) == len(data)
