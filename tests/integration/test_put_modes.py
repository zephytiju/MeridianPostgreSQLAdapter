# SPDX-License-Identifier: Apache-2.0
"""Released Catalog/Core contracts executed through public APIs on real PostgreSQL."""

from __future__ import annotations

import json
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from threading import Barrier
from types import SimpleNamespace

import pytest
from conftest import make_binding
from meridian_storage.errors import ConflictError, ValidationError
from meridian_storage.registry import (
    NamespaceDefinition,
    ResourceBundle,
    ResourceDefinition,
    SchemaDefinition,
    SchemaRef,
)
from meridian_storage.runtime.config import (
    CatalogConfig,
    CatalogsConfig,
    LiveSchemaConfig,
    PlacementRule,
    PlacementSelector,
    ResourcePin,
    ResourcesConfig,
    RetryPolicy,
    RuntimeConfig,
    SchemaConfig,
    SchemaProviderConfig,
    ValidationConfig,
)
from meridian_storage.semantics import StructuredCatalogProvider
from meridian_storage.spi import SecretValue
from psycopg import connect, sql

from meridian_storage import Meridian, OperationContext, ResourceRef
from meridian_storage.adapters.postgresql._settings import PostgreSQLSettings
from meridian_storage.adapters.postgresql.migration import MigrationExecutor
from meridian_storage.adapters.postgresql.schema import SchemaCompiler

pytestmark = pytest.mark.integration
FIXTURE = json.loads(
    (Path(__file__).parents[2] / "contracts/conformance/structured-put.v2.json").read_text()
)


@pytest.fixture(scope="module")
def public_runtime(postgresql_dsn):
    """Provision only this fixture's namespace; discovery loads installed packages."""
    namespace = "put_modes_" + uuid.uuid4().hex[:12]
    catalog = StructuredCatalogProvider()
    schemas, resources, layouts = [], [], []
    for name, fields in (
        (
            "records",
            [
                {"name": "id", "logicalType": "string", "nullable": False, "mutable": False},
                {"name": "value", "logicalType": "string", "nullable": False, "mutable": True},
                {"name": "optional", "logicalType": "string", "nullable": True, "mutable": True},
            ],
        ),
        (
            "immutable",
            [
                {"name": "id", "logicalType": "string", "nullable": False, "mutable": False},
                {"name": "value", "logicalType": "string", "nullable": False, "mutable": False},
            ],
        ),
        *(
            (
                "timestamps_" + str(mask),
                [
                    {"name": "id", "logicalType": "string", "nullable": False, "mutable": False},
                    {"name": "value", "logicalType": "string", "nullable": False, "mutable": True},
                    *[
                        {
                            "name": name,
                            "logicalType": "utcTimestamp",
                            "nullable": True,
                            "mutable": name != "createdAt",
                        }
                        for bit, name in enumerate(("createdAt", "updatedAt"))
                        if mask & (1 << bit)
                    ],
                ],
            )
            for mask in range(4)
        ),
    ):
        schema = SchemaDefinition(
            SchemaRef("structured", "puttest", name, "1.0.0"),
            {"fields": fields, "identity": ["id"]},
        )
        resource = ResourceDefinition(
            ResourceRef("structured", "puttest", name),
            "relational",
            schema=schema.ref,
            required_scope=("workspace", "project"),
        )
        schemas.append(schema)
        resources.append(resource)
        layouts.append(
            {
                "ref": resource.ref.canonical,
                "table": name,
                "profile": "relational",
                "schemaFingerprint": schema.fingerprint,
                "resourceFingerprint": resource.fingerprint,
                "fields": [{**field, "column": field["name"].lower()} for field in fields],
                "identity": ["id"],
                "indexes": [],
                "relation": None,
            }
        )
    bundle = ResourceBundle(
        "puttest.schemas",
        "1.0.0",
        "1.0.0",
        namespaces=(NamespaceDefinition("structured", "puttest"),),
        schemas=tuple(schemas),
        resources=tuple(resources),
    )
    binding, user, password = make_binding(postgresql_dsn)
    binding = replace(
        binding,
        physical_namespace=namespace,
        settings={
            "formatVersion": "meridian.postgresql.settings.v1",
            "scopeKeys": ["workspace", "project"],
            "topology": dict(binding.settings["topology"]),
            "resources": layouts,
        },
    )
    settings = PostgreSQLSettings.from_binding(binding)
    plan = SchemaCompiler(settings).compile()
    binding = replace(binding, required_physical_fingerprint=plan.physical_fingerprint)
    config = RuntimeConfig(
        profile="put-mode-conformance",
        catalogs=CatalogsConfig(
            (
                CatalogConfig(
                    "structured",
                    catalog.manifest().package_name,
                    "2.x",
                    catalog.manifest().fingerprint,
                ),
            )
        ),
        resources=ResourcesConfig(
            tuple(ResourcePin(r.ref, bundle.provider_id, r.fingerprint) for r in resources)
        ),
        schemas=SchemaConfig(
            (
                SchemaProviderConfig(
                    bundle.provider_id, "puttest-fixture", "1.x", bundle.fingerprint
                ),
            ),
            LiveSchemaConfig(False, False, None),
        ),
        bindings=(binding,),
        placements=(
            PlacementRule(
                "puttest",
                PlacementSelector(tuple(r.ref for r in resources), None, {}),
                binding.id,
                {},
            ),
        ),
        validation=ValidationConfig(True, True, 10_000, 128, RetryPolicy(1, 0, 0, 0)),
    )
    provider = SimpleNamespace(
        provider_id=bundle.provider_id,
        provider_contract_version="1.0.0",
        load=lambda: bundle,
    )
    resolver = SimpleNamespace(
        resolve=lambda ref: SecretValue(
            (user if ref.reference == "identity" else password).encode()
        )
    )
    runtime = Meridian(config, schema_providers=(provider,), secret_resolver=resolver)
    try:
        with connect(postgresql_dsn) as connection:
            MigrationExecutor(settings).apply(connection, plan)
            # Replay the consumer's exact logical/system timestamp pair on creates.
            for mask in range(4):
                connection.execute(
                    sql.SQL(
                        "ALTER TABLE {} ALTER COLUMN __created_at SET DEFAULT "
                        "'2026-09-07T02:32:25.706010Z'::timestamptz"
                    ).format(sql.Identifier(namespace, "timestamps_" + str(mask)))
                )
        runtime.start()
        yield runtime
    finally:
        runtime.close()
        with connect(postgresql_dsn, autocommit=True) as connection:
            connection.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(namespace))
            )


def context(**changes):
    return replace(
        OperationContext(
            principal_ref="test:put-modes",
            request_id=uuid.uuid4().hex,
            tenant="tenant-a",
            scope={"workspace": "workspace-a", "project": "project-a"},
        ),
        **changes,
    )


def put(
    runtime,
    record_id,
    *,
    mode="if_absent",
    version=None,
    value="new",
    ctx=None,
    optional=None,
    include_optional=False,
    resource="puttest.records",
):
    data = {"id": record_id, "value": value}
    if include_optional:
        data["optional"] = optional
    with runtime.context(ctx or context()):
        return runtime.execute(
            runtime.catalog("structured").put(
                resource=resource,
                data=data,
                mode=mode,
                expected_version=version,
            )
        ).data


def get(runtime, record_id, ctx=None, resource="puttest.records"):
    with runtime.context(ctx or context()):
        return runtime.execute(
            runtime.catalog("structured").get(
                resource=resource,
                where={"id": record_id},
            )
        ).data


@pytest.mark.parametrize("case", FIXTURE["existenceCases"], ids=lambda c: c["name"])
def test_released_existence_matrix(public_runtime, case):
    runtime = public_runtime
    record_id = uuid.uuid4().hex
    before = None
    if case["recordPresent"]:
        before = put(runtime, record_id, value="original")
        assert before["recordVersion"] == case["currentVersion"]
    if case["expectedOutcome"] == "conflict":
        with pytest.raises(ConflictError):
            put(runtime, record_id, mode=case["mode"], version=case["expectedVersion"])
        assert get(runtime, record_id) == before
    else:
        after = put(runtime, record_id, mode=case["mode"], version=case["expectedVersion"])
        assert after["recordVersion"] == (2 if before else 1)
        assert after["value"] == "new"
        assert get(runtime, record_id) == after
        if before:
            assert after["createdAt"] == before["createdAt"]


@pytest.mark.parametrize("mode", ["update", "upsert"])
@pytest.mark.parametrize("version", [None, 1])
def test_existing_field_and_version_behavior(public_runtime, mode, version):
    key = uuid.uuid4().hex
    before = put(public_runtime, key, optional="keep", include_optional=True)
    after = put(public_runtime, key, mode=mode, version=version, value="changed")
    assert after["optional"] == ("keep" if version is not None else None)
    assert after["recordVersion"] == 2
    assert after["createdAt"] == before["createdAt"]
    # Explicit null retains its existing meaning on every update path.
    final = put(
        public_runtime, key, mode=mode, version=2 if version else None, include_optional=True
    )
    assert final["optional"] is None and final["recordVersion"] == 3


@pytest.mark.parametrize("mode", ["update", "upsert"])
def test_immutable_only_put_retains_values_and_version(public_runtime, mode):
    key = uuid.uuid4().hex
    before = put(public_runtime, key, resource="puttest.immutable", value="original")
    after = put(public_runtime, key, mode=mode, resource="puttest.immutable", value="ignored")
    assert after == before


def test_concurrent_creates_have_one_winner(public_runtime):
    key, barrier = uuid.uuid4().hex, Barrier(4)

    def create(_):
        barrier.wait(timeout=10)
        try:
            return put(public_runtime, key)
        except ConflictError:
            return None

    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(create, range(4)))
    winners = [result for result in results if result is not None]
    assert len(winners) == 1
    assert get(public_runtime, key) == winners[0]
    assert winners[0]["recordVersion"] == 1


@pytest.mark.parametrize("version", [None, 1])
def test_concurrent_upserts_are_atomic(public_runtime, version):
    key, barrier = uuid.uuid4().hex, Barrier(4)
    if version is not None:
        put(public_runtime, key)

    def update(index):
        barrier.wait(timeout=10)
        try:
            return put(public_runtime, key, mode="upsert", version=version, value=str(index))
        except ConflictError:
            return None

    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(update, range(4)))
    versions = sorted(result["recordVersion"] for result in results if result is not None)
    assert versions == ([1, 2, 3, 4] if version is None else [2])
    assert get(public_runtime, key)["recordVersion"] == versions[-1]


@pytest.mark.parametrize("mode", ["if_absent", "update", "upsert"])
def test_modes_preserve_tenant_and_each_scope_dimension(public_runtime, mode):
    key = uuid.uuid4().hex
    contexts = [
        context(),
        context(tenant="tenant-b"),
        context(scope={"workspace": "workspace-b", "project": "project-a"}),
        context(scope={"workspace": "workspace-a", "project": "project-b"}),
    ]
    originals = [put(public_runtime, key, value=str(i), ctx=ctx) for i, ctx in enumerate(contexts)]
    if mode == "if_absent":
        with pytest.raises(ConflictError):
            put(public_runtime, key, ctx=contexts[0])
    else:
        originals[0] = put(public_runtime, key, mode=mode, version=1, ctx=contexts[0])
    assert [get(public_runtime, key, ctx=ctx) for ctx in contexts] == originals
    with pytest.raises(ValidationError):
        put(public_runtime, key, mode=mode, ctx=context(scope={"workspace": "workspace-a"}))


def test_recognized_replay_returns_original_after_later_update(public_runtime):
    key, replay_key = uuid.uuid4().hex, uuid.uuid4().hex
    original = put(public_runtime, key, ctx=context(idempotency_key=replay_key))
    updated = put(public_runtime, key, mode="update", version=1, value="later")
    replay = put(public_runtime, key, ctx=context(idempotency_key=replay_key))
    assert replay == original and get(public_runtime, key) == updated
    with pytest.raises(ConflictError):
        put(public_runtime, key, ctx=context(idempotency_key=uuid.uuid4().hex))
    # Mode is part of the fingerprint: reusing a key cannot turn create into update.
    with pytest.raises(ConflictError):
        put(public_runtime, key, mode="upsert", ctx=context(idempotency_key=replay_key))
    assert get(public_runtime, key) == updated


def test_unsupported_contracts_fail_before_writes(public_runtime):
    from meridian_storage import Expression, Operation

    key = uuid.uuid4().hex
    provider = StructuredCatalogProvider()
    operation = provider.normalize(
        provider.create_surface().put(
            resource="puttest.records",
            data={"id": key, "value": "must-not-exist"},
        )
    )
    # The Adapter SPI must also fail closed if an incompatible caller bypasses
    # Catalog normalization. Each rejected request leaves the real table empty.
    from meridian_storage.spi.adapters import ExecutionRequest

    adapter = public_runtime._adapter_runtimes["postgresql-test"]
    invalid = [replace(operation, operation_version=v) for v in ("1.0.0", "3.0.0")]
    raw = operation.to_dict()
    del raw["input"]["mode"]
    invalid.append(Operation.from_mapping(raw))
    raw = operation.to_dict()
    raw["input"]["queryPlan"] = {}
    invalid.append(Operation.from_mapping(raw))
    for candidate in invalid:
        session = adapter.open_session(transactional=False)
        try:
            with pytest.raises(ValidationError):
                session.execute(
                    ExecutionRequest(
                        operation=candidate,
                        context=context(),
                        request_id=uuid.uuid4().hex,
                        execution_id=uuid.uuid4().hex,
                        binding_id="postgresql-test",
                        registry_revision=1,
                        registry_fingerprint="sha256:" + "0" * 64,
                        attempt=1,
                    )
                )
        finally:
            session.close()
        assert get(public_runtime, key) is None
    with public_runtime.context(context()), pytest.raises(ValidationError):
        public_runtime.execute(
            Expression(
                "structured",
                "put",
                {
                    "resource": "puttest.records",
                    "data": {"id": key, "value": "must-not-exist"},
                },
            )
        )
    assert get(public_runtime, key) is None


@pytest.mark.parametrize("mask", range(4))
@pytest.mark.parametrize(
    "mode,existing,version",
    [
        ("if_absent", False, None),
        ("upsert", False, None),
        ("update", True, None),
        ("update", True, 1),
        ("upsert", True, None),
        ("upsert", True, 1),
    ],
)
def test_logical_timestamp_round_trips(public_runtime, mask, mode, existing, version):
    """Logical immutable provenance time must survive all public write/read paths."""
    runtime = public_runtime
    catalog = runtime.catalog("structured")
    resource = "puttest.timestamps_" + str(mask)
    data = {"id": uuid.uuid4().hex, "value": "original"}
    logical = {
        name: value
        for bit, (name, value) in enumerate(
            (
                ("createdAt", "2026-09-07T02:32:25.701304Z"),
                ("updatedAt", "2025-01-02T03:04:05.123456Z"),
            )
        )
        if mask & (1 << bit)
    }
    data.update(logical)
    with runtime.context(context()):
        before = None
        if existing:
            before = runtime.execute(catalog.put(resource=resource, data=data)).data
        data["value"] = "changed"
        if existing and "updatedAt" in logical:
            logical["updatedAt"] = "2025-02-03T04:05:06.234567Z"
            data.update(logical)
        result = runtime.execute(
            catalog.put(
                resource=resource,
                data={
                    k: v for k, v in data.items() if not (version is not None and k == "createdAt")
                },
                mode=mode,
                expected_version=version,
            )
        ).data
        assert result["recordVersion"] == (2 if existing else 1)
        assert {name: result[name] for name in data} == data
        assert set(result) == set(data) | {"createdAt", "updatedAt", "recordVersion"}
        for name in ("createdAt", "updatedAt"):
            if name not in logical:
                assert result[name].endswith("Z")
                if name == "createdAt":
                    assert result[name] == "2026-09-07T02:32:25.706010Z"
                assert result[name] not in logical.values()
        if before:
            assert result["createdAt"] == before["createdAt"]
        assert (
            runtime.execute(catalog.get(resource=resource, where={"id": data["id"]})).data == result
        )
        rows = runtime.execute(catalog.query(resource=resource, where={"id": data["id"]})).data
        assert list(rows["items"]) == [result]
        projected = runtime.execute(
            catalog.query(
                resource=resource,
                where={"id": data["id"]},
                select=list(data),
            )
        ).data
        assert list(projected["items"]) == [data]


@pytest.mark.parametrize("mask", [1, 2, 3])
def test_null_logical_timestamps_are_not_system_metadata(public_runtime, mask):
    runtime = public_runtime
    resource = "puttest.timestamps_" + str(mask)
    data = {"id": uuid.uuid4().hex, "value": "nullable"}
    data.update(
        {name: None for bit, name in enumerate(("createdAt", "updatedAt")) if mask & (1 << bit)}
    )
    with runtime.context(context()):
        catalog = runtime.catalog("structured")
        result = runtime.execute(catalog.put(resource=resource, data=data)).data
        assert {name: result[name] for name in data} == data
        assert (
            runtime.execute(catalog.get(resource=resource, where={"id": data["id"]})).data == result
        )
