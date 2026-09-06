# SPDX-License-Identifier: Apache-2.0
"""Installed lifecycle fixtures and public Writer/Runner against real PostgreSQL."""

from __future__ import annotations

import json
import subprocess
import sys
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Barrier
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import pytest
from conftest import make_binding
from meridian_storage.projection import (
    OutboxDataV1,
    ProjectionRunner,
    ProjectionSpec,
    TransactionalOutboxWriter,
)
from meridian_storage.projection.testing import OutboxConformanceTarget, run_outbox_conformance
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
from meridian_storage.spi.adapters import AdapterCreateContext, SecretValue
from psycopg import connect, sql

from meridian_storage import Meridian, MeridianError, OperationContext
from meridian_storage.adapters.postgresql import PostgreSQLAdapterFactory, PostgreSQLOutbox
from meridian_storage.adapters.postgresql._settings import PostgreSQLSettings
from meridian_storage.adapters.postgresql.migration import MigrationExecutor
from meridian_storage.adapters.postgresql.projection._storage import INTENT_FIELDS
from meridian_storage.adapters.postgresql.schema import SchemaCompiler

pytestmark = pytest.mark.integration
NOW = datetime(2026, 1, 1, tzinfo=UTC)
FP = "sha256:" + "a" * 64


def ctx(tenant: str = "a", workspace: str = "a") -> OperationContext:
    return OperationContext(
        principal_ref="test:host", tenant=tenant, scope={"workspace": workspace}
    )


def intent(name: str = "event", **kw: Any) -> OutboxDataV1:
    return OutboxDataV1(
        source_resource="example.source",
        source_schema="example.source@1.0.0",
        source_identity=kw.pop("source_identity", name),
        event_id=name,
        source_version=kw.pop("source_version", 1),
        mutation_kind="put",
        payload=kw.pop("payload", {"id": name}),
        occurred_at=kw.pop("occurred_at", NOW),
        **kw,
    )


@pytest.fixture
def durable(postgresql_dsn: str) -> Iterator[Any]:
    binding, user, password = make_binding(postgresql_dsn)
    namespace = "ob_" + uuid4().hex[:12]
    layouts, schemas, resources = [], [], []
    for name in ("source", "target", "outbox", "other_outbox"):
        fields = (
            [
                {
                    "name": n,
                    "column": n.lower(),
                    "logicalType": kind,
                    "nullable": nullable,
                    "mutable": False,
                }
                for n, (kind, nullable) in INTENT_FIELDS.items()
            ]
            if "outbox" in name
            else [
                {
                    "name": "id",
                    "column": "id",
                    "logicalType": "string",
                    "nullable": False,
                    "mutable": False,
                },
                {
                    "name": "name",
                    "column": "name",
                    "logicalType": "string",
                    "nullable": True,
                    "mutable": True,
                },
                {
                    "name": "sourceVersion",
                    "column": "source_version",
                    "logicalType": "json",
                    "nullable": True,
                    "mutable": True,
                },
            ]
        )
        identity = ["eventId"] if "outbox" in name else ["id"]
        schema = SchemaDefinition(
            SchemaRef("structured", "example", name, "1.0.0"),
            {"semanticKind": "relational", "fields": fields, "identity": identity},
        )
        ref = ResourceRef.parse("example." + name, catalog="structured")
        resource = ResourceDefinition(
            ref, "relational", schema=schema.ref, required_scope=("workspace",)
        )
        layouts.append(
            {
                "ref": ref.canonical,
                "table": name,
                "profile": "relational",
                "schemaFingerprint": schema.fingerprint,
                "resourceFingerprint": resource.fingerprint,
                "fields": fields,
                "identity": identity,
                "indexes": [],
                "relation": None,
            }
        )
        schemas.append(schema)
        resources.append(resource)
    binding = replace(
        binding, physical_namespace=namespace, settings={**binding.settings, "resources": layouts}
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
        namespaces=(NamespaceDefinition("structured", "example"),),
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

    providers = [StructuredCatalogProvider()]
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
    spec = ProjectionSpec(
        name="example-projection",
        source_catalog="structured",
        source="example.source",
        source_schema="example.source@1.0.0",
        target_catalog="structured",
        target="example.target",
        target_schema="example.target@1.0.0",
    )
    contexts, runtimes = [], []
    h = SimpleNamespace(spec=spec, namespace=namespace, dsn=postgresql_dsn)

    def start() -> None:
        h.meridian = Meridian(
            RuntimeConfig.from_mapping(config),
            schema_providers=[Schemas()],
            secret_resolver=Secrets(),
        )
        h.meridian.start()
        contexts.append(h.meridian)
        h.runtime = PostgreSQLAdapterFactory().create(
            AdapterCreateContext(
                binding=binding,
                identity=SecretValue(user.encode()),
                credential=SecretValue(password.encode()),
            )
        )
        h.runtime.open()
        runtimes.append(h.runtime)

    def port(context: OperationContext | None = None, **kw: Any) -> PostgreSQLOutbox:
        return PostgreSQLOutbox(
            h.runtime,
            resource=kw.pop("resource", "example.outbox"),
            spec=kw.pop("spec", spec),
            context=context or ctx(),
            poison_threshold=2,
            **kw,
        )

    def reopen() -> PostgreSQLOutbox:
        h.meridian.close()
        h.runtime.close()
        start()
        return port()

    def seed(
        data: OutboxDataV1,
        context: OperationContext | None = None,
        resource: str = "example.outbox",
    ) -> None:
        with h.meridian.context(context or ctx()):
            h.meridian.execute(
                h.meridian.catalog("structured").put(
                    resource=resource, data=data.to_mapping(), mode="if_absent"
                )
            )

    h.port, h.reopen, h.seed = port, reopen, seed
    h.child_config = {
        "config": config,
        "bundle": bundle.to_dict(),
        "spec": asdict(spec),
        "binding": binding.to_dict(),
        "identity": user,
        "credential": password,
    }
    start()
    try:
        yield h
    finally:
        for runtime in (*contexts, *runtimes):
            runtime.close()
        with connect(postgresql_dsn) as connection:
            connection.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(namespace)))


def claim(port: PostgreSQLOutbox, owner: str = "owner", **kw: Any) -> Any:
    return port.atomic_claim(
        owner=owner,
        limit=kw.pop("limit", 10),
        lease_duration=timedelta(seconds=10),
        now=kw.pop("now", NOW),
        **kw,
    )


def complete(port: PostgreSQLOutbox, data: OutboxDataV1, owner: str = "owner", **kw: Any) -> Any:
    return port.complete(
        data.event_id,
        owner=owner,
        acknowledged_source_version=kw.pop("version", data.source_version),
        target_fingerprint=FP,
        now=kw.pop("now", NOW),
        **kw,
    )


def test_released_shared_lifecycle_conformance(durable: Any) -> None:
    report = run_outbox_conformance(
        OutboxConformanceTarget(
            outbox=durable.port(),
            seed=durable.seed,
            inspect_record=lambda event: durable.port().get(event),
            checkpoint=lambda partition: durable.port().checkpoint(partition),
            reopen=durable.reopen,
            source_resource=durable.spec.source,
            source_schema=durable.spec.source_schema,
        )
    )
    assert len(report.checks) == 5
    assert (
        report.same_owner_completion
        == report.same_owner_release
        == "accepted-indistinguishable-owner"
    )


def test_real_concurrent_claimers_and_completers(durable: Any) -> None:
    for i in range(24):
        durable.seed(intent(str(i)))
        durable.seed(intent(f"{i}-next", source_identity=str(i), source_version=2))
    barrier = Barrier(4)

    def worker(n: int) -> Any:
        port = durable.port()
        barrier.wait(timeout=5)
        return claim(port, str(n), limit=8)

    with ThreadPoolExecutor(max_workers=4) as executor:
        batches = list(executor.map(worker, range(4)))
    assert all(len(batch) <= 8 for batch in batches)
    records = [r for batch in batches for r in batch]
    assert len(records) == len({r.data.event_id for r in records})
    # SKIP LOCKED snapshots can select a row whose competing claim commits
    # before the row lock is acquired. The conditional state write rejects it
    # and may return a short batch. Subsequent calls must retain all remaining
    # intents, while never returning an already live lease or a later version.
    for _ in range(3):
        remaining = claim(durable.port(), "drain", limit=8)
        assert len(remaining) <= 8
        records.extend(remaining)
        if not remaining:
            break
    ids = [r.data.event_id for r in records]
    assert len(ids) == len(set(ids)) == 24
    assert all(r.data.source_version == 1 for r in records)
    assert not claim(durable.port(), "extra")
    one = records[0]
    barrier = Barrier(2)

    def finish() -> Any:
        barrier.wait(timeout=5)
        try:
            return complete(durable.port(), one.data, one.lease.owner)
        except MeridianError as error:
            return error.code

    with ThreadPoolExecutor(max_workers=2) as executor:
        values = list(executor.map(lambda _: finish(), range(2)))
    assert sum(getattr(v, "revision", None) == 1 for v in values) == 1
    assert "MERIDIAN_OUTBOX_LEASE_LOST" in values
    assert durable.port().checkpoint(one.data.partition_key).revision == 1


def test_scope_resource_projection_and_selection_isolation(durable: Any) -> None:
    data = intent()
    for context in (ctx(), ctx("b"), ctx(workspace="b")):
        durable.seed(data, context)
    durable.seed(data, resource="example.other_outbox")
    ports = [
        durable.port(),
        durable.port(ctx("b")),
        durable.port(ctx(workspace="b")),
        durable.port(resource="example.other_outbox"),
        durable.port(spec=replace(durable.spec, name="other")),
    ]
    assert all(claim(p)[0].data == data for p in ports)
    complete(ports[0], data)
    assert ports[0].lag(now=NOW).incomplete_count == 0
    assert all(p.checkpoint(data.partition_key).revision == 0 for p in ports[1:])
    assert all(p.lag(now=NOW).incomplete_count == 1 for p in ports[1:])
    absent = durable.port(ctx("absent"))
    assert not claim(absent)
    with pytest.raises(MeridianError, match="not found"):
        complete(absent, data)
    labels = durable.port(spec=replace(durable.spec, target_labels=("required",)))
    assert not claim(labels)


def test_numeric_source_versions_precede_occurrence_order(durable: Any) -> None:
    older = intent("earlier-version", source_version=2, occurred_at=NOW + timedelta(seconds=1))
    newer = intent("later-version", source_identity=older.source_identity, source_version=10)
    durable.seed(newer)
    durable.seed(older)
    assert claim(durable.port())[0].data == older
    complete(durable.port(), older)
    assert claim(durable.port())[0].data == newer
    assert complete(durable.port(), newer).revision == 2


@pytest.mark.parametrize("version", [None, "opaque-v1", 1])
def test_exact_version_roundtrip(durable: Any, version: Any) -> None:
    data = intent(source_version=version)
    durable.seed(data)
    assert claim(durable.port())[0].data == data
    assert complete(durable.port(), data).source_version == version
    assert durable.reopen().checkpoint(data.partition_key).source_version == version


@pytest.mark.parametrize(
    "failure", ["mismatch", "duplicate", "intent-write", "outer-rollback", None]
)
def test_public_writer_atomicity_and_immutable_progress(durable: Any, failure: str | None) -> None:
    m = durable.meridian
    data = intent(payload={"id": "event", "name": "after"})
    if failure == "duplicate":
        durable.seed(data)
        claim(durable.port())
        before = durable.port().get(data.event_id)
    if failure == "intent-write":
        with connect(durable.dsn) as connection:
            connection.execute(
                sql.SQL("ALTER TABLE {}.outbox ADD CHECK (false) NOT VALID").format(
                    sql.Identifier(durable.namespace)
                )
            )
    with m.context(ctx()):
        mutation = m.catalog("structured").put(
            resource="example.source", data={"id": "event", "name": "after"}, mode="if_absent"
        )

        def write() -> None:
            writer = TransactionalOutboxWriter(m, outbox_resource="example.outbox")
            with m.transaction("structured:example.source"):
                writer.commit(
                    mutation,
                    replace(data, source_version=9, digest="") if failure == "mismatch" else data,
                )
                if failure == "outer-rollback":
                    raise RuntimeError("crash before commit")

        if failure:
            with pytest.raises((MeridianError, RuntimeError)):
                write()
            assert (
                m.execute(
                    m.catalog("structured").get(resource="example.source", where={"id": "event"})
                ).data
                is None
            )
        else:
            write()
            assert (
                m.execute(
                    m.catalog("structured").get(resource="example.source", where={"id": "event"})
                ).data["name"]
                == "after"
            )
    if failure == "duplicate":
        assert durable.reopen().get(data.event_id) == before
    else:
        assert durable.reopen().lag(now=NOW).incomplete_count == (0 if failure else 1)


@pytest.mark.parametrize("point", ["claim", "target-acknowledgement", "checkpoint-write"])
def test_crash_recovery_and_idempotent_target_replay(durable: Any, point: str) -> None:
    data = intent()
    durable.seed(data)
    port = durable.port()
    claim(port)
    m = durable.meridian

    def target() -> Any:
        with m.context(ctx()):
            return m.execute(
                m.catalog("structured").put(
                    resource="example.target",
                    data={"id": "event", "sourceVersion": 1},
                    mode="upsert",
                )
            )

    if point != "claim":
        target()
    if point == "checkpoint-write":
        # Fail after checkpoint persistence but before state completion. Both
        # statements must roll back, retaining the live claim and zero progress.
        with connect(durable.dsn) as connection:
            connection.execute(
                sql.SQL(
                    "ALTER TABLE {}.__meridian_outbox_state ADD CONSTRAINT "
                    "fail_completion CHECK (state <> 'COMPLETED') NOT VALID"
                ).format(sql.Identifier(durable.namespace))
            )
        with pytest.raises(MeridianError, match="constraint rejected"):
            complete(port, data)
        assert port.checkpoint(data.partition_key).revision == 0
        assert port.get(data.event_id).state.value == "LEASED"
        with connect(durable.dsn) as connection:
            connection.execute(
                sql.SQL(
                    "ALTER TABLE {}.__meridian_outbox_state DROP CONSTRAINT fail_completion"
                ).format(sql.Identifier(durable.namespace))
            )
    reopened = durable.reopen()
    m = durable.meridian
    clock = NOW + timedelta(seconds=11)
    runner = ProjectionRunner(
        meridian=m,
        spec=durable.spec,
        outbox=reopened,
        project=lambda payload, context: m.catalog("structured").put(
            resource="example.target",
            data={"id": payload["id"], "sourceVersion": context.source_version},
            mode="upsert",
        ),
        worker_id="recovered",
        clock=lambda: clock,
    )
    with m.context(ctx()):
        assert runner.run_once(now=clock).completed == 1
        assert runner.run_once(now=clock).claimed == 0
        assert (
            m.execute(
                m.catalog("structured").get(resource="example.target", where={"id": "event"})
            ).data["sourceVersion"]
            == 1
        )
    assert reopened.checkpoint(data.partition_key).revision == 1
    assert reopened.get(data.event_id).attempt_count == 2


@pytest.mark.parametrize("fault", ["missing", "key", "column"])
def test_startup_fails_closed_for_unmigrated_storage(durable: Any, fault: str) -> None:
    with connect(durable.dsn) as connection:
        table = sql.Identifier(durable.namespace, "__meridian_outbox_checkpoint")
        if fault == "missing":
            connection.execute(sql.SQL("DROP TABLE {}").format(table))
        elif fault == "key":
            connection.execute(
                sql.SQL("ALTER TABLE {} DROP CONSTRAINT {}").format(
                    table, sql.Identifier("__meridian_outbox_checkpoint_pkey")
                )
            )
        else:
            connection.execute(sql.SQL("ALTER TABLE {} ADD COLUMN unexpected text").format(table))
    with pytest.raises(RuntimeError, match="outbox storage"):
        durable.port()


@pytest.mark.parametrize(
    "kwargs",
    [
        {"limit": 0},
        {"limit": 1001},
        {"limit": True},
        {"owner": ""},
        {"lease_duration": timedelta(0)},
        {"now": datetime(2026, 1, 1)},
    ],
)
def test_claim_bounds_fail_without_state_changes(durable: Any, kwargs: Any) -> None:
    data = intent()
    durable.seed(data)
    with pytest.raises(ValueError):
        durable.port().atomic_claim(
            **{"owner": "owner", "limit": 1, "lease_duration": timedelta(seconds=1), **kwargs}
        )
    assert durable.port().get(data.event_id).state.value == "PENDING"


def test_expiry_during_checkpoint_write_rolls_back_progress(durable: Any) -> None:
    data = intent()
    durable.seed(data)
    port = durable.port()
    with connect(durable.dsn) as connection:
        connection.execute(
            sql.SQL(
                "CREATE FUNCTION {}.delay_checkpoint() RETURNS trigger LANGUAGE plpgsql "
                "AS $$ BEGIN PERFORM pg_sleep(0.3); RETURN NEW; END $$"
            ).format(sql.Identifier(durable.namespace))
        )
        connection.execute(
            sql.SQL(
                "CREATE TRIGGER delay_checkpoint BEFORE INSERT ON {}.__meridian_outbox_checkpoint "
                "FOR EACH ROW EXECUTE FUNCTION {}.delay_checkpoint()"
            ).format(sql.Identifier(durable.namespace), sql.Identifier(durable.namespace))
        )
    port.atomic_claim(owner="owner", limit=1, lease_duration=timedelta(seconds=0.15))
    with pytest.raises(MeridianError, match="lease expired"):
        port.complete(
            data.event_id, owner="owner", acknowledged_source_version=1, target_fingerprint=FP
        )
    assert port.checkpoint(data.partition_key).revision == 0
    assert port.get(data.event_id).state.value == "LEASED"


def test_uncommitted_source_and_intent_are_invisible_to_claimers(durable: Any) -> None:
    m, port = durable.meridian, durable.port()
    data = intent()
    with ThreadPoolExecutor(max_workers=1) as executor:
        with m.context(ctx()), m.transaction("structured:example.source"):
            TransactionalOutboxWriter(m, outbox_resource="example.outbox").commit(
                m.catalog("structured").put(
                    resource="example.source", data={"id": "event"}, mode="if_absent"
                ),
                data,
            )
            assert executor.submit(claim, port).result(timeout=5) == ()
        assert executor.submit(claim, port).result(timeout=5)[0].data == data


def test_shared_projection_name_retains_leases_across_selection_change(durable: Any) -> None:
    data = intent(target_labels=("search",))
    durable.seed(data)
    claim(durable.port())
    same_projection = durable.port(spec=replace(durable.spec, target_labels=("search",)))
    assert not claim(same_projection, owner="other")
    complete(durable.port(), data)
    assert same_projection.checkpoint(data.partition_key).revision == 1


@pytest.mark.parametrize("phase", ["claim", "target-acknowledgement"])
def test_abrupt_host_process_exit_preserves_intent(durable: Any, phase: str) -> None:
    data = intent()
    durable.seed(data)
    result = subprocess.run(
        [sys.executable, str(Path(__file__).with_name("outbox_crash_host.py"))],
        input=json.dumps({**durable.child_config, "phase": phase}),
        text=True,
        capture_output=True,
        timeout=20,
    )
    assert result.returncode == 91, result.stderr
    port = durable.reopen()
    assert port.get(data.event_id).lease.owner == "doomed-host"
    assert port.checkpoint(data.partition_key).revision == 0
    with durable.meridian.context(ctx()):
        target = durable.meridian.execute(
            durable.meridian.catalog("structured").get(
                resource="example.target", where={"id": "event"}
            )
        ).data
    assert (target is None) == (phase == "claim")
    assert claim(port, now=NOW + timedelta(seconds=2))[0].attempt_count == 2
    assert complete(port, data, now=NOW + timedelta(seconds=2)).revision == 1


def test_claim_result_byte_limit_rolls_back_every_lease(durable: Any) -> None:
    for i in range(3):
        durable.seed(intent(str(i), payload={"id": str(i), "content": "x" * 1000}))
    original = durable.runtime._context
    durable.runtime._context = replace(
        original,
        binding=replace(
            original.binding, client=replace(original.binding.client, max_result_bytes=2000)
        ),
    )
    with pytest.raises(MeridianError, match="result byte limit"):
        claim(durable.port())
    durable.runtime._context = original
    assert all(durable.port().get(str(i)).state.value == "PENDING" for i in range(3))
    assert len(claim(durable.port())) == 3
