# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Disposable adapter-owned migration setup adapted from published PostgreSQL 2.1.0 tests."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import asdict, replace
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import pytest
from binding import make_binding
from meridian_storage.projection import (
    OutboxDataV1,
    ProjectionSpec,
)
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

from meridian_storage import Meridian, OperationContext
from meridian_storage.adapters.postgresql import PostgreSQLAdapterFactory, PostgreSQLOutbox
from meridian_storage.adapters.postgresql._settings import PostgreSQLSettings
from meridian_storage.adapters.postgresql.migration import MigrationExecutor
from meridian_storage.adapters.postgresql.projection._storage import INTENT_FIELDS
from meridian_storage.adapters.postgresql.schema import SchemaCompiler

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
        payload=kw.pop("payload", {"id": name, "name": "first", "deleted": False}),
        occurred_at=kw.pop("occurred_at", NOW),
        **kw,
    )


@pytest.fixture
def durable(postgresql_dsn: str) -> Iterator[Any]:
    binding, user, password = make_binding(postgresql_dsn)
    namespace = "ob_" + uuid4().hex[:12]
    layouts, schemas, resources = [], [], []
    for name in ("source", "target", "outbox", "other_outbox"):
        definitions = (
            {n: (kind, nullable, False) for n, (kind, nullable) in INTENT_FIELDS.items()}
            if "outbox" in name
            else {
                "id": ("string", False, False),
                "name": ("string", False, True),
                "deleted": ("boolean", False, True),
            }
            if name == "source"
            else {
                "id": ("string", False, False),
                "sourceKey": ("string", False, True),
                "sourceVersion": ("int64", False, True),
                "deleted": ("boolean", False, True),
                "document": ("json", False, True),
            }
        )
        fields = [
            {
                "name": n,
                "column": n.lower(),
                "logicalType": kind,
                "nullable": nullable,
                "mutable": mutable,
            }
            for n, (kind, nullable, mutable) in definitions.items()
        ]
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


@pytest.fixture(scope="session")
def postgresql_dsn() -> str:
    import os

    value = os.environ.get("MERIDIAN_POSTGRESQL_TEST_DSN")
    if not value:
        pytest.fail("released-provider conformance requires a disposable PostgreSQL DSN")
    return value
