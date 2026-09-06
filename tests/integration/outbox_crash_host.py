# SPDX-License-Identifier: Apache-2.0
"""Disposable host process used to prove committed work survives abrupt exit."""

import json
import os
import sys
from datetime import UTC, datetime, timedelta

from meridian_storage.projection import ProjectionSpec
from meridian_storage.registry.resources import (
    NamespaceDefinition,
    ResourceBundle,
    ResourceDefinition,
    ResourceRef,
    SchemaDefinition,
    SchemaRef,
)
from meridian_storage.runtime.config import BindingConfig, RuntimeConfig
from meridian_storage.spi.adapters import AdapterCreateContext, SecretValue

from meridian_storage import Meridian, OperationContext
from meridian_storage.adapters.postgresql import PostgreSQLAdapterFactory, PostgreSQLOutbox

payload = json.load(sys.stdin)
bundle = payload["bundle"]
bundle = ResourceBundle(
    bundle["providerId"],
    bundle["providerVersion"],
    bundle["providerContractVersion"],
    namespaces=tuple(NamespaceDefinition(**n) for n in bundle["namespaces"]),
    schemas=tuple(
        SchemaDefinition(SchemaRef.parse(s["ref"]), s["definition"]) for s in bundle["schemas"]
    ),
    resources=tuple(
        ResourceDefinition(
            ResourceRef.parse(r["ref"]),
            r["profile"],
            schema=SchemaRef.parse(r["schema"]),
            required_scope=tuple(r["requiredScope"]),
        )
        for r in bundle["resources"]
    ),
)


class Schemas:
    provider_id = bundle.provider_id
    provider_contract_version = "1.0.0"

    def load(self):
        return bundle


class Secrets:
    def resolve(self, ref):
        return SecretValue(
            payload["identity" if ref.reference == "identity" else "credential"].encode()
        )


runtime = PostgreSQLAdapterFactory().create(
    AdapterCreateContext(
        binding=BindingConfig.from_mapping(payload["binding"], "binding"),
        identity=SecretValue(payload["identity"].encode()),
        credential=SecretValue(payload["credential"].encode()),
    )
)
runtime.open()
context = OperationContext(principal_ref="test:host", tenant="a", scope={"workspace": "a"})
port = PostgreSQLOutbox(
    runtime,
    resource="example.outbox",
    spec=ProjectionSpec(**payload["spec"]),
    context=context,
    poison_threshold=2,
)
(record,) = port.atomic_claim(
    owner="doomed-host",
    limit=1,
    lease_duration=timedelta(seconds=1),
    now=datetime(2026, 1, 1, tzinfo=UTC),
)
if payload["phase"] == "target-acknowledgement":
    meridian = Meridian(
        RuntimeConfig.from_mapping(payload["config"]),
        schema_providers=[Schemas()],
        secret_resolver=Secrets(),
    )
    meridian.start()
    with meridian.context(context):
        acknowledged = meridian.execute(
            meridian.catalog("structured").put(
                resource="example.target",
                data={"id": "event", "sourceVersion": record.data.source_version},
                mode="upsert",
            )
        )
        assert acknowledged.data["sourceVersion"] == record.data.source_version
# Deliberately bypass pool cleanup and finally handlers, after the durable commit.
os._exit(91)
