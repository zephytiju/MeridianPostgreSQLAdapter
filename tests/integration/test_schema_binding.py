# SPDX-License-Identifier: Apache-2.0
"""Adapter SPI acceptance; full consumer facade uses released Constructs output."""

from contextlib import contextmanager
from dataclasses import replace
from uuid import uuid4

import pytest
from conftest import make_create_context, physical_resources
from meridian_storage.semantics import (
    SchemaAPI,
    SemanticsSchemaProvider,
    StructuredCatalogProvider,
    sha256_fingerprint,
)
from meridian_storage.spi.adapters import ExecutionRequest
from psycopg import connect, sql
from psycopg.rows import dict_row

from meridian_storage import OperationContext
from meridian_storage.adapters.postgresql import (
    MigrationExecutor,
    PostgreSQLAdapterFactory,
    PostgreSQLSchemaRepository,
    PostgreSQLSettings,
    SchemaCompiler,
)

pytestmark = pytest.mark.integration


def test_metadata_binding_migration_readiness_and_session_dispatch(postgresql_dsn):
    namespace = "metadata_binding_" + uuid4().hex[:12]
    bootstrap = SemanticsSchemaProvider().load()
    resource = next(r for r in bootstrap.resources if r.ref.catalog == "structured")
    schema = bootstrap.schemas[0]
    create, _, _ = make_create_context(postgresql_dsn)
    # Adapter-owned layout contract test, not a consumer runtime configuration.
    binding = replace(
        create.binding,
        physical_namespace=namespace,
        settings={
            "formatVersion": "meridian.postgresql.settings.v1",
            "scopeKeys": ["workspace"],
            "topology": dict(create.binding.settings["topology"]),
            "resources": [
                {
                    "ref": resource.ref.canonical,
                    "profile": "metadata-registry",
                    "table": "__meridian_schema_registry",
                    "schemaFingerprint": schema.fingerprint,
                    "resourceFingerprint": resource.fingerprint,
                    "fields": [],
                    "identity": [],
                    "indexes": [],
                    "relation": None,
                }
            ],
        },
    )
    settings = PostgreSQLSettings.from_binding(binding)
    plan = SchemaCompiler(settings).compile()
    create = replace(
        create, binding=replace(binding, required_physical_fingerprint=plan.physical_fingerprint)
    )
    runtime = PostgreSQLAdapterFactory().create(create)

    @contextmanager
    def connections():
        with connect(postgresql_dsn) as connection:
            yield connection

    ctx = OperationContext(
        tenant="binding-tenant", principal_ref="test", scope={"workspace": "binding"}
    )
    repository = PostgreSQLSchemaRepository(
        connection_factory=connections, physical_namespace=namespace, context=ctx
    )
    provider = StructuredCatalogProvider()
    surface = provider.create_surface()
    operation = provider.normalize(
        surface.publish_schema(
            namespace="binding",
            name="metadata",
            version="1.0.0",
            definition={
                "semanticKind": "relational",
                "fields": [{"name": "id", "logicalType": "string", "nullable": False}],
                "identity": ["id"],
            },
        )
    )
    request = ExecutionRequest(
        operation=operation,
        context=ctx,
        request_id="metadata-request",
        execution_id="metadata-execution",
        binding_id=binding.id,
        registry_revision=1,
        registry_fingerprint=bootstrap.fingerprint,
        attempt=1,
    )
    try:
        with connect(postgresql_dsn, row_factory=dict_row) as connection:
            assert MigrationExecutor(settings).apply(connection, plan).applied
            assert not MigrationExecutor(settings).apply(connection, plan).applied
        runtime.open()
        runtime.probe()
        assert (
            runtime.verify_physical(physical_resources(settings)).fingerprint
            == plan.physical_fingerprint
        )
        session = runtime.open_session(transactional=True)
        session.begin()
        published = session.execute(request).data
        session.rollback()
        session.close()
        assert repository.revision == 0
        session = runtime.open_session(transactional=False)
        published = session.execute(request).data
        assert not published["idempotent"]
        assert session.execute(request).data["idempotent"]
        session.close()
        observed = SchemaAPI(repository).read(namespace="binding", name="metadata", version="1.0.0")
        assert sha256_fingerprint(observed.to_dict()) == sha256_fingerprint(
            published["publication"]
        )
    finally:
        runtime.close()
        with connections() as connection:
            connection.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(namespace))
            )
