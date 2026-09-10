# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from dataclasses import replace
from uuid import uuid4

import pytest
from conftest import fp, physical_resources, read_compatible_binding
from meridian_storage.context import OperationContext
from meridian_storage.runtime.operations import Operation
from meridian_storage.spi.adapters import AdapterCreateContext, ExecutionRequest, SecretValue
from psycopg import connect, errors, sql
from psycopg.conninfo import conninfo_to_dict

from meridian_storage.adapters.postgresql import PostgreSQLAdapterFactory, PostgreSQLSettings
from meridian_storage.adapters.postgresql.migration import MigrationExecutor
from meridian_storage.adapters.postgresql.schema import SchemaCompiler

pytestmark = pytest.mark.integration


def test_compatible_reader_requires_only_select_and_preserves_physical_metadata(postgresql_dsn):
    namespace = "meridian_read_compatibility"
    reader_binding = replace(read_compatible_binding(postgresql_dsn), physical_namespace=namespace)
    reader_settings = PostgreSQLSettings.from_binding(reader_binding)
    layout = next(iter(reader_settings.resources.values()))
    proof = reader_settings.read_compatibility[layout.ref.canonical]
    raw = reader_binding.to_dict()["settings"]
    del raw["readCompatibility"]
    raw["resources"][0]["resourceFingerprint"] = proof.stored_resource.fingerprint
    stored_settings = PostgreSQLSettings.from_binding(replace(reader_binding, settings=raw))
    plan = SchemaCompiler(stored_settings).compile()
    role = "meridian_reader_" + uuid4().hex
    with connect(postgresql_dsn) as owner:
        owner.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(namespace)))
        MigrationExecutor(stored_settings).apply(owner, plan)
        owner.execute(
            sql.SQL(
                "INSERT INTO {}.people (__tenant, __scope_workspace, id, name) VALUES (%s,%s,%s,%s)"
            ).format(sql.Identifier(namespace)),
            ("tenant-a", "workspace-a", "00000000-0000-0000-0000-000000000001", "Ada"),
        )
        owner.execute(
            sql.SQL("CREATE ROLE {} LOGIN PASSWORD 'read-compatibility-test-only'").format(
                sql.Identifier(role)
            )
        )
        owner.execute(
            sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(
                sql.Identifier(namespace), sql.Identifier(role)
            )
        )
        owner.execute(
            sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA {} TO {}").format(
                sql.Identifier(namespace), sql.Identifier(role)
            )
        )
        before = owner.execute(
            sql.SQL("SELECT * FROM {}.__meridian_resources ORDER BY resource_ref").format(
                sql.Identifier(namespace)
            )
        ).fetchall()
    context = AdapterCreateContext(
        binding=replace(reader_binding, required_physical_fingerprint=plan.physical_fingerprint),
        identity=SecretValue(role.encode()),
        credential=SecretValue(b"read-compatibility-test-only"),
    )
    reader = PostgreSQLAdapterFactory().create(context)
    try:
        reader.open()
        assert reader.probe().evidence["accessMode"] == "read-only"
        requested = physical_resources(reader_settings)
        verification = reader.verify_physical(requested)
        assert verification.fingerprint == plan.physical_fingerprint
        assert verification.evidence["readCompatibility"] == "structured.put.v1-v2"
        with connect(postgresql_dsn) as owner:
            owner.execute(
                sql.SQL("UPDATE {}.__meridian_resources SET resource_fingerprint = %s").format(
                    sql.Identifier(namespace)
                ),
                (fp("unexpected-stored-definition"),),
            )
        with pytest.raises(RuntimeError, match="resource fingerprint"):
            reader.verify_physical(requested)
        with connect(postgresql_dsn) as owner:
            owner.execute(
                sql.SQL("UPDATE {}.__meridian_resources SET resource_fingerprint = %s").format(
                    sql.Identifier(namespace)
                ),
                (proof.stored_resource.fingerprint,),
            )
        for field in ("resource_fingerprint", "schema_fingerprint", "profile"):
            with pytest.raises(RuntimeError, match="metadata mismatch"):
                reader.verify_physical((replace(requested[0], **{field: fp("incorrect")}),))
        operation = Operation(
            catalog="structured",
            operation_contract="meridian.structured.get",
            operation_version="1.0.0",
            resources=(layout.ref,),
            input={"where": {"name": "Ada"}},
            read_only=True,
            idempotent=True,
        )
        operation_context = OperationContext(
            principal_ref="reader", tenant="tenant-a", scope={"workspace": "workspace-a"}
        )
        request = ExecutionRequest(
            operation=operation,
            context=operation_context,
            request_id="read",
            execution_id="read",
            binding_id=reader_binding.id,
            registry_revision=1,
            registry_fingerprint=fp("registry"),
            attempt=1,
        )
        for transactional in (False, True):
            session = reader.open_session(transactional=transactional)
            try:
                if transactional:
                    session.begin()
                assert session.execute(request).data["name"] == "Ada"
                assert (
                    session.execute(
                        replace(request, context=replace(operation_context, tenant="other"))
                    ).data
                    is None
                )
                if transactional:
                    session.rollback()
            finally:
                session.close()
        # Even a privileged credential cannot write through a compatibility pool.
        owner_credentials = conninfo_to_dict(postgresql_dsn)
        privileged = PostgreSQLAdapterFactory().create(
            replace(
                context,
                identity=SecretValue(owner_credentials["user"].encode()),
                credential=SecretValue(owner_credentials["password"].encode()),
            )
        )
        try:
            privileged.open()
            with (
                pytest.raises(errors.ReadOnlySqlTransaction),
                privileged._semantics_connection() as connection,
            ):
                connection.execute(
                    sql.SQL("DELETE FROM {}.people").format(sql.Identifier(namespace))
                )
        finally:
            privileged.close()
        with connect(postgresql_dsn) as owner:
            after = owner.execute(
                sql.SQL("SELECT * FROM {}.__meridian_resources ORDER BY resource_ref").format(
                    sql.Identifier(namespace)
                )
            ).fetchall()
            assert after == before
            # Ordinary migration replay still recognizes the exact stored plan.
            assert not MigrationExecutor(stored_settings).apply(owner, plan).applied
    finally:
        reader.close()
        with connect(postgresql_dsn) as owner:
            owner.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(namespace)))
            owner.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(role)))
