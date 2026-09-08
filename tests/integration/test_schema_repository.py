# SPDX-License-Identifier: Apache-2.0
"""Real PostgreSQL acceptance through public SchemaAPI and injected repository."""

from __future__ import annotations

import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, nullcontext
from uuid import uuid4

import pytest
from meridian_storage.errors import CompatibilityError
from meridian_storage.semantics import (
    IncompatibleSchema,
    InvalidDefinition,
    RegistryRevisionConflict,
    ResourceNotFound,
    SchemaAPI,
    SchemaRepository,
    SchemaVersionConflict,
    SemanticsSchemaProvider,
)
from psycopg import connect, sql
from psycopg.errors import InsufficientPrivilege
from psycopg.types.json import Jsonb

from meridian_storage import OperationContext
from meridian_storage.adapters.postgresql import (
    PostgreSQLSchemaRepository,
    migrate_schema_repository,
)

pytestmark = pytest.mark.integration


def context(tenant="schema-tenant", workspace="schema-workspace"):
    return OperationContext(
        tenant=tenant,
        principal_ref="test:schema-writer",
        scope={"workspace": workspace},
    )


def definition(label="original"):
    return {
        "semanticKind": "relational",
        "fields": [{"name": "id", "logicalType": "string", "nullable": False}],
        "identity": ["id"],
        "extensions": {
            "org.prism/json-schema-2020-12.v1": {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": {"title": {"type": "string", "description": label}},
                "unevaluatedProperties": False,
            }
        },
    }


def publish(api, *, version="1.0.0", label="original", **kwargs):
    return api.publish(
        namespace="prism.components",
        name="metadata",
        version=version,
        definition=definition(label),
        **kwargs,
    )


def read(api, result):
    return api.read(
        namespace="prism.components",
        name="metadata",
        version="1.0.0",
        expected_fingerprint=result.publication.fingerprint,
    )


@pytest.fixture
def storage():
    dsn = os.environ.get("MERIDIAN_POSTGRESQL_TEST_DSN")
    if not dsn:
        pytest.fail("real PostgreSQL DSN is mandatory; acceptance cannot skip")
    namespace = "schema_acceptance_" + uuid4().hex[:12]

    @contextmanager
    def connections():
        with connect(dsn) as connection:
            yield connection

    def repository(*, ctx=None, connection_factory=connections):
        return PostgreSQLSchemaRepository(
            connection_factory=connection_factory,
            physical_namespace=namespace,
            context=ctx or context(),
            catalogs=("structured",),
        )

    with connections() as connection:
        first = migrate_schema_repository(connection, physical_namespace=namespace)
        second = migrate_schema_repository(connection, physical_namespace=namespace)
        assert first.applied and not second.applied
        assert first.plan_fingerprint == second.plan_fingerprint
    try:
        yield repository, namespace, connections
    finally:
        with connections() as connection:
            connection.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(namespace)))


def test_immutable_replay_exact_pin_and_new_connection(storage):
    factory, _, _ = storage
    repository = factory()
    assert isinstance(repository, SchemaRepository)
    api = SchemaAPI(repository)
    first = publish(api, expected_revision=0)
    repeated = publish(SchemaAPI(factory()), expected_revision=1)
    assert repeated.idempotent
    assert repeated.publication == first.publication
    assert repeated.registry_revision == first.registry_revision == 1
    assert read(SchemaAPI(factory()), first) == first.publication
    assert read(api, first).document.to_dict()["extensions"] == definition()["extensions"]
    with pytest.raises(SchemaVersionConflict):
        publish(api, label="changed same version")
    with pytest.raises(RegistryRevisionConflict):
        publish(api, version="1.1.0", expected_revision=0)
    with pytest.raises(IncompatibleSchema):
        api.read(
            namespace="prism.components",
            name="metadata",
            version="1.0.0",
            expected_fingerprint="sha256:" + "0" * 64,
        )
    assert factory().snapshot() == repository.snapshot()


def test_concurrent_identical_publications_have_one_commit(storage):
    factory, _, _ = storage
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _: publish(SchemaAPI(factory())), range(16)))
    assert sum(not result.idempotent for result in results) == 1
    assert len({result.publication.published_at for result in results}) == 1
    assert {result.registry_revision for result in results} == {1}
    assert factory().revision == 1


def test_concurrent_changed_content_conflicts(storage):
    factory, _, _ = storage

    def attempt(label):
        try:
            return publish(SchemaAPI(factory()), label=label)
        except SchemaVersionConflict:
            return None

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(attempt, ("a", "b")))
    assert sum(result is not None for result in results) == 1
    assert factory().revision == 1


def test_scope_isolation_and_core_provider_contract(storage):
    factory, _, _ = storage
    first = publish(SchemaAPI(factory()))
    for ctx in (
        context(tenant="another-tenant"),
        context(workspace="another-workspace"),
    ):
        api = SchemaAPI(factory(ctx=ctx))
        assert api.registry_revision == 0
        with pytest.raises(ResourceNotFound):
            read(api, first)
        other = publish(api, label="other scope")
        assert other.publication.fingerprint != first.publication.fingerprint
    live = SemanticsSchemaProvider(factory()).load_live()
    assert live.schemas == (first.publication.document.to_core_definition(),)
    assert live.extensions["registryFingerprint"] == factory().snapshot().fingerprint
    assert live.fingerprint != first.publication.fingerprint


def test_rollback_does_not_publish(storage):
    factory, _, connections = storage
    with connections() as connection:

        @contextmanager
        def same_connection():
            yield connection

        with pytest.raises(RuntimeError, match="rollback"), connection.transaction():
            publish(SchemaAPI(factory(connection_factory=same_connection)))
            raise RuntimeError("rollback")
    assert factory().revision == 0
    with pytest.raises(ResourceNotFound):
        SchemaAPI(factory()).read(namespace="prism.components", name="metadata", version="1.0.0")


def test_missing_migration_fails_closed(storage):
    _, namespace, connections = storage
    repository = PostgreSQLSchemaRepository(
        connection_factory=connections,
        physical_namespace=namespace + "_absent",
        context=context(),
        catalogs=("structured",),
    )
    with pytest.raises(CompatibilityError):
        publish(SchemaAPI(repository))
    with connections() as connection:
        assert (
            connection.execute("SELECT to_regnamespace(%s)", (namespace + "_absent",)).fetchone()[0]
            is None
        )


def test_unsupported_catalog_has_no_publication(storage):
    factory, _, _ = storage
    api = SchemaAPI(factory())
    with pytest.raises((CompatibilityError, ValueError, InvalidDefinition)):
        api.publish(
            catalog="query",
            namespace="example",
            name="invalid",
            version="1.0.0",
            definition=definition(),
        )
    assert factory().revision == 0


def test_fresh_process_reads_exact_durable_bytes(storage):
    factory, namespace, _ = storage
    first = publish(SchemaAPI(factory()))
    code = """
import os
from contextlib import contextmanager, nullcontext
from psycopg import connect
from meridian_storage import OperationContext
from meridian_storage.semantics import SchemaAPI
from meridian_storage.adapters.postgresql import PostgreSQLSchemaRepository
@contextmanager
def connections():
    with connect(os.environ["MERIDIAN_POSTGRESQL_TEST_DSN"]) as connection:
        yield connection
repository = PostgreSQLSchemaRepository(connection_factory=connections,
    physical_namespace=os.environ["SCHEMA_ACCEPTANCE_NAMESPACE"],
    context=OperationContext(tenant="schema-tenant", principal_ref="test:schema-writer",
        scope={"workspace":"schema-workspace"}), catalogs=("structured",))
value = SchemaAPI(repository).read(namespace="prism.components",name="metadata",version="1.0.0",
    expected_fingerprint=os.environ["SCHEMA_ACCEPTANCE_FINGERPRINT"])
print(value.fingerprint)
"""
    environment = dict(
        os.environ,
        SCHEMA_ACCEPTANCE_NAMESPACE=namespace,
        SCHEMA_ACCEPTANCE_FINGERPRINT=first.publication.fingerprint,
    )
    observed = subprocess.check_output(
        [sys.executable, "-I", "-c", code], env=environment, text=True
    )
    assert observed.strip() == first.publication.fingerprint


def test_concurrent_compare_and_set_has_one_winner(storage):
    factory, _, _ = storage

    def attempt(_):
        try:
            return publish(SchemaAPI(factory()), expected_revision=0)
        except RegistryRevisionConflict:
            return None

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(attempt, range(8)))
    assert sum(result is not None for result in results) == 1
    assert factory().revision == 1


def test_versions_deprecation_and_breaking_change_policy(storage):
    factory, _, _ = storage
    repository = factory()
    api = SchemaAPI(repository)
    first = publish(api)
    second = publish(api, version="1.2.0")
    latest = publish(api, version="1.10.0")
    assert [
        item.ref.version for item in repository.list_schema_versions(first.publication.ref)
    ] == [
        "1.0.0",
        "1.2.0",
        "1.10.0",
    ]
    with pytest.raises(SchemaVersionConflict):
        publish(api, version="1.3.0")
    changed = definition()
    changed["fields"].append({"name": "required", "logicalType": "string", "nullable": False})
    with pytest.raises(IncompatibleSchema):
        api.publish(
            namespace="prism.components", name="metadata", version="2.0.0", definition=changed
        )
    accepted = api.publish(
        namespace="prism.components",
        name="metadata",
        version="2.0.0",
        definition=changed,
        allow_breaking=True,
    )
    assert accepted.compatibility.breaking
    assert accepted.registry_revision == 4
    with pytest.raises(RegistryRevisionConflict):
        repository.deprecate_schema(accepted.publication.ref, expected_revision=3)
    deprecated = repository.deprecate_schema(accepted.publication.ref, expected_revision=4)
    assert factory().get_schema(accepted.publication.ref, include_deprecated=True) == deprecated
    with pytest.raises(ResourceNotFound):
        repository.get_schema(accepted.publication.ref)
    assert repository.deprecate_schema(accepted.publication.ref) == deprecated
    assert repository.revision == 5
    assert (
        len(repository.list_schema_versions(first.publication.ref, include_deprecated=False)) == 3
    )
    assert api.read(namespace="prism.components", name="metadata") == latest.publication
    assert repository.get_schema(second.publication.ref) == second.publication


@pytest.mark.parametrize(
    "corruption", ["schema", "revision", "envelope", "fingerprint", "status", "timestamp"]
)
def test_corruption_fails_closed_on_reads_and_writes(storage, corruption):
    factory, namespace, connections = storage
    first = publish(SchemaAPI(factory()))
    table = sql.Identifier(namespace, "__meridian_schema_registry")
    with connections() as connection:
        payload = connection.execute(sql.SQL("SELECT payload FROM {}").format(table)).fetchone()[0]
        if corruption == "schema":
            payload["schemas"][0]["schema"]["extensions"] = {}
        elif corruption == "revision":
            payload["revision"] += 1
        elif corruption == "envelope":
            payload["schemas"][0]["extra"] = "unrecognized"
        elif corruption == "fingerprint":
            payload["fingerprint"] = "sha256:" + "0" * 64
        elif corruption == "status":
            payload["schemas"][0]["status"] = "invalid"
        else:
            payload["schemas"][0]["publishedAt"] = None
        connection.execute(sql.SQL("UPDATE {} SET payload = %s").format(table), (Jsonb(payload),))
    for operation in (
        lambda: read(SchemaAPI(factory()), first),
        lambda: publish(SchemaAPI(factory()), version="1.1.0"),
    ):
        with pytest.raises(CompatibilityError):
            operation()


@pytest.mark.parametrize("drift", ["marker", "column", "primary-key"])
def test_migration_drift_fails_before_empty_registry_read(storage, drift):
    factory, namespace, connections = storage
    with connections() as connection:
        if drift == "marker":
            connection.execute(
                sql.SQL("UPDATE {} SET fingerprint = 'invalid'").format(
                    sql.Identifier(namespace, "__meridian_schema_registry_migration")
                )
            )
        elif drift == "column":
            connection.execute(
                sql.SQL("ALTER TABLE {} ADD COLUMN drift text").format(
                    sql.Identifier(namespace, "__meridian_schema_registry")
                )
            )
        else:
            connection.execute(
                sql.SQL("ALTER TABLE {} DROP CONSTRAINT __meridian_schema_registry_pkey").format(
                    sql.Identifier(namespace, "__meridian_schema_registry")
                )
            )
    with pytest.raises(CompatibilityError):
        factory().snapshot()


def test_runtime_without_ddl_or_delete_privileges(storage):
    factory, namespace, connections = storage
    role = "writer_" + uuid4().hex[:12]
    reader = "reader_" + uuid4().hex[:12]
    table = sql.Identifier(namespace, "__meridian_schema_registry")
    marker = sql.Identifier(namespace, "__meridian_schema_registry_migration")
    with connections() as connection:
        for name in (role, reader):
            connection.execute(sql.SQL("CREATE ROLE {} NOLOGIN").format(sql.Identifier(name)))
            connection.execute(
                sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(
                    sql.Identifier(namespace), sql.Identifier(name)
                )
            )
            connection.execute(
                sql.SQL("GRANT SELECT ON {}, {} TO {}").format(table, marker, sql.Identifier(name))
            )
        connection.execute(
            sql.SQL("GRANT INSERT, UPDATE ON {} TO {}").format(table, sql.Identifier(role))
        )
        before = connection.execute(
            "SELECT c.oid, c.relname FROM pg_class c "
            "JOIN pg_namespace n ON n.oid=c.relnamespace "
            "WHERE n.nspname=%s ORDER BY c.oid",
            (namespace,),
        ).fetchall()

    @contextmanager
    def restricted(name=role):
        with connections() as connection:
            connection.execute(sql.SQL("SET ROLE {}").format(sql.Identifier(name)))
            yield connection

    try:
        with restricted() as connection:
            assert not connection.execute(
                "SELECT has_schema_privilege(%s, 'CREATE')", (namespace,)
            ).fetchone()[0]
            with pytest.raises(InsufficientPrivilege), connection.transaction():
                connection.execute(
                    sql.SQL("CREATE TABLE {} (id integer)").format(
                        sql.Identifier(namespace, "forbidden")
                    )
                )
            with pytest.raises(InsufficientPrivilege), connection.transaction():
                connection.execute(sql.SQL("DELETE FROM {}").format(table))
        api = SchemaAPI(factory(connection_factory=restricted))
        first = publish(api)
        assert publish(api).idempotent
        assert read(api, first) == first.publication
        read_only = SchemaAPI(factory(connection_factory=lambda: restricted(reader)))
        assert read(read_only, first) == first.publication
        with connections() as connection:
            after = connection.execute(
                "SELECT c.oid, c.relname FROM pg_class c "
                "JOIN pg_namespace n ON n.oid=c.relnamespace "
                "WHERE n.nspname=%s ORDER BY c.oid",
                (namespace,),
            ).fetchall()
        assert after == before
    finally:
        with connections() as connection:
            for name in (role, reader):
                connection.execute(sql.SQL("DROP OWNED BY {}").format(sql.Identifier(name)))
                connection.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(name)))


def test_failed_write_savepoint_preserves_outer_transaction(storage):
    factory, _, connections = storage
    with connections() as connection:
        api = SchemaAPI(factory(connection_factory=lambda: nullcontext(connection)))
        first = publish(api)
        with pytest.raises(SchemaVersionConflict):
            publish(api, label="conflict")
        second = publish(api, version="1.1.0")
        assert second.registry_revision == 2
    assert factory().revision == 2
    assert read(SchemaAPI(factory()), first) == first.publication


def test_dotted_name_and_namespace_boundaries_survive_storage(storage):
    factory, _, _ = storage
    api = SchemaAPI(factory())
    first = api.publish(
        namespace="example", name="dotted.name", version="1.0.0", definition=definition()
    )
    second = api.publish(
        namespace="example.dotted",
        name="name",
        version="1.0.0",
        definition=definition("other address"),
    )
    assert not first.idempotent and not second.idempotent
    for result in (first, second):
        reference = result.publication.ref
        assert factory().get_schema(reference) == result.publication
        assert factory().list_schema_versions(reference) == (result.publication,)
    assert len(factory().snapshot().schemas) == 2
