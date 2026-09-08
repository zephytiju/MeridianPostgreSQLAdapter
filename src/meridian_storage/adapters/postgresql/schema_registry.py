# SPDX-License-Identifier: Apache-2.0
"""Durable Schema metadata. Only the explicit Platform migration hook executes DDL.

Each tenant/scope is one atomic, checksummed registry row. Row locking serializes
publication and deprecation; readers see a complete committed revision. Resource
activation and physical collection layouts are deliberately separate.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Iterator, Sequence
from contextlib import AbstractContextManager, contextmanager
from dataclasses import replace
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast

from meridian_storage.errors import (
    CompatibilityError,
    ErrorCode,
    MeridianError,
    MeridianTimeoutError,
)
from meridian_storage.semantics import (
    IncompatibleSchema,
    PublishedSchema,
    PublishResult,
    RegistryRevisionConflict,
    RegistrySnapshot,
    ResourceNotFound,
    SchemaDocument,
    SchemaReference,
    SchemaStatus,
    SchemaVersionConflict,
    SemanticVersion,
    classify_compatibility,
    sha256_fingerprint,
    validate_schema,
)
from psycopg import Connection, Error, sql
from psycopg.rows import tuple_row
from psycopg.types.json import Jsonb

from meridian_storage import OperationContext

from ._errors import map_postgresql_error
from .query._sql import BoundStatement, ident

if TYPE_CHECKING:
    from .migration import MigrationEvidence

REGISTRY_TABLE = "__meridian_schema_registry"
MIGRATION_TABLE = "__meridian_schema_registry_migration"
_FORMAT = "meridian.postgresql.schema-registry.v1"
_SUPPORTED_CATALOGS = frozenset({"structured"})


def _namespace(value: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[a-z_][a-z0-9_]{0,62}", value):
        raise ValueError("metadata physical namespace must be a lowercase SQL identifier")
    return value


def _incompatible() -> CompatibilityError:
    return CompatibilityError(
        ErrorCode.PHYSICAL_FINGERPRINT,
        "Schema registry storage is missing, incompatible, or corrupt; Platform migration required",
    )


def _storage_statements(namespace: str) -> tuple[BoundStatement, ...]:
    _namespace(namespace)
    return (
        BoundStatement(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(namespace))),
        BoundStatement(
            sql.SQL(
                "CREATE TABLE IF NOT EXISTS {} (singleton boolean PRIMARY KEY CHECK (singleton), "
                "format text NOT NULL, fingerprint text NOT NULL)"
            ).format(ident(namespace, MIGRATION_TABLE))
        ),
        BoundStatement(
            sql.SQL(
                "CREATE TABLE IF NOT EXISTS {} (tenant text NOT NULL, scope jsonb NOT NULL, "
                "revision bigint NOT NULL CHECK (revision >= 0), fingerprint text NOT NULL, "
                "payload jsonb NOT NULL, PRIMARY KEY (tenant, scope))"
            ).format(ident(namespace, REGISTRY_TABLE))
        ),
    )


def _migration_fingerprint(namespace: str) -> str:
    return sha256_fingerprint(
        {
            "format": _FORMAT,
            "statements": [s.command.as_string(None) for s in _storage_statements(namespace)],
        }
    )


def schema_registry_migration_statements(namespace: str) -> tuple[BoundStatement, ...]:
    """Adapter-internal composition into the Platform's deterministic migration plan."""
    return (
        *_storage_statements(namespace),
        BoundStatement(
            sql.SQL(
                "INSERT INTO {} (singleton, format, fingerprint) VALUES (true, {}, {}) "
                "ON CONFLICT (singleton) DO NOTHING"
            ).format(
                ident(namespace, MIGRATION_TABLE),
                sql.Literal(_FORMAT),
                sql.Literal(_migration_fingerprint(namespace)),
            ),
        ),
    )


def verify_schema_repository(connection: Connection[Any], *, physical_namespace: str) -> str:
    """Read-only migration verification for deployment readiness and repository operations."""
    namespace = _namespace(physical_namespace)
    with connection.cursor(row_factory=tuple_row) as cursor:
        cursor.execute(
            "SELECT to_regclass(%s), to_regclass(%s)",
            (
                f"{namespace}.{MIGRATION_TABLE}",
                f"{namespace}.{REGISTRY_TABLE}",
            ),
        )
        tables = cursor.fetchone()
        if tables is None or any(table is None for table in tables):
            raise _incompatible()
        cursor.execute(
            sql.SQL("SELECT format, fingerprint FROM {} WHERE singleton").format(
                ident(namespace, MIGRATION_TABLE)
            )
        )
        marker = cursor.fetchone()
        expected = _migration_fingerprint(namespace)
        if marker != (_FORMAT, expected):
            raise _incompatible()
        for table, columns, primary in (
            (
                MIGRATION_TABLE,
                [("singleton", "boolean"), ("format", "text"), ("fingerprint", "text")],
                "PRIMARY KEY (singleton)",
            ),
            (
                REGISTRY_TABLE,
                [
                    ("tenant", "text"),
                    ("scope", "jsonb"),
                    ("revision", "bigint"),
                    ("fingerprint", "text"),
                    ("payload", "jsonb"),
                ],
                "PRIMARY KEY (tenant, scope)",
            ),
        ):
            cursor.execute(
                "SELECT attname, format_type(atttypid, atttypmod), attnotnull "
                "FROM pg_attribute WHERE attrelid = to_regclass(%s) "
                "AND attnum > 0 AND NOT attisdropped ORDER BY attnum",
                (f"{namespace}.{table}",),
            )
            if cursor.fetchall() != [(name, kind, True) for name, kind in columns]:
                raise _incompatible()
            cursor.execute(
                "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
                "WHERE conrelid = to_regclass(%s) AND contype = 'p'",
                (f"{namespace}.{table}",),
            )
            if cursor.fetchall() != [(primary,)]:
                raise _incompatible()
    return expected


def migrate_schema_repository(
    connection: Connection[Any], *, physical_namespace: str
) -> MigrationEvidence:
    """Platform-only additive migration. Never called by repository or runtime startup.

    Joins an enclosing transaction. Rollback removes the entire initial migration.
    Reapplication verifies the stored immutable migration fingerprint.
    """
    from .migration import MigrationEvidence

    namespace = _namespace(physical_namespace)
    with connection.transaction(), connection.cursor(row_factory=tuple_row) as cursor:
        cursor.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
            (f"meridian:{namespace}:migration",),
        )
        cursor.execute("SELECT to_regclass(%s)", (f"{namespace}.{MIGRATION_TABLE}",))
        exists = cursor.fetchone()
        if exists and exists[0] is not None:
            fingerprint = verify_schema_repository(connection, physical_namespace=namespace)
            return MigrationEvidence(fingerprint, fingerprint, False)
        for statement in schema_registry_migration_statements(namespace):
            cursor.execute(cast(sql.SQL | sql.Composed, statement.command), statement.parameters)
        fingerprint = verify_schema_repository(connection, physical_namespace=namespace)
        return MigrationEvidence(fingerprint, fingerprint, True)


def _snapshot(revision: int, schemas: Sequence[PublishedSchema]) -> RegistrySnapshot:
    ordered = tuple(
        sorted(
            schemas,
            key=lambda item: (
                item.ref.catalog.value,
                item.ref.namespace,
                item.ref.name,
                SemanticVersion.parse(cast(str, item.ref.version)),
            ),
        )
    )
    fingerprint = sha256_fingerprint(
        {
            "revision": revision,
            "schemas": [item.to_dict() for item in ordered],
            "resources": [],
        }
    )
    return RegistrySnapshot(revision, fingerprint, ordered, ())


def _payload(snapshot: RegistrySnapshot) -> dict[str, object]:
    # Both namespace and Schema name may contain dots in the public grammar.
    # Preserve their explicit boundaries instead of parsing the display id.
    return {**snapshot.to_dict(), "references": [item.ref.to_dict() for item in snapshot.schemas]}


def _same_schema(left: SchemaReference, right: SchemaReference) -> bool:
    return (left.catalog, left.namespace, left.name) == (right.catalog, right.namespace, right.name)


class PostgreSQLSchemaRepository:
    """Public Semantics SchemaRepository supplied by deployment to SchemaAPI/provider.

    The connection factory owns credentials and connection lifetime. Every call
    opens a transaction/savepoint; an enclosing transaction retains commit authority.
    This release supports structured Schema metadata only. It never provisions
    collections, alters data tables, selects an Engine, or falls back to memory.
    """

    def __init__(
        self,
        *,
        connection_factory: Callable[[], AbstractContextManager[Connection[Any]]],
        physical_namespace: str,
        context: OperationContext,
        catalogs: Sequence[str] = ("structured",),
        operation_timeout_ms: int = 30_000,
    ) -> None:
        self._namespace = _namespace(physical_namespace)
        if (
            isinstance(catalogs, (str, bytes))
            or not catalogs
            or len(set(catalogs)) != len(catalogs)
            or set(catalogs) - _SUPPORTED_CATALOGS
        ):
            raise ValueError("Schema repository requires a nonempty supported Catalog subset")
        if not context.tenant or any(
            not isinstance(key, str) or not key or not isinstance(value, str) or not value
            for key, value in context.scope.items()
        ):
            raise ValueError("Schema repository requires an explicit tenant and valid scope")
        self._connections = connection_factory
        if type(operation_timeout_ms) is not int or operation_timeout_ms < 1:
            raise ValueError("Schema operation timeout must be a positive integer")
        self._timeout_ms = operation_timeout_ms
        self._context = context
        self._catalogs = frozenset(catalogs)
        self._key = (context.tenant, Jsonb(dict(context.scope)))
        self._table = ident(self._namespace, REGISTRY_TABLE)

    @property
    def revision(self) -> int:
        return self.snapshot().revision

    @contextmanager
    def _connection(self) -> Iterator[Connection[Any]]:
        remaining = self._context.remaining_seconds()
        if remaining is not None and remaining <= 0:
            raise MeridianTimeoutError(ErrorCode.DEADLINE_EXCEEDED, "Schema deadline expired")
        timeout = (
            self._timeout_ms
            if remaining is None
            else max(1, min(self._timeout_ms, int(remaining * 1000)))
        )
        try:
            with self._connections() as connection, connection.transaction():
                connection.execute(
                    "SELECT set_config('statement_timeout', %s, true)",
                    (f"{timeout}ms",),
                )
                verify_schema_repository(connection, physical_namespace=self._namespace)
                yield connection
        except Error as exc:
            raise map_postgresql_error(exc) from None

    def _catalog(self, reference: SchemaReference) -> None:
        if reference.catalog.value not in self._catalogs:
            raise ValueError("Schema Catalog is not installed in this repository")

    def _read(self, connection: Connection[Any], *, write: bool = False) -> RegistrySnapshot:
        with connection.cursor(row_factory=tuple_row) as cursor:
            if write:
                empty = _snapshot(0, ())
                empty_payload = _payload(empty)
                cursor.execute(
                    sql.SQL(
                        "INSERT INTO {} (tenant, scope, revision, fingerprint, payload) "
                        "VALUES (%s, %s, 0, %s, %s) ON CONFLICT (tenant, scope) DO NOTHING"
                    ).format(self._table),
                    (*self._key, sha256_fingerprint(empty_payload), Jsonb(empty_payload)),
                )
            cursor.execute(
                sql.SQL(
                    "SELECT revision, fingerprint, payload FROM {} WHERE tenant = %s AND scope = %s"
                ).format(self._table)
                + (sql.SQL(" FOR UPDATE") if write else sql.SQL("")),
                self._key,
            )
            row = cursor.fetchone()
        if row is None:
            return _snapshot(0, ())
        try:
            revision, fingerprint, payload = row
            if type(revision) is not int or revision < 0 or type(payload) is not dict:
                raise ValueError("invalid envelope")
            if set(payload) != {"revision", "fingerprint", "schemas", "resources", "references"}:
                raise ValueError("invalid envelope fields")
            publications = []
            for entry, address in zip(payload["schemas"], payload["references"], strict=True):
                document = entry["schema"]
                reference = SchemaReference.parse(address).exact()
                parsed = SchemaDocument.from_definition(
                    catalog=reference.catalog,
                    namespace=reference.namespace,
                    name=reference.name,
                    version=cast(str, reference.version),
                    definition=document,
                )
                self._catalog(parsed.ref)
                validate_schema(parsed)
                status = SchemaStatus(entry["status"])
                for timestamp in (entry["publishedAt"], entry["deprecatedAt"]):
                    if (
                        timestamp is not None
                        and datetime.fromisoformat(timestamp).utcoffset() is None
                    ):
                        raise ValueError("invalid timestamp")
                if not entry["publishedAt"] or (
                    (status is SchemaStatus.DEPRECATED) != (entry["deprecatedAt"] is not None)
                ):
                    raise ValueError("invalid publication status")
                publication = PublishedSchema(
                    parsed, status, entry["publishedAt"], entry["deprecatedAt"]
                )
                if publication.to_dict() != entry or reference.to_dict() != address:
                    raise ValueError("noncanonical envelope")
                publications.append(publication)
            snapshot = _snapshot(revision, publications)
            if (
                len({item.ref for item in publications}) != len(publications)
                or _payload(snapshot) != payload
                or sha256_fingerprint(payload) != fingerprint
            ):
                raise ValueError("registry fingerprint mismatch")
            return snapshot
        except (KeyError, TypeError, ValueError, AttributeError, MeridianError):
            raise _incompatible() from None

    def _write(self, connection: Connection[Any], snapshot: RegistrySnapshot) -> None:
        payload = _payload(snapshot)
        connection.execute(
            sql.SQL(
                "UPDATE {} SET revision = %s, fingerprint = %s, payload = %s "
                "WHERE tenant = %s AND scope = %s"
            ).format(self._table),
            (
                snapshot.revision,
                sha256_fingerprint(payload),
                Jsonb(payload),
                *self._key,
            ),
        )

    @staticmethod
    def _revision(expected: int | None, observed: int) -> None:
        if expected is not None and (type(expected) is not int or expected < 0):
            raise ValueError("expected revision must be a nonnegative integer")
        if expected is not None and expected != observed:
            raise RegistryRevisionConflict(
                f"expected registry revision {expected}, observed {observed}",
                requirement="registry.compare-and-set",
            )

    def publish_schema(
        self,
        document: SchemaDocument,
        *,
        expected_revision: int | None = None,
        allow_breaking: bool = False,
    ) -> PublishResult:
        self._catalog(document.ref)
        validate_schema(document)
        if type(allow_breaking) is not bool:
            raise ValueError("allow_breaking must be a boolean")
        with self._connection() as connection:
            before = self._read(connection, write=True)
            self._revision(expected_revision, before.revision)
            versions = [item for item in before.schemas if _same_schema(item.ref, document.ref)]
            for existing in versions:
                if existing.ref == document.ref:
                    if existing.fingerprint == document.fingerprint:
                        return PublishResult(existing, True, before.revision)
                    raise SchemaVersionConflict(
                        "Schema version already has a different fingerprint",
                        requirement="schema.version-immutable",
                        logical_references=(document.ref.canonical,),
                    )
            report = None
            if versions:
                previous = versions[-1]
                if SemanticVersion.parse(cast(str, document.ref.version)) <= SemanticVersion.parse(
                    cast(str, previous.ref.version)
                ):
                    raise SchemaVersionConflict(
                        "Schema versions must increase", requirement="schema.version-monotonic"
                    )
                report = classify_compatibility(previous.document, document)
                if report.breaking and not allow_breaking:
                    raise IncompatibleSchema(
                        "breaking Schema change requires explicit allow_breaking",
                        requirement="schema.breaking-explicit",
                    )
            publication = PublishedSchema(
                document,
                SchemaStatus.PUBLISHED,
                datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            )
            after = _snapshot(before.revision + 1, (*before.schemas, publication))
            self._write(connection, after)
            return PublishResult(publication, False, after.revision, report)

    def snapshot(self) -> RegistrySnapshot:
        with self._connection() as connection:
            return self._read(connection)

    def list_schema_versions(
        self,
        reference: SchemaReference,
        *,
        include_deprecated: bool = True,
    ) -> tuple[PublishedSchema, ...]:
        reference = SchemaReference.parse(reference)
        self._catalog(reference)
        return tuple(
            item
            for item in self.snapshot().schemas
            if _same_schema(item.ref, reference)
            and (include_deprecated or item.status is SchemaStatus.PUBLISHED)
        )

    def get_schema(
        self,
        reference: SchemaReference,
        *,
        include_deprecated: bool = False,
    ) -> PublishedSchema:
        reference = SchemaReference.parse(reference)
        candidates = self.list_schema_versions(reference, include_deprecated=include_deprecated)
        for item in reversed(candidates):
            if reference.version is None or item.ref.version == reference.version:
                return item
        raise ResourceNotFound(
            "Schema was not found",
            requirement="schema.exists",
            logical_references=(reference.canonical,),
        )

    def deprecate_schema(
        self,
        reference: SchemaReference,
        *,
        expected_revision: int | None = None,
    ) -> PublishedSchema:
        reference = SchemaReference.parse(reference).exact()
        self._catalog(reference)
        with self._connection() as connection:
            before = self._read(connection, write=True)
            self._revision(expected_revision, before.revision)
            for item in before.schemas:
                if item.ref == reference:
                    if item.status is SchemaStatus.DEPRECATED:
                        return item
                    result = replace(
                        item,
                        status=SchemaStatus.DEPRECATED,
                        deprecated_at=datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                    )
                    self._write(
                        connection,
                        _snapshot(
                            before.revision + 1,
                            tuple(
                                result if old.ref == reference else old for old in before.schemas
                            ),
                        ),
                    )
                    return result
            raise ResourceNotFound(
                "Schema was not found",
                requirement="schema.exists",
                logical_references=(reference.canonical,),
            )


__all__ = ["PostgreSQLSchemaRepository", "migrate_schema_repository", "verify_schema_repository"]
