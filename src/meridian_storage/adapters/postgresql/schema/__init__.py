# SPDX-License-Identifier: Apache-2.0
"""Deterministic PostgreSQL/PostGIS physical schema compiler."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

from psycopg import sql

from .._settings import FieldLayout, PostgreSQLSettings, ResourceLayout
from ..projection._storage import is_outbox, migration_statements, validate_layout
from ..query._sql import BoundStatement, ident
from ..schema_registry import schema_registry_migration_statements

_INTERNAL_COLUMNS = frozenset(
    {
        "__tenant",
        "__record_version",
        "__created_at",
        "__updated_at",
        "__source_collection",
        "__source_record_id",
        "__target_collection",
        "__target_record_id",
    }
)


def _sql_type(field: FieldLayout) -> sql.Composable:
    if field.cardinality == "many":
        return sql.SQL("jsonb")
    simple = {
        "boolean": "boolean",
        "int8": "smallint",
        "int16": "smallint",
        "int32": "integer",
        "int64": "bigint",
        "float64": "double precision",
        "string": "text",
        "bytes": "bytea",
        "uuid": "uuid",
        "utcTimestamp": "timestamp with time zone",
        "date": "date",
        "duration": "interval",
        "enum": "text",
        "json": "jsonb",
        "recordRef": "jsonb",
        "objectRef": "jsonb",
        "wgs84Point": "geography(Point,4326)",
    }
    if field.logical_type == "decimal":
        precision = field.precision or 38
        scale = field.scale or 0
        if not 1 <= precision <= 1000 or not 0 <= scale <= precision:
            raise ValueError(f"invalid decimal layout for field {field.name!r}")
        return sql.SQL("numeric({}, {})").format(sql.Literal(precision), sql.Literal(scale))
    try:
        return sql.SQL(simple[field.logical_type])
    except KeyError as exc:
        raise ValueError(f"unsupported logical type {field.logical_type!r}") from exc


def _scope_columns(settings: PostgreSQLSettings) -> tuple[str, ...]:
    return ("__tenant", *(f"__scope_{key}" for key in settings.scope_keys))


def _column_definition(field: FieldLayout) -> sql.Composed:
    nullable = sql.SQL("") if field.nullable else sql.SQL(" NOT NULL")
    return sql.Identifier(field.column) + sql.SQL(" ") + _sql_type(field) + nullable


def _relation_generated_columns(layout: ResourceLayout) -> tuple[sql.Composed, ...]:
    relation = layout.relation
    if relation is None:
        return ()
    fields = layout.field_map
    source = sql.Identifier(fields[relation.source_field].column)
    target = sql.Identifier(fields[relation.target_field].column)

    def collection(column: sql.Identifier) -> sql.Composed:
        return sql.SQL(
            "(({} -> 'collectionRef' ->> 'catalog') || ':' || "
            "({} -> 'collectionRef' ->> 'namespace') || '.' || "
            "({} -> 'collectionRef' ->> 'name'))"
        ).format(column, column, column)

    return (
        sql.Identifier("__source_collection")
        + sql.SQL(" text GENERATED ALWAYS AS (")
        + collection(source)
        + sql.SQL(") STORED"),
        sql.Identifier("__source_record_id")
        + sql.SQL(" text GENERATED ALWAYS AS ((")
        + source
        + sql.SQL(" -> 'recordId')::text) STORED"),
        sql.Identifier("__target_collection")
        + sql.SQL(" text GENERATED ALWAYS AS (")
        + collection(target)
        + sql.SQL(") STORED"),
        sql.Identifier("__target_record_id")
        + sql.SQL(" text GENERATED ALWAYS AS ((")
        + target
        + sql.SQL(" -> 'recordId')::text) STORED"),
    )


@dataclass(frozen=True, slots=True)
class MigrationPlan:
    statements: tuple[BoundStatement, ...]
    plan_fingerprint: str
    physical_fingerprint: str
    resource_fingerprints: tuple[tuple[str, str], ...]


class SchemaCompiler:
    """Compile settings to deterministic, transaction-safe DDL without executing it."""

    def __init__(self, settings: PostgreSQLSettings) -> None:
        self.settings = settings

    def compile(self) -> MigrationPlan:
        self.settings.require_writable()
        statements: list[BoundStatement] = [
            BoundStatement(sql.SQL("CREATE EXTENSION IF NOT EXISTS postgis")),
            BoundStatement(
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                    sql.Identifier(self.settings.physical_schema)
                )
            ),
            self._metadata_table(),
            self._migration_table(),
        ]
        if any(layout.ref.catalog == "evidence" for layout in self.settings.resources.values()):
            statements.append(self._evidence_replay_table())
        outboxes = [layout for layout in self.settings.resources.values() if is_outbox(layout)]
        if outboxes:
            for layout in outboxes:
                validate_layout(layout)
            statements.extend(migration_statements(self.settings.physical_schema))
        for layout in self.settings.resources.values():
            if layout.profile == "metadata-registry":
                statements.extend(
                    schema_registry_migration_statements(self.settings.physical_schema)
                )
            else:
                statements.extend(self._resource(layout))
        canonical = [statement.command.as_string(None) for statement in statements]
        payload = json.dumps(canonical, ensure_ascii=False, separators=(",", ":"))
        physical = "sha256:" + hashlib.sha256(payload.encode("utf-8")).hexdigest()
        logical_payload = json.dumps(
            {
                "physicalFingerprint": physical,
                "resources": [
                    {
                        "profile": layout.profile,
                        "ref": layout.ref.canonical,
                        "resourceFingerprint": layout.resource_fingerprint,
                        "schemaFingerprint": layout.schema_fingerprint,
                    }
                    for layout in self.settings.resources.values()
                ],
            },
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        plan_fingerprint = "sha256:" + hashlib.sha256(logical_payload.encode("utf-8")).hexdigest()
        return MigrationPlan(
            tuple(statements),
            plan_fingerprint,
            physical,
            tuple(
                (layout.ref.canonical, layout.resource_fingerprint)
                for layout in self.settings.resources.values()
            ),
        )

    def _metadata_table(self) -> BoundStatement:
        return BoundStatement(
            sql.SQL(
                "CREATE TABLE IF NOT EXISTS {} ("
                "resource_ref text PRIMARY KEY, "
                "resource_fingerprint text NOT NULL, "
                "schema_fingerprint text NOT NULL, "
                "profile text NOT NULL, "
                "physical_fingerprint text NOT NULL, "
                "updated_at timestamp with time zone NOT NULL DEFAULT clock_timestamp()"
                ")"
            ).format(ident(self.settings.physical_schema, "__meridian_resources"))
        )

    def _migration_table(self) -> BoundStatement:
        return BoundStatement(
            sql.SQL(
                "CREATE TABLE IF NOT EXISTS {} ("
                "plan_fingerprint text PRIMARY KEY, "
                "applied_at timestamp with time zone NOT NULL DEFAULT clock_timestamp()"
                ")"
            ).format(ident(self.settings.physical_schema, "__meridian_migrations"))
        )

    def _evidence_replay_table(self) -> BoundStatement:
        # Deployment migration owns this table; runtime startup never creates it.
        return BoundStatement(
            sql.SQL(
                "CREATE TABLE IF NOT EXISTS {} ("
                "replay_key text PRIMARY KEY, "
                "request_fingerprint text NOT NULL, "
                "result jsonb"
                ")"
            ).format(ident(self.settings.physical_schema, "__meridian_evidence_replay"))
        )

    def _resource(self, layout: ResourceLayout) -> list[BoundStatement]:
        scope = _scope_columns(self.settings)
        column_definitions: list[sql.Composable] = [
            sql.Identifier("__tenant") + sql.SQL(" text NOT NULL"),
            *(sql.Identifier(name) + sql.SQL(" text NOT NULL") for name in scope[1:]),
            *(_column_definition(field) for field in layout.fields),
            sql.Identifier("__record_version") + sql.SQL(" bigint NOT NULL DEFAULT 1"),
            sql.Identifier("__created_at")
            + sql.SQL(" timestamp with time zone NOT NULL DEFAULT clock_timestamp()"),
            sql.Identifier("__updated_at")
            + sql.SQL(" timestamp with time zone NOT NULL DEFAULT clock_timestamp()"),
            *_relation_generated_columns(layout),
        ]
        primary = (*scope, *(layout.field_map[name].column for name in layout.identity))
        definitions = list(column_definitions)
        definitions.append(
            sql.SQL("PRIMARY KEY (")
            + sql.SQL(", ").join(sql.Identifier(name) for name in primary)
            + sql.SQL(")")
        )
        table = ident(self.settings.physical_schema, layout.table)
        result = [
            BoundStatement(
                sql.SQL("CREATE TABLE IF NOT EXISTS {} (").format(table)
                + sql.SQL(", ").join(definitions)
                + sql.SQL(")")
            )
        ]
        result.extend(
            BoundStatement(
                sql.SQL("ALTER TABLE {} ADD COLUMN IF NOT EXISTS ").format(table) + definition
            )
            for definition in column_definitions
        )
        for index in layout.indexes:
            if index.kind == "relation-endpoint":
                # Generated endpoint indexes below satisfy this logical index. The
                # JSON RecordRef column itself is intentionally not indexed.
                continue
            indexed_columns = tuple(layout.field_map[name].column for name in index.fields)
            index_name = sql.Identifier(f"{layout.table}_{index.name}")
            if index.kind == "geospatial":
                command = sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} USING gist ({})").format(
                    index_name, table, sql.Identifier(indexed_columns[0])
                )
            elif index.kind == "full-text":
                command = sql.SQL(
                    "CREATE INDEX IF NOT EXISTS {} ON {} USING gin "
                    "(to_tsvector('simple', coalesce({}, '')))"
                ).format(index_name, table, sql.Identifier(indexed_columns[0]))
            else:
                prefix = sql.SQL("UNIQUE ") if index.unique else sql.SQL("")
                method = sql.SQL("hash") if index.kind == "hash" else sql.SQL("btree")
                selected = (*scope, *indexed_columns) if index.kind != "hash" else indexed_columns
                command = (
                    sql.SQL("CREATE ")
                    + prefix
                    + sql.SQL("INDEX IF NOT EXISTS {} ON {} USING {} (").format(
                        index_name, table, method
                    )
                    + sql.SQL(", ").join(sql.Identifier(name) for name in selected)
                    + sql.SQL(")")
                )
            result.append(BoundStatement(command))
        if layout.relation is not None:
            for suffix, endpoint in (
                ("source", ("__source_collection", "__source_record_id")),
                ("target", ("__target_collection", "__target_record_id")),
            ):
                result.append(
                    BoundStatement(
                        sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} (").format(
                            sql.Identifier(f"{layout.table}__{suffix}_endpoint"), table
                        )
                        + sql.SQL(", ").join(sql.Identifier(name) for name in (*scope, *endpoint))
                        + sql.SQL(")")
                    )
                )
        return result


__all__ = ["_INTERNAL_COLUMNS", "MigrationPlan", "SchemaCompiler"]
