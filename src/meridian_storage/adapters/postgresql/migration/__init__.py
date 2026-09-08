# SPDX-License-Identifier: Apache-2.0
"""Platform-invoked migration and logical transfer hooks.

These hooks execute plans supplied by the adapter. They never provision a
database, change identities, create backups, or select an Engine.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, cast

from psycopg import Connection, sql
from psycopg.rows import dict_row

from .._settings import PostgreSQLSettings
from ..query._sql import ident
from ..query.dml import DMLCompiler, jsonable
from ..schema import MigrationPlan
from ..schema_registry import verify_schema_repository


@dataclass(frozen=True, slots=True)
class MigrationEvidence:
    plan_fingerprint: str
    physical_fingerprint: str
    applied: bool


class MigrationExecutor:
    """Execute one deterministic DDL plan under a transaction-scoped advisory lock."""

    def __init__(self, settings: PostgreSQLSettings) -> None:
        self.settings = settings

    def apply(
        self,
        connection: Connection[Any],
        plan: MigrationPlan,
        *,
        expected_physical_fingerprint: str | None = None,
    ) -> MigrationEvidence:
        lock_key = f"meridian:{self.settings.physical_schema}:migration"
        with connection.transaction():
            connection.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (lock_key,))
            migration_table = ident(self.settings.physical_schema, "__meridian_migrations")
            table_name = f"{self.settings.physical_schema}.__meridian_migrations"
            table_exists = connection.execute(
                "SELECT to_regclass(%s) IS NOT NULL AS present", (table_name,)
            ).fetchone()
            if table_exists and _scalar(table_exists, "present"):
                existing = connection.execute(
                    sql.SQL("SELECT 1 AS present FROM {} WHERE plan_fingerprint = %s").format(
                        migration_table
                    ),
                    (plan.plan_fingerprint,),
                ).fetchone()
            else:
                existing = None
            if expected_physical_fingerprint is not None:
                observed = self._current_fingerprint(connection)
                if observed is not None and observed != expected_physical_fingerprint:
                    raise RuntimeError("physical fingerprint changed before migration")
            if existing is not None:
                self._verify_registry(connection)
                return MigrationEvidence(
                    plan.plan_fingerprint,
                    plan.physical_fingerprint,
                    False,
                )
            for statement in plan.statements:
                connection.execute(
                    cast(sql.SQL | sql.Composed, statement.command),
                    statement.parameters,
                )
            self._verify_registry(connection)
            resource_table = ident(self.settings.physical_schema, "__meridian_resources")
            for ref, resource_fingerprint in plan.resource_fingerprints:
                layout = self.settings.resources[ref]
                connection.execute(
                    sql.SQL(
                        "INSERT INTO {} (resource_ref, resource_fingerprint, schema_fingerprint, "
                        "profile, physical_fingerprint) VALUES (%s, %s, %s, %s, %s) "
                        "ON CONFLICT (resource_ref) DO UPDATE SET "
                        "resource_fingerprint = EXCLUDED.resource_fingerprint, "
                        "schema_fingerprint = EXCLUDED.schema_fingerprint, "
                        "profile = EXCLUDED.profile, "
                        "physical_fingerprint = EXCLUDED.physical_fingerprint, "
                        "updated_at = clock_timestamp()"
                    ).format(resource_table),
                    (
                        ref,
                        resource_fingerprint,
                        layout.schema_fingerprint,
                        layout.profile,
                        plan.physical_fingerprint,
                    ),
                )
            connection.execute(
                sql.SQL("INSERT INTO {} (plan_fingerprint) VALUES (%s)").format(migration_table),
                (plan.plan_fingerprint,),
            )
        return MigrationEvidence(plan.plan_fingerprint, plan.physical_fingerprint, True)

    def _verify_registry(self, connection: Connection[Any]) -> None:
        if any(
            layout.profile == "metadata-registry" for layout in self.settings.resources.values()
        ):
            verify_schema_repository(connection, physical_namespace=self.settings.physical_schema)

    def _current_fingerprint(self, connection: Connection[Any]) -> str | None:
        table = ident(self.settings.physical_schema, "__meridian_resources")
        table_name = f"{self.settings.physical_schema}.__meridian_resources"
        exists = connection.execute(
            "SELECT to_regclass(%s) IS NOT NULL AS present", (table_name,)
        ).fetchone()
        if not exists or not _scalar(exists, "present"):
            return None
        rows = connection.execute(
            sql.SQL("SELECT DISTINCT physical_fingerprint AS physical_fingerprint FROM {}").format(
                table
            )
        ).fetchall()
        values = {str(_scalar(row, "physical_fingerprint")) for row in rows}
        if len(values) > 1:
            raise RuntimeError("physical metadata contains divergent fingerprints")
        return next(iter(values), None)


def _scalar(row: object, key: str) -> object:
    if isinstance(row, Mapping):
        return row[key]
    return cast(Sequence[object], row)[0]


class LogicalTransfer:
    """Portable JSON-lines export/import; callers own destination and retention."""

    def __init__(self, settings: PostgreSQLSettings) -> None:
        self.settings = settings

    def export_rows(
        self,
        connection: Connection[Any],
        resources: Sequence[str],
        *,
        tenant: str,
        scope: Mapping[str, str],
    ) -> Iterable[str]:
        scope_columns, scope_values = self._scope(tenant, scope)
        for ref in resources:
            layout = self.settings.resources[ref]
            if layout.profile == "metadata-registry":
                raise ValueError("Schema metadata requires SchemaAPI snapshot or Platform backup")
            projections: list[sql.Composable] = []
            for field in layout.fields:
                column: sql.Composable = sql.Identifier(field.column)
                if field.logical_type == "wgs84Point" and field.cardinality == "one":
                    column = sql.SQL(
                        "jsonb_build_object('longitude', ST_X(({})::geometry), "
                        "'latitude', ST_Y(({})::geometry))"
                    ).format(column, column)
                projections.append(column + sql.SQL(" AS {}").format(sql.Identifier(field.name)))
            query = (
                sql.SQL("SELECT ")
                + sql.SQL(", ").join(projections)
                + sql.SQL(" FROM {} WHERE ").format(
                    ident(self.settings.physical_schema, layout.table)
                )
                + sql.SQL(" AND ").join(
                    sql.SQL("{} = %s").format(sql.Identifier(column)) for column in scope_columns
                )
                + sql.SQL(" ORDER BY ")
                + sql.SQL(", ").join(
                    sql.Identifier(layout.field_map[field].column) for field in layout.identity
                )
            )
            with connection.cursor(row_factory=dict_row) as cursor:
                cursor.execute(query, scope_values)
                for row in cursor:
                    yield json.dumps(
                        {"resource": ref, "values": jsonable(row)},
                        sort_keys=True,
                        separators=(",", ":"),
                        default=str,
                    )

    def import_rows(
        self,
        connection: Connection[Any],
        lines: Iterable[str],
        *,
        tenant: str,
        scope: Mapping[str, str],
    ) -> int:
        scope_columns, scope_values = self._scope(tenant, scope)
        count = 0
        encoder = DMLCompiler(self.settings)
        with connection.transaction():
            for line in lines:
                payload = json.loads(line)
                if set(payload) != {"resource", "values"} or not isinstance(
                    payload["values"], dict
                ):
                    raise ValueError("invalid Meridian logical export record")
                layout = self.settings.resources[payload["resource"]]
                if layout.profile == "metadata-registry":
                    raise ValueError(
                        "Schema metadata requires SchemaAPI publication or Platform restore"
                    )
                values = payload["values"]
                if set(values) != {field.name for field in layout.fields}:
                    raise ValueError("logical import fields do not match the pinned layout")
                encoder._validate_relation_values(layout, values)
                columns = (
                    *scope_columns,
                    *(layout.field_map[name].column for name in values),
                )
                value_sql: list[sql.Composable] = [sql.Placeholder() for _ in scope_values]
                parameters: list[object] = list(scope_values)
                for name, value in values.items():
                    expression, bound = encoder._value(layout.field_map[name], value)
                    value_sql.append(expression)
                    parameters.extend(bound)
                identity_columns = (
                    *scope_columns,
                    *(layout.field_map[field].column for field in layout.identity),
                )
                mutable_columns = [
                    field.column
                    for field in layout.fields
                    if field.mutable and field.name not in layout.identity
                ]
                update = (
                    sql.SQL(", ").join(
                        sql.SQL("{} = EXCLUDED.{}").format(
                            sql.Identifier(column), sql.Identifier(column)
                        )
                        for column in mutable_columns
                    )
                    if mutable_columns
                    else sql.SQL("{} = {}.{}").format(
                        sql.Identifier(layout.field_map[layout.identity[0]].column),
                        sql.Identifier(layout.table),
                        sql.Identifier(layout.field_map[layout.identity[0]].column),
                    )
                )
                command = (
                    sql.SQL("INSERT INTO {} (").format(
                        ident(self.settings.physical_schema, layout.table)
                    )
                    + sql.SQL(", ").join(sql.Identifier(column) for column in columns)
                    + sql.SQL(") VALUES (")
                    + sql.SQL(", ").join(value_sql)
                    + sql.SQL(") ON CONFLICT (")
                    + sql.SQL(", ").join(sql.Identifier(column) for column in identity_columns)
                    + sql.SQL(") DO UPDATE SET ")
                    + update
                )
                connection.execute(command, tuple(parameters))
                count += 1
        return count

    def _scope(
        self, tenant: str, scope: Mapping[str, str]
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        missing = set(self.settings.scope_keys) - set(scope)
        extra = set(scope) - set(self.settings.scope_keys)
        if not tenant or missing or extra:
            raise ValueError(
                f"scope is incomplete: missing={sorted(missing)}, extra={sorted(extra)}"
            )
        return (
            ("__tenant", *(f"__scope_{key}" for key in self.settings.scope_keys)),
            (tenant, *(scope[key] for key in self.settings.scope_keys)),
        )


@dataclass(frozen=True, slots=True)
class RecoveryHook:
    """Descriptive IaC hand-off; intentionally has no backup/restore side effects."""

    engine_profile: str
    logical_export_supported: bool = True
    physical_backup_authority: str = "platform-iac"
    physical_restore_authority: str = "platform-iac"


__all__ = [
    "LogicalTransfer",
    "MigrationEvidence",
    "MigrationExecutor",
    "RecoveryHook",
]
