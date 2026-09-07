# SPDX-License-Identifier: Apache-2.0
"""Mapping-first structured DML and atomic-claim compilation."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from meridian_storage.context import OperationContext
from meridian_storage.query.ast import parse_filter
from meridian_storage.registry.resources import ResourceRef
from meridian_storage.semantics import RecordReference
from psycopg import sql

from .._settings import FieldLayout, PostgreSQLSettings, ResourceLayout
from . import _ExpressionCompiler
from ._sql import BoundStatement, conjunction, ident
from ._values import adapt_driver_value, jsonable, wgs84_coordinates


@dataclass(frozen=True, slots=True)
class DMLCommand:
    statement: BoundStatement
    method: str
    layout: ResourceLayout
    single: bool = False
    conditional: bool = False


@dataclass(frozen=True, slots=True)
class AppendBatchCommand:
    commands: tuple[DMLCommand, ...]


class DMLCompiler:
    def __init__(self, settings: PostgreSQLSettings) -> None:
        self.settings = settings

    def compile(
        self,
        method: str,
        resource: ResourceRef,
        input_value: Mapping[str, object],
        context: OperationContext,
    ) -> DMLCommand | AppendBatchCommand:
        layout = self.settings.layout(resource)
        if method == "put":
            return self._put(layout, input_value, context)
        if method == "get":
            return self._get(layout, input_value, context)
        if method == "patch":
            return self._patch(layout, input_value, context)
        if method == "delete":
            return self._delete(layout, input_value, context)
        if method == "append":
            data = input_value.get("data", input_value.get("event"))
            if isinstance(data, Sequence) and not isinstance(data, (str, bytes, bytearray)):
                if not 1 <= len(data) <= 10_000:
                    raise ValueError("append batch must contain between 1 and 10000 records")
                # Compile every row before executing any SQL. Each mapping may omit
                # different nullable fields; array order remains result order.
                return AppendBatchCommand(
                    tuple(self._append(layout, {"data": item}, context) for item in data)
                )
            return self._append(layout, input_value, context)
        raise ValueError(f"DML compiler does not implement {method!r}")

    def _append(
        self,
        layout: ResourceLayout,
        input_value: Mapping[str, object],
        context: OperationContext,
    ) -> DMLCommand:
        data = input_value.get("data", input_value.get("event"))
        if not isinstance(data, Mapping):
            raise TypeError("evidence.append requires a data object")
        unknown = set(data) - set(layout.field_map)
        missing = {field.name for field in layout.fields if not field.nullable} - set(data)
        if unknown or missing:
            raise ValueError(
                f"append fields mismatch: missing={sorted(missing)}, unknown={sorted(unknown)}"
            )
        columns = [*self._scope_columns(), *(layout.field_map[name].column for name in data)]
        scope_values = self._scope_values(context)
        value_sql: list[sql.Composable] = [sql.Placeholder() for _ in scope_values]
        parameters: list[object] = list(scope_values)
        for name, value in data.items():
            expression, bound = self._value(layout.field_map[name], value)
            value_sql.append(expression)
            parameters.extend(bound)
        command = (
            sql.SQL("INSERT INTO {} AS t (").format(self._table(layout))
            + sql.SQL(", ").join(sql.Identifier(column) for column in columns)
            + sql.SQL(") VALUES (")
            + sql.SQL(", ").join(value_sql)
            + sql.SQL(") RETURNING ")
            + self._returning(layout)
        )
        return DMLCommand(BoundStatement(command, tuple(parameters)), "append", layout, single=True)

    def atomic_claim(
        self,
        resource: ResourceRef,
        *,
        where: Mapping[str, object],
        changes: Mapping[str, object],
        limit: int,
        context: OperationContext,
    ) -> DMLCommand:
        """Compile a bounded claim using ``FOR UPDATE SKIP LOCKED``."""

        if isinstance(limit, bool) or not 1 <= limit <= 500:
            raise ValueError("atomic claim limit must be between 1 and 500")
        layout = self.settings.layout(resource)
        where_sql = self._where(layout, where, context, alias="c")
        assignments, assignment_parameters = self._assignments(layout, changes, alias="t")
        keys = self._key_columns(layout)
        join = sql.SQL(" AND ").join(
            sql.SQL("t.{} = c.{}").format(sql.Identifier(column), sql.Identifier(column))
            for column in keys
        )
        projection = self._returning(layout, alias="t")
        command = (
            sql.SQL("WITH candidates AS (SELECT ")
            + sql.SQL(", ").join(sql.Identifier(column) for column in keys)
            + sql.SQL(" FROM {} AS c WHERE ").format(self._table(layout))
            + where_sql.command
            + sql.SQL(" ORDER BY ")
            + sql.SQL(", ").join(sql.Identifier(column) for column in keys)
            + sql.SQL(" FOR UPDATE SKIP LOCKED LIMIT %s) UPDATE {} AS t SET ").format(
                self._table(layout)
            )
            + assignments
            + sql.SQL(
                ", __record_version = t.__record_version + 1, __updated_at = clock_timestamp() "
            )
            + sql.SQL("FROM candidates AS c WHERE ")
            + join
            + sql.SQL(" RETURNING ")
            + projection
        )
        return DMLCommand(
            BoundStatement(
                command,
                (*where_sql.parameters, limit, *assignment_parameters),
            ),
            "atomicClaim",
            layout,
        )

    def _put(
        self,
        layout: ResourceLayout,
        input_value: Mapping[str, object],
        context: OperationContext,
    ) -> DMLCommand:
        mode = input_value.get("mode")
        if not isinstance(mode, str) or mode not in {"if_absent", "update", "upsert"}:
            raise ValueError(
                "structured.put requires an explicit if_absent, update, or upsert mode"
            )
        expected = input_value.get("expectedVersion")
        if mode == "if_absent" and expected is not None:
            raise ValueError("expectedVersion is invalid with structured.put mode if_absent")
        if expected is not None and (
            isinstance(expected, bool) or not isinstance(expected, (str, int))
        ):
            raise TypeError("expectedVersion must be a string or integer")
        data = input_value.get("data")
        if not isinstance(data, Mapping):
            raise TypeError("structured.put data must be an object")
        unknown = set(data) - set(layout.field_map)
        missing = {field.name for field in layout.fields if not field.nullable} - set(data)
        if unknown or missing:
            raise ValueError(
                f"put fields mismatch: missing={sorted(missing)}, unknown={sorted(unknown)}"
            )
        self._validate_relation_values(layout, data)
        columns = [*self._scope_columns(), *(layout.field_map[name].column for name in data)]
        scope_values = self._scope_values(context)
        values_sql: list[sql.Composable] = [sql.Placeholder() for _ in scope_values]
        parameters: list[object] = list(scope_values)
        for name, value in data.items():
            expression, bound = self._value(layout.field_map[name], value)
            values_sql.append(expression)
            parameters.extend(bound)
        projection = self._returning(layout)
        if expected is not None:
            identity = {name: data[name] for name in layout.identity if name in data}
            if len(identity) != len(layout.identity):
                raise ValueError("conditional put requires every identity field")
            assignments, update_parameters = self._assignments(
                layout,
                {name: value for name, value in data.items() if name not in layout.identity},
            )
            where = self._where(layout, identity, context)
            command = (
                sql.SQL("UPDATE {} AS t SET ").format(self._table(layout))
                + assignments
                + sql.SQL(
                    ", __record_version = t.__record_version + 1, "
                    "__updated_at = clock_timestamp() WHERE "
                )
                + where.command
                + sql.SQL(" AND t.__record_version = %s RETURNING ")
                + projection
            )
            return DMLCommand(
                BoundStatement(command, (*update_parameters, *where.parameters, expected)),
                "put",
                layout,
                single=True,
                conditional=True,
            )
        identity_columns = self._key_columns(layout)
        mutable = [
            field for field in layout.fields if field.mutable and field.name not in layout.identity
        ]
        if mutable:
            update: sql.Composable = sql.SQL(", ").join(
                sql.SQL("{} = EXCLUDED.{}").format(
                    sql.Identifier(field.column), sql.Identifier(field.column)
                )
                for field in mutable
            )
            update += sql.SQL(
                ", __record_version = t.__record_version + 1, __updated_at = clock_timestamp()"
            )
        else:
            update = sql.SQL("__updated_at = t.__updated_at")
        if mode == "update":
            # Preserve the unconditional put field/version behavior: omitted
            # nullable mutable fields take their insert value (NULL), immutable
            # fields stay unchanged, and immutable-only rows do not advance.
            clauses: list[sql.Composable] = []
            update_parameters_list: list[object] = []
            for field in mutable:
                expression, bound = self._value(field, data.get(field.name))
                clauses.append(sql.Identifier(field.column) + sql.SQL(" = ") + expression)
                update_parameters_list.extend(bound)
            if mutable:
                update_assignments: sql.Composable = sql.SQL(", ").join(clauses) + sql.SQL(
                    ", __record_version = t.__record_version + 1, __updated_at = clock_timestamp()"
                )
            else:
                update_assignments = sql.SQL("__updated_at = t.__updated_at")
            where = self._where(layout, {name: data[name] for name in layout.identity}, context)
            command = (
                sql.SQL("UPDATE {} AS t SET ").format(self._table(layout))
                + update_assignments
                + sql.SQL(" WHERE ")
                + where.command
                + sql.SQL(" RETURNING ")
                + projection
            )
            return DMLCommand(
                BoundStatement(command, (*update_parameters_list, *where.parameters)),
                "put",
                layout,
                single=True,
                conditional=True,
            )
        command = (
            sql.SQL("INSERT INTO {} AS t (").format(self._table(layout))
            + sql.SQL(", ").join(sql.Identifier(column) for column in columns)
            + sql.SQL(") VALUES (")
            + sql.SQL(", ").join(values_sql)
            + sql.SQL(")")
        )
        if mode == "upsert":
            command += (
                sql.SQL(" ON CONFLICT (")
                + sql.SQL(", ").join(sql.Identifier(column) for column in identity_columns)
                + sql.SQL(") DO UPDATE SET ")
                + update
            )
        command += sql.SQL(" RETURNING ") + projection
        return DMLCommand(BoundStatement(command, tuple(parameters)), "put", layout, single=True)

    def _get(
        self,
        layout: ResourceLayout,
        input_value: Mapping[str, object],
        context: OperationContext,
    ) -> DMLCommand:
        where = input_value.get("where")
        if not isinstance(where, Mapping):
            raise TypeError("structured.get where must be an object")
        predicate = self._where(layout, where, context)
        command = (
            sql.SQL("SELECT ")
            + self._returning(layout)
            + sql.SQL(" FROM {} AS t WHERE ").format(self._table(layout))
            + predicate.command
            + sql.SQL(" LIMIT 2")
        )
        return DMLCommand(BoundStatement(command, predicate.parameters), "get", layout, single=True)

    def _patch(
        self,
        layout: ResourceLayout,
        input_value: Mapping[str, object],
        context: OperationContext,
    ) -> DMLCommand:
        where = input_value.get("where")
        changes = input_value.get("changes")
        if not isinstance(where, Mapping) or not isinstance(changes, Mapping):
            raise TypeError("structured.patch where and changes must be objects")
        assignments, parameters = self._assignments(layout, changes)
        predicate = self._where(layout, where, context)
        expected = input_value.get("expectedVersion")
        expected_sql = sql.SQL("")
        expected_parameters: tuple[object, ...] = ()
        if expected is not None:
            expected_sql = sql.SQL(" AND t.__record_version = %s")
            expected_parameters = (expected,)
        command = (
            sql.SQL("UPDATE {} AS t SET ").format(self._table(layout))
            + assignments
            + sql.SQL(
                ", __record_version = t.__record_version + 1, "
                "__updated_at = clock_timestamp() WHERE "
            )
            + predicate.command
            + expected_sql
            + sql.SQL(" RETURNING ")
            + self._returning(layout)
        )
        return DMLCommand(
            BoundStatement(command, (*parameters, *predicate.parameters, *expected_parameters)),
            "patch",
            layout,
            single=False,
            conditional=expected is not None,
        )

    def _delete(
        self,
        layout: ResourceLayout,
        input_value: Mapping[str, object],
        context: OperationContext,
    ) -> DMLCommand:
        where = input_value.get("where")
        if not isinstance(where, Mapping):
            raise TypeError("structured.delete where must be an object")
        predicate = self._where(layout, where, context)
        expected = input_value.get("expectedVersion")
        expected_sql = sql.SQL("")
        parameters = list(predicate.parameters)
        if expected is not None:
            expected_sql = sql.SQL(" AND t.__record_version = %s")
            parameters.append(expected)
        command = (
            sql.SQL("DELETE FROM {} AS t WHERE ").format(self._table(layout))
            + predicate.command
            + expected_sql
            + sql.SQL(" RETURNING ")
            + self._returning(layout)
        )
        return DMLCommand(
            BoundStatement(command, tuple(parameters)),
            "delete",
            layout,
            conditional=expected is not None,
        )

    def _where(
        self,
        layout: ResourceLayout,
        where: Mapping[str, object],
        context: OperationContext,
        *,
        alias: str = "t",
    ) -> BoundStatement:
        aliases = {layout.ref.canonical: alias}
        compiler = _ExpressionCompiler(self.settings, aliases, {layout.ref.canonical: layout})
        scope = BoundStatement(
            sql.SQL(" AND ").join(
                sql.SQL("{} = %s").format(ident(alias, column)) for column in self._scope_columns()
            ),
            self._scope_values(context),
        )
        expression = parse_filter(where)
        return scope if expression is None else conjunction((scope, compiler.compile(expression)))

    def _assignments(
        self,
        layout: ResourceLayout,
        changes: Mapping[str, object],
        *,
        alias: str = "t",
    ) -> tuple[sql.Composed, tuple[object, ...]]:
        if not changes:
            raise ValueError("mutation changes cannot be empty")
        unknown = set(changes) - set(layout.field_map)
        immutable = {
            name
            for name in changes
            if name in layout.identity
            or (name in layout.field_map and not layout.field_map[name].mutable)
        }
        if unknown or immutable:
            raise ValueError(
                f"mutation fields invalid: unknown={sorted(unknown)}, immutable={sorted(immutable)}"
            )
        self._validate_relation_values(layout, changes)
        clauses: list[sql.Composable] = []
        parameters: list[object] = []
        for name, value in changes.items():
            field = layout.field_map[name]
            expression, bound = self._value(field, value)
            clauses.append(sql.Identifier(field.column) + sql.SQL(" = ") + expression)
            parameters.extend(bound)
        return sql.SQL(", ").join(clauses), tuple(parameters)

    @staticmethod
    def _value(field: FieldLayout, value: object) -> tuple[sql.Composable, tuple[object, ...]]:
        if value is None:
            if not field.nullable:
                raise ValueError(f"field {field.name!r} is not nullable")
            return sql.Placeholder(), (None,)
        if field.cardinality == "many" or field.logical_type in {"json", "recordRef", "objectRef"}:
            return sql.Placeholder(), (adapt_driver_value(field, value),)
        if field.logical_type == "wgs84Point":
            longitude, latitude = wgs84_coordinates(value, field.name)
            return (
                sql.SQL("ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography"),
                (longitude, latitude),
            )
        if field.logical_type == "bytes":
            return sql.Placeholder(), (adapt_driver_value(field, value),)
        return sql.Placeholder(), (value,)

    def _returning(self, layout: ResourceLayout, *, alias: str = "t") -> sql.Composed:
        expressions: list[sql.Composable] = []
        for field in layout.fields:
            column = ident(alias, field.column)
            if field.logical_type == "wgs84Point" and field.cardinality == "one":
                expression = sql.SQL(
                    "jsonb_build_object('longitude', ST_X(({})::geometry), "
                    "'latitude', ST_Y(({})::geometry))"
                ).format(column, column)
            else:
                expression = column
            expressions.append(expression + sql.SQL(" AS {}").format(sql.Identifier(field.name)))
        for physical, logical in (
            ("__record_version", "recordVersion"),
            ("__created_at", "createdAt"),
            ("__updated_at", "updatedAt"),
        ):
            # Schema-declared timestamps own their logical result keys. Emitting
            # a second alias here lets dict rows overwrite immutable user data.
            if logical in {"createdAt", "updatedAt"} and logical in layout.field_map:
                continue
            expressions.append(
                ident(alias, physical) + sql.SQL(" AS {}").format(sql.Identifier(logical))
            )
        return sql.SQL(", ").join(expressions)

    def _table(self, layout: ResourceLayout) -> sql.Composed:
        return ident(self.settings.physical_schema, layout.table)

    def _scope_columns(self) -> tuple[str, ...]:
        return ("__tenant", *(f"__scope_{key}" for key in self.settings.scope_keys))

    def _scope_values(self, context: OperationContext) -> tuple[str, ...]:
        missing = set(self.settings.scope_keys) - set(context.scope)
        extra = set(context.scope) - set(self.settings.scope_keys)
        if context.tenant is None or missing or extra:
            raise ValueError(
                f"operation scope mismatch: missing={sorted(missing)}, extra={sorted(extra)}"
            )
        return (context.tenant, *(context.scope[key] for key in self.settings.scope_keys))

    def _key_columns(self, layout: ResourceLayout) -> tuple[str, ...]:
        return (
            *self._scope_columns(),
            *(layout.field_map[name].column for name in layout.identity),
        )

    @staticmethod
    def _validate_relation_values(
        layout: ResourceLayout,
        values: Mapping[str, object],
    ) -> None:
        relation = layout.relation
        if relation is None:
            return
        for field_name, allowed in (
            (relation.source_field, relation.source_collections),
            (relation.target_field, relation.target_collections),
        ):
            if field_name not in values:
                continue
            value = values.get(field_name)
            if not isinstance(value, Mapping):
                raise TypeError(f"relation endpoint {field_name!r} must be a RecordReference")
            reference = RecordReference.from_mapping(value)
            if reference.collection_ref.canonical not in allowed:
                raise ValueError(
                    f"relation endpoint {field_name!r} references a Collection outside its pin"
                )


__all__ = ["AppendBatchCommand", "DMLCommand", "DMLCompiler", "jsonable"]
