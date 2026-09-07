# SPDX-License-Identifier: Apache-2.0
"""Validated Meridian query-plan to parameterized PostgreSQL translation."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import replace
from typing import Any, cast

from meridian_storage.query.adapter import (
    CompiledQuery,
    NormalizedQueryResult,
    QueryCapabilities,
    TranslationContext,
)
from meridian_storage.query.cursor import CursorSigner
from meridian_storage.query.wire import QueryOperation
from psycopg import sql

from .._settings import FieldLayout, PostgreSQLSettings, ResourceLayout
from ..descriptor import QUERY_CAPABILITIES
from ._sql import BoundStatement, conjunction, disjunction, ident
from ._values import encode_query_value


class _ExpressionCompiler:
    def __init__(
        self,
        settings: PostgreSQLSettings,
        aliases: Mapping[str, str],
        layouts: Mapping[str, ResourceLayout],
    ) -> None:
        self.settings = settings
        self.aliases = aliases
        self.layouts = layouts

    def compile(self, expression: Any) -> BoundStatement:
        value = expression.to_dict() if hasattr(expression, "to_dict") else expression
        if not isinstance(value, Mapping):
            raise TypeError("query expression must be a canonical mapping")
        kind = value.get("kind")
        if kind == "field":
            layout, alias, field = self._field(value)
            del layout
            return BoundStatement(ident(alias, field.column))
        if kind == "literal":
            return BoundStatement(sql.Placeholder(), (value.get("value"),))
        if kind == "parameter":
            raise ValueError("unbound query parameters cannot cross the Adapter SPI")
        if kind == "point":
            coordinates = value.get("coordinates")
            if not isinstance(coordinates, Sequence) or len(coordinates) != 2:
                raise ValueError("point requires longitude and latitude")
            return BoundStatement(
                sql.SQL("ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography"),
                (coordinates[0], coordinates[1]),
            )
        if kind in {"not", "negate"}:
            operand = self.compile(value["operand"])
            token = sql.SQL("NOT ") if kind == "not" else sql.SQL("-")
            return BoundStatement(
                token + sql.SQL("(") + operand.command + sql.SQL(")"),
                operand.parameters,
            )
        if kind in {"and", "or"}:
            operands = value.get("operands")
            if not isinstance(operands, Sequence):
                raise TypeError("boolean operands must be an array")
            compiled_operands = (self.compile(item) for item in operands)
            return (
                conjunction(compiled_operands) if kind == "and" else disjunction(compiled_operands)
            )
        if kind in {
            "eq",
            "ne",
            "lt",
            "lte",
            "gt",
            "gte",
            "add",
            "subtract",
            "multiply",
            "divide",
            "modulo",
        }:
            left_value = value["left"]
            right_value = value["right"]
            left = self.compile(left_value)
            right = self.compile(right_value)
            left_field = self._expression_field(left_value)
            right_field = self._expression_field(right_value)
            if left_field is not None and self._is_literal(right_value):
                right = self._adapt(right, left_field)
            if right_field is not None and self._is_literal(left_value):
                left = self._adapt(left, right_field)
            operator = {
                "eq": "=",
                "ne": "<>",
                "lt": "<",
                "lte": "<=",
                "gt": ">",
                "gte": ">=",
                "add": "+",
                "subtract": "-",
                "multiply": "*",
                "divide": "/",
                "modulo": "%",
            }[cast(str, kind)]
            return BoundStatement(
                sql.SQL("(")
                + left.command
                + sql.SQL(f" {operator} ")
                + right.command
                + sql.SQL(")"),
                (*left.parameters, *right.parameters),
            )
        if kind == "prefix":
            left = self.compile(value["left"])
            right = self.compile(value["right"])
            return BoundStatement(
                sql.SQL("(")
                + left.command
                + sql.SQL(" LIKE (replace(replace(replace((")
                + right.command
                + sql.SQL(
                    " )::text, '\\\\', '\\\\\\\\'), '%', '\\%'), '_', '\\_') || '%') ESCAPE '\\\\')"
                ),
                (*left.parameters, *right.parameters),
            )
        if kind == "contains":
            left_layout = self._expression_field(value["left"])
            left = self.compile(value["left"])
            right = self.compile(value["right"])
            if left_layout is not None and (
                left_layout.logical_type in {"json", "recordRef", "objectRef"}
                or left_layout.cardinality == "many"
            ):
                right = self._adapt(right, left_layout)
                command = left.command + sql.SQL(" @> (") + right.command + sql.SQL(")::jsonb")
            else:
                command = (
                    sql.SQL("position((")
                    + right.command
                    + sql.SQL(")::text in ")
                    + left.command
                    + sql.SQL(") > 0")
                )
                right, left = left, right
            return BoundStatement(command, (*left.parameters, *right.parameters))
        if kind == "isNull":
            operand = self.compile(value["operand"])
            expected = value.get("expected") is True
            return BoundStatement(
                sql.SQL("(")
                + operand.command
                + (sql.SQL(" IS NULL)") if expected else sql.SQL(" IS NOT NULL)")),
                operand.parameters,
            )
        if kind in {"in", "notIn"}:
            operand = self.compile(value["operand"])
            candidates = value.get("values")
            if not isinstance(candidates, Sequence) or not candidates:
                raise ValueError("membership requires candidates")
            compiled_items = tuple(self.compile(item) for item in candidates)
            field_layout = self._expression_field(value["operand"])
            if field_layout is not None:
                compiled_items = tuple(self._adapt(item, field_layout) for item in compiled_items)
            token = sql.SQL(" NOT IN (") if kind == "notIn" else sql.SQL(" IN (")
            return BoundStatement(
                sql.SQL("(")
                + operand.command
                + token
                + sql.SQL(", ").join(item.command for item in compiled_items)
                + sql.SQL("))"),
                (
                    *operand.parameters,
                    *(parameter for item in compiled_items for parameter in item.parameters),
                ),
            )
        if kind == "timestampRange":
            operand = self.compile(value["operand"])
            parts: list[BoundStatement] = []
            if value.get("start") is not None:
                start = self.compile(value["start"])
                op = ">=" if value.get("includeStart") else ">"
                parts.append(
                    BoundStatement(
                        operand.command + sql.SQL(f" {op} ") + start.command,
                        (*operand.parameters, *start.parameters),
                    )
                )
            if value.get("end") is not None:
                end = self.compile(value["end"])
                op = "<=" if value.get("includeEnd") else "<"
                parts.append(
                    BoundStatement(
                        operand.command + sql.SQL(f" {op} ") + end.command,
                        (*operand.parameters, *end.parameters),
                    )
                )
            return conjunction(parts)
        if kind == "documentPath":
            document = self.compile(value["document"])
            pointer = value.get("pointer")
            if not isinstance(pointer, str) or not pointer.startswith("/"):
                raise ValueError("document path must be an RFC 6901 pointer")
            segments = [
                segment.replace("~1", "/").replace("~0", "~") for segment in pointer[1:].split("/")
            ]
            return BoundStatement(
                sql.SQL("(") + document.command + sql.SQL(" #> %s::text[])"),
                (*document.parameters, segments),
            )
        if kind == "fullText":
            fields = value.get("fields")
            if not isinstance(fields, Sequence) or not fields:
                raise ValueError("full-text requires fields")
            compiled_fields = tuple(self.compile(item) for item in fields)
            joined = sql.SQL(", ").join(item.command for item in compiled_fields)
            return BoundStatement(
                sql.SQL("to_tsvector('simple', concat_ws(' ', ")
                + joined
                + sql.SQL(")) @@ websearch_to_tsquery('simple', %s)"),
                (
                    *(parameter for item in compiled_fields for parameter in item.parameters),
                    value.get("query"),
                ),
            )
        if kind in {"distance", "distanceWithin"}:
            left = self.compile(value["left"])
            right = self.compile(value["right"])
            if kind == "distance":
                return BoundStatement(
                    sql.SQL("ST_Distance(")
                    + left.command
                    + sql.SQL(", ")
                    + right.command
                    + sql.SQL(")"),
                    (*left.parameters, *right.parameters),
                )
            return BoundStatement(
                sql.SQL("ST_DWithin(")
                + left.command
                + sql.SQL(", ")
                + right.command
                + sql.SQL(", %s)"),
                (*left.parameters, *right.parameters, value.get("distance")),
            )
        if kind == "aggregate":
            function = value.get("function")
            if function not in {"count", "sum", "avg", "min", "max", "percentile"}:
                raise ValueError("unsupported aggregate")
            operand_value = value.get("operand")
            if function == "count" and operand_value is None:
                return BoundStatement(sql.SQL("count(*)"))
            operand = self.compile(operand_value)
            distinct = sql.SQL("DISTINCT ") if value.get("distinct") else sql.SQL("")
            if function == "percentile":
                raise ValueError(
                    "percentile requires an explicit percentile in V1 and is unavailable"
                )
            return BoundStatement(
                sql.SQL(f"{function}(") + distinct + operand.command + sql.SQL(")"),
                operand.parameters,
            )
        raise ValueError(f"unsupported query expression kind: {kind!r}")

    def _field(self, value: Mapping[str, object]) -> tuple[ResourceLayout, str, FieldLayout]:
        resource = value.get("resource")
        if resource is None:
            canonical = next(iter(self.aliases))
        elif not isinstance(resource, str):
            raise TypeError("field Resource must be a string")
        else:
            canonical = self._canonical_resource(resource)
        layout = self.layouts[canonical]
        name = value.get("name")
        if not isinstance(name, str) or name not in layout.field_map:
            raise ValueError(f"field {name!r} is absent from pinned resource {canonical}")
        return layout, self.aliases[canonical], layout.field_map[name]

    def _canonical_resource(self, value: str) -> str:
        if value in self.aliases:
            return value
        matches = [
            key for key in self.aliases if key.endswith(f":{value}") or key.endswith(f".{value}")
        ]
        if len(matches) != 1:
            raise ValueError(f"field Resource {value!r} is not an unambiguous target")
        return matches[0]

    def _expression_field(self, value: object) -> FieldLayout | None:
        if not isinstance(value, Mapping) or value.get("kind") != "field":
            return None
        return self._field(value)[2]

    @staticmethod
    def _is_literal(value: object) -> bool:
        return isinstance(value, Mapping) and value.get("kind") == "literal"

    @staticmethod
    def _adapt(statement: BoundStatement, field: FieldLayout) -> BoundStatement:
        return BoundStatement(
            statement.command,
            tuple(encode_query_value(field, item) for item in statement.parameters),
        )


class PostgreSQLQueryTranslator:
    """Compile released Query plans; the returned command is opaque outside the SPI."""

    def __init__(
        self,
        settings: PostgreSQLSettings,
        *,
        cursor_signer: CursorSigner | None = None,
    ) -> None:
        self.settings = settings
        self._cursor_signer = cursor_signer

    @property
    def capabilities(self) -> QueryCapabilities:
        return QUERY_CAPABILITIES

    def compile(self, plan: object, context: TranslationContext) -> CompiledQuery:
        operation = plan if isinstance(plan, QueryOperation) else getattr(plan, "operation", plan)
        if not isinstance(operation, QueryOperation):
            raise TypeError("PostgreSQL translator requires a released QueryOperation/PlannedQuery")
        if operation.consistency not in QUERY_CAPABILITIES.consistency_classes:
            raise ValueError("query consistency is not advertised by this Adapter")
        if operation.page.point_in_time:
            raise ValueError("point-in-time pagination is not advertised by this Adapter")
        if operation.result.include_total:
            raise ValueError("total counts are not advertised by this Adapter")
        if getattr(plan, "binding_id", context.binding_id) != context.binding_id:
            raise ValueError("query plan and translation Binding ids differ")
        if (
            getattr(plan, "registry_fingerprint", context.registry_fingerprint)
            != context.registry_fingerprint
        ):
            raise ValueError("query plan and translation registry revisions differ")
        if getattr(plan, "empty_result", False):
            return self._compiled(
                context,
                BoundStatement(sql.SQL("SELECT NULL WHERE FALSE")),
                operation,
                extra={"empty": True, "pageSize": operation.page.size},
            )
        if operation.operation == "traverse":
            return self._compile_traversal(operation, context)
        if operation.operation not in {"get", "scan", "search", "aggregate"}:
            raise ValueError("mutation QueryOperation values are carried by structured Operations")
        return self._compile_select(operation, context)

    def normalize_result(
        self,
        compiled: CompiledQuery,
        raw_result: object,
    ) -> NormalizedQueryResult:
        command = cast(Mapping[str, Any], compiled.command)
        if command.get("empty"):
            return NormalizedQueryResult([], provenance={"adapter": "postgresql"})
        if not isinstance(raw_result, Sequence) or isinstance(raw_result, (str, bytes, bytearray)):
            raise TypeError("PostgreSQL raw query result must be a row sequence")
        rows = [dict(cast(Mapping[str, object], row)) for row in raw_result]
        page_size = cast(int, command.get("pageSize", len(rows)))
        has_more = len(rows) > page_size
        rows = rows[:page_size]
        cursor: str | None = None
        cursor_fields = cast(Sequence[str], command.get("cursorFields", ()))
        if has_more and rows and cursor_fields:
            if self._cursor_signer is None:
                raise RuntimeError("pagination requires an Adapter-owned CursorSigner")
            cursor_context = cast(Mapping[str, Any], command["cursorContext"])
            cursor = self._cursor_signer.issue(
                plan_fingerprint=cast(str, cursor_context["planFingerprint"]),
                schema_fingerprints=cast(Mapping[str, str], cursor_context["schemaFingerprints"]),
                registry_fingerprint=cast(str, cursor_context["registryFingerprint"]),
                scope_fingerprint=cast(str, cursor_context["scopeFingerprint"]),
                sort_tuple=cast(Any, [rows[-1][field] for field in cursor_fields]),
                page_size=page_size,
            )
        internal = frozenset(cast(Sequence[str], command.get("internalColumns", ())))
        data = [{key: value for key, value in row.items() if key not in internal} for row in rows]
        if command.get("single"):
            normalized: object = data[0] if data else None
        else:
            normalized = data
        return NormalizedQueryResult(
            cast(Any, normalized),
            cursor=cursor,
            provenance={
                "adapter": "postgresql",
                "consistency": "strong",
                "pagination": "live-keyset",
            },
        )

    def _compile_select(
        self, operation: QueryOperation, context: TranslationContext
    ) -> CompiledQuery:
        aliases, layouts = self._targets(operation)
        compiler = _ExpressionCompiler(self.settings, aliases, layouts)
        base_ref = operation.targets[0].resource.canonical
        base_layout = layouts[base_ref]
        base_alias = aliases[base_ref]
        from_clause = sql.SQL(" FROM {} AS {}").format(
            ident(self.settings.physical_schema, base_layout.table),
            sql.Identifier(base_alias),
        )
        join_parameters: list[object] = []
        for join in operation.joins:
            canonical = join.target.resource.canonical
            layout = layouts[canonical]
            predicate = conjunction(
                (
                    compiler.compile(join.on),
                    self._scope_predicate(aliases[canonical], context),
                )
            )
            token = sql.SQL(" LEFT JOIN ") if join.kind == "left" else sql.SQL(" INNER JOIN ")
            from_clause += (
                token
                + sql.SQL("{} AS {} ON ").format(
                    ident(self.settings.physical_schema, layout.table),
                    sql.Identifier(aliases[canonical]),
                )
                + predicate.command
            )
            join_parameters.extend(predicate.parameters)
        where = self._scope_predicate(base_alias, context)
        if operation.filter is not None:
            where = conjunction((where, compiler.compile(operation.filter)))
        if operation.operation == "aggregate":
            select, select_parameters = self._aggregate_projection(operation, compiler)
            group = self._grouping(operation, compiler)
            statement = BoundStatement(
                sql.SQL("SELECT ")
                + select
                + from_clause
                + sql.SQL(" WHERE ")
                + where.command
                + group.command,
                (*select_parameters, *join_parameters, *where.parameters, *group.parameters),
            )
            return self._compiled(
                context, statement, operation, extra={"pageSize": operation.page.size}
            )
        order = self._order(operation, base_layout, compiler)
        cursor_fields = tuple(f"__cursor_{index}" for index in range(len(order)))
        select, select_parameters = self._record_projection(
            operation, base_layout, base_alias, compiler, order, cursor_fields
        )
        if operation.page.cursor is not None:
            where = conjunction((where, self._keyset(operation, context, order)))
        order_sql = sql.SQL(" ORDER BY ") + sql.SQL(", ").join(
            expression.command + sql.SQL(f" {direction.upper()} NULLS {nulls.upper()}")
            for expression, direction, nulls in order
        )
        statement = BoundStatement(
            sql.SQL("SELECT ")
            + select
            + from_clause
            + sql.SQL(" WHERE ")
            + where.command
            + order_sql
            + sql.SQL(" LIMIT %s"),
            (
                *select_parameters,
                *join_parameters,
                *where.parameters,
                *(parameter for expression, _, _ in order for parameter in expression.parameters),
                operation.page.size + 1,
            ),
        )
        return self._compiled(
            context,
            statement,
            operation,
            extra={
                "pageSize": operation.page.size,
                "single": operation.operation == "get",
                "cursorFields": list(cursor_fields),
                "internalColumns": list(cursor_fields),
                "cursorContext": {
                    "planFingerprint": self._cursor_plan_fingerprint(operation),
                    "schemaFingerprints": dict(context.schema_fingerprints),
                    "registryFingerprint": context.registry_fingerprint,
                    "scopeFingerprint": context.scope_fingerprint,
                },
            },
        )

    def _targets(
        self, operation: QueryOperation
    ) -> tuple[dict[str, str], dict[str, ResourceLayout]]:
        aliases: dict[str, str] = {}
        layouts: dict[str, ResourceLayout] = {}
        for index, target in enumerate(operation.targets):
            canonical = target.resource.canonical
            aliases[canonical] = f"t{index}"
            layouts[canonical] = self.settings.layout(target.resource)
        return aliases, layouts

    def _scope_predicate(self, alias: str, context: TranslationContext) -> BoundStatement:
        # Scope values are injected at execution because TranslationContext deliberately
        # contains only a scope fingerprint. Placeholders are tagged for the runtime.
        columns = ("__tenant", *(f"__scope_{key}" for key in self.settings.scope_keys))
        return BoundStatement(
            sql.SQL(" AND ").join(
                sql.SQL("{} = %s").format(ident(alias, column)) for column in columns
            ),
            tuple({"$scope": column} for column in columns),
        )

    def _record_projection(
        self,
        operation: QueryOperation,
        layout: ResourceLayout,
        alias: str,
        compiler: _ExpressionCompiler,
        order: Sequence[tuple[BoundStatement, str, str]],
        cursor_fields: Sequence[str],
    ) -> tuple[sql.Composed, tuple[object, ...]]:
        expressions: list[sql.Composable] = []
        parameters: list[object] = []
        if operation.result.projection:
            for index, projection in enumerate(operation.result.projection):
                compiled = compiler.compile(projection.expression)
                selected_alias = projection.alias or f"value_{index}"
                expressions.append(
                    compiled.command + sql.SQL(" AS {}").format(sql.Identifier(selected_alias))
                )
                parameters.extend(compiled.parameters)
        else:
            for field in layout.fields:
                column = ident(alias, field.column)
                if field.logical_type == "wgs84Point" and field.cardinality == "one":
                    column = sql.SQL(
                        "jsonb_build_object('longitude', ST_X(({})::geometry), "
                        "'latitude', ST_Y(({})::geometry))"
                    ).format(column, column)
                expressions.append(column + sql.SQL(" AS {}").format(sql.Identifier(field.name)))
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
        for (compiled, _, _), cursor_alias in zip(order, cursor_fields, strict=True):
            expressions.append(
                compiled.command + sql.SQL(" AS {}").format(sql.Identifier(cursor_alias))
            )
            parameters.extend(compiled.parameters)
        return sql.SQL(", ").join(expressions), tuple(parameters)

    def _aggregate_projection(
        self, operation: QueryOperation, compiler: _ExpressionCompiler
    ) -> tuple[sql.Composed, tuple[object, ...]]:
        expressions: list[sql.Composable] = []
        parameters: list[object] = []
        for index, grouping in enumerate(operation.grouping):
            compiled = compiler.compile(grouping)
            expressions.append(
                compiled.command + sql.SQL(" AS {}").format(sql.Identifier(f"group_{index}"))
            )
            parameters.extend(compiled.parameters)
        for aggregate in operation.aggregates:
            compiled = compiler.compile(aggregate.aggregate)
            expressions.append(
                compiled.command + sql.SQL(" AS {}").format(sql.Identifier(aggregate.name))
            )
            parameters.extend(compiled.parameters)
        return sql.SQL(", ").join(expressions), tuple(parameters)

    def _grouping(self, operation: QueryOperation, compiler: _ExpressionCompiler) -> BoundStatement:
        if not operation.grouping:
            return BoundStatement(sql.SQL(""))
        compiled_grouping = tuple(compiler.compile(item) for item in operation.grouping)
        return BoundStatement(
            sql.SQL(" GROUP BY ") + sql.SQL(", ").join(item.command for item in compiled_grouping),
            tuple(parameter for item in compiled_grouping for parameter in item.parameters),
        )

    def _order(
        self,
        operation: QueryOperation,
        layout: ResourceLayout,
        compiler: _ExpressionCompiler,
    ) -> tuple[tuple[BoundStatement, str, str], ...]:
        result = [
            (compiler.compile(item.expression), item.direction, item.nulls)
            for item in operation.order
        ]
        represented = {
            item.expression.name
            for item in operation.order
            if getattr(item.expression, "resource", None) in {None, layout.ref.canonical}
            and hasattr(item.expression, "name")
        }
        for name in layout.identity:
            if name not in represented:
                result.append(
                    (
                        BoundStatement(ident("t0", layout.field_map[name].column)),
                        "asc",
                        "last",
                    )
                )
        return tuple(result)

    @staticmethod
    def _cursor_plan_fingerprint(operation: QueryOperation) -> str:
        # Continuation tokens and a shrinking per-attempt deadline are not query
        # semantics. Keep every filter, order, projection and Resource in the pin.
        return replace(
            operation,
            page=replace(operation.page, cursor=None),
            budget=replace(operation.budget, deadline_ms=30_000),
        ).fingerprint

    def _keyset(
        self,
        operation: QueryOperation,
        context: TranslationContext,
        order: Sequence[tuple[BoundStatement, str, str]],
    ) -> BoundStatement:
        if self._cursor_signer is None or operation.page.cursor is None:
            raise ValueError("cursor pagination requires an Adapter-owned CursorSigner")
        payload = self._cursor_signer.verify(operation.page.cursor)
        if (
            payload.plan_fingerprint != self._cursor_plan_fingerprint(operation)
            or dict(payload.schema_fingerprints) != dict(context.schema_fingerprints)
            or payload.registry_fingerprint != context.registry_fingerprint
            or payload.scope_fingerprint != context.scope_fingerprint
            or payload.page_size != operation.page.size
            or len(payload.sort_tuple) != len(order)
        ):
            raise ValueError(
                "cursor does not match the current plan, Registry, Schema, scope, or order"
            )
        branches: list[BoundStatement] = []
        for index, ((expression, direction, nulls), value) in enumerate(
            zip(order, payload.sort_tuple, strict=True)
        ):
            equal: list[BoundStatement] = []
            for previous, cursor_value in zip(
                order[:index], payload.sort_tuple[:index], strict=True
            ):
                prior = previous[0]
                equal.append(
                    BoundStatement(
                        prior.command + sql.SQL(" IS NOT DISTINCT FROM %s"),
                        (*prior.parameters, cursor_value),
                    )
                )
            comparison = self._after(expression, direction, nulls, value)
            branches.append(conjunction((*equal, comparison)))
        return disjunction(branches)

    @staticmethod
    def _after(
        expression: BoundStatement, direction: str, nulls: str, value: object
    ) -> BoundStatement:
        operator = ">" if direction == "asc" else "<"
        if value is None:
            if nulls == "last":
                return BoundStatement(sql.SQL("FALSE"))
            return BoundStatement(
                expression.command + sql.SQL(" IS NOT NULL"),
                expression.parameters,
            )
        if nulls == "last":
            command = (
                expression.command
                + sql.SQL(" IS NULL OR (")
                + expression.command
                + sql.SQL(" IS NOT NULL AND ")
                + expression.command
                + sql.SQL(f" {operator} %s)")
            )
        else:
            command = (
                expression.command
                + sql.SQL(" IS NOT NULL AND ")
                + expression.command
                + sql.SQL(f" {operator} %s")
            )
        return BoundStatement(
            sql.SQL("(") + command + sql.SQL(")"),
            (
                *expression.parameters,
                *expression.parameters,
                *(expression.parameters if nulls == "last" else ()),
                value,
            ),
        )

    def _compile_traversal(
        self, operation: QueryOperation, context: TranslationContext
    ) -> CompiledQuery:
        traversal = operation.traversal
        assert traversal is not None
        relations = tuple(traversal.relation_collections)
        if not traversal.resolved or not relations:
            raise ValueError("all-neighbors traversal must be Registry-resolved before translation")
        if (
            traversal.all_neighbors
            and traversal.registry_fingerprint != context.registry_fingerprint
        ):
            raise ValueError("all-neighbors traversal Registry closure is stale")
        if traversal.record_filter is not None:
            raise ValueError("cross-Collection traversal record filters are unavailable in V1")
        if len(relations) > operation.budget.max_relation_resources:
            raise ValueError("traversal relation collection bound exceeded")
        if traversal.max_depth > min(
            operation.budget.max_traversal_depth,
            int(QUERY_CAPABILITIES.limits["traversalDepth"]),
        ):
            raise ValueError("traversal depth bound exceeded")
        edge_parts: list[sql.Composable] = []
        parameters: list[object] = []
        for ref in relations:
            layout = self.settings.layout(ref)
            if layout.relation is None:
                raise ValueError(f"traversal resource {ref} is not a Relation Collection")
            scope = self._scope_predicate("r", context)
            relation_predicate = traversal.relation_predicates.get(ref.canonical)
            if relation_predicate is not None:
                relation_compiler = _ExpressionCompiler(
                    self.settings,
                    {ref.canonical: "r"},
                    {ref.canonical: layout},
                )
                scope = conjunction((scope, relation_compiler.compile(relation_predicate)))
            if traversal.direction in {"outbound", "any"} or not layout.relation.directed:
                forward = (
                    sql.SQL(
                        "SELECT %s::text AS relation_ref, "
                        "r.__source_collection AS source_collection, "
                        "r.__source_record_id AS source_id, "
                        "r.__target_collection AS target_collection, "
                        "r.__target_record_id AS target_id FROM {} AS r WHERE "
                    ).format(ident(self.settings.physical_schema, layout.table))
                    + scope.command
                )
                edge_parts.append(forward)
                parameters.extend((ref.canonical, *scope.parameters))
            if traversal.direction in {"inbound", "any"} or not layout.relation.directed:
                reverse = (
                    sql.SQL(
                        "SELECT %s::text AS relation_ref, "
                        "r.__target_collection AS source_collection, "
                        "r.__target_record_id AS source_id, "
                        "r.__source_collection AS target_collection, "
                        "r.__source_record_id AS target_id FROM {} AS r WHERE "
                    ).format(ident(self.settings.physical_schema, layout.table))
                    + scope.command
                )
                edge_parts.append(reverse)
                parameters.extend((ref.canonical, *scope.parameters))
        start_collection = traversal.start.collection_ref.canonical
        import json

        start_id = json.dumps(
            traversal.start.to_dict()["recordId"], separators=(",", ":"), sort_keys=True
        )
        command = (
            sql.SQL("WITH RECURSIVE edges AS (")
            + sql.SQL(" UNION ALL ").join(edge_parts)
            + sql.SQL(
                "), walk(depth, collection_ref, record_id, path, relation_path) AS ("
                "SELECT 0, %s::text, %s::text, ARRAY[%s::text || E'\\x1f' || %s::text], "
                "ARRAY[]::text[] UNION ALL "
                "SELECT w.depth + 1, e.target_collection, e.target_id, "
                "w.path || (e.target_collection || E'\\x1f' || e.target_id), "
                "w.relation_path || e.relation_ref FROM walk AS w JOIN edges AS e "
                "ON e.source_collection = w.collection_ref AND e.source_id = w.record_id "
                "WHERE w.depth < %s AND NOT "
                "(e.target_collection || E'\\x1f' || e.target_id) = ANY(w.path)"
                ') SELECT collection_ref AS "collectionRef", record_id::jsonb AS "recordId", '
                'depth, path, relation_path AS "relationPath" FROM walk '
                "WHERE depth BETWEEN %s AND %s ORDER BY depth, collection_ref, record_id LIMIT %s"
            )
        )
        parameters.extend(
            (
                start_collection,
                start_id,
                start_collection,
                start_id,
                traversal.max_depth,
                traversal.min_depth,
                traversal.max_depth,
                min(operation.page.size, operation.budget.max_returned_paths) + 1,
            )
        )
        statement = BoundStatement(command, tuple(parameters))
        return self._compiled(
            context,
            statement,
            operation,
            extra={"pageSize": operation.page.size, "internalColumns": []},
        )

    def _compiled(
        self,
        context: TranslationContext,
        statement: BoundStatement,
        operation: QueryOperation,
        *,
        extra: Mapping[str, object],
    ) -> CompiledQuery:
        names = [f"p{index}" for index in range(len(statement.parameters))]
        command: dict[str, object] = {
            "formatVersion": "meridian.postgresql.command.v1",
            "sql": statement.command.as_string(None),
            "parameterOrder": names,
            "resultShape": operation.result.shape,
            "deadlineMs": context.deadline_ms,
            "maxResultBytes": operation.budget.max_normalized_bytes,
            **extra,
        }
        return CompiledQuery(
            adapter_id="postgresql",
            plan_fingerprint=context.plan_fingerprint,
            command=cast(Any, command),
            parameters={
                name: cast(Any, value)
                for name, value in zip(names, statement.parameters, strict=True)
            },
            expected_result_shape=operation.result.shape,
        )


__all__ = ["PostgreSQLQueryTranslator"]
