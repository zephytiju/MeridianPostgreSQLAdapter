# SPDX-License-Identifier: Apache-2.0
"""Core Operation dispatch without exposing engine concepts to consumers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, cast

from meridian_storage.query.adapter import CompiledQuery, TranslationContext
from meridian_storage.query.ast import (
    Aggregate,
    Field,
    NamedAggregate,
    Projection,
    Sort,
    full_text,
    parse_filter,
)
from meridian_storage.query.wire import (
    PageSpec,
    QueryOperation,
    QueryTarget,
    ResultSpec,
    SafetyBudget,
    TraversalSpec,
)
from meridian_storage.semantics import RecordReference, SchemaDocument, sha256_fingerprint
from meridian_storage.spi.adapters import ExecutionRequest

from ._settings import PostgreSQLSettings
from .query import PostgreSQLQueryTranslator
from .query.dml import AppendBatchCommand, DMLCommand, DMLCompiler


@dataclass(frozen=True, slots=True)
class QueryCommand:
    compiled: CompiledQuery
    translator: PostgreSQLQueryTranslator


@dataclass(frozen=True, slots=True)
class MetadataPublishCommand:
    document: SchemaDocument
    expected_revision: int | None
    allow_breaking: bool


type AdapterCommand = AppendBatchCommand | DMLCommand | QueryCommand | MetadataPublishCommand


class OperationCompiler:
    def __init__(
        self,
        settings: PostgreSQLSettings,
        translator: PostgreSQLQueryTranslator,
    ) -> None:
        self.settings = settings
        self.translator = translator
        self.dml = DMLCompiler(settings)

    def compile(self, request: ExecutionRequest) -> AdapterCommand:
        operation = request.operation
        if operation.catalog not in {"structured", "evidence"}:
            raise ValueError("PostgreSQL V1 implements structured and evidence operations")
        prefix = f"meridian.{operation.catalog}."
        if not operation.operation_contract.startswith(prefix):
            raise ValueError("Operation contract does not match its Catalog")
        method = operation.operation_contract.removeprefix(prefix)
        allowed = {
            "structured": {
                "aggregate",
                "create_resource",
                "delete",
                "get",
                "patch",
                "publish_schema",
                "put",
                "query",
                "search",
                "traverse",
            },
            "evidence": {"append", "query"},
        }
        if method not in allowed[operation.catalog]:
            raise ValueError(f"unsupported {operation.catalog} Operation contract: {method!r}")
        version = "2.0.0" if (operation.catalog, method) == ("structured", "put") else "1.0.0"
        if operation.operation_version != version:
            raise ValueError(f"{operation.operation_contract} requires Operation version {version}")
        if method == "put" and "queryPlan" in operation.input:
            raise ValueError("structured.put cannot contain a queryPlan")
        if method == "create_resource":
            raise ValueError("physical DDL is only available through the Platform migration hook")
        if len(operation.resources) < 1:
            raise ValueError("PostgreSQL Operation requires a Resource")
        if any(resource.catalog != operation.catalog for resource in operation.resources):
            raise ValueError("Operation Resources must belong to its Catalog")
        if (
            method != "traverse"
            and "queryPlan" not in operation.input
            and len(operation.resources) != 1
        ):
            raise ValueError("non-traversal Operations require exactly one Resource")
        if method == "publish_schema":
            return self._publish_schema(request)
        if any(
            self.settings.layout(ref).profile == "metadata-registry" for ref in operation.resources
        ):
            raise ValueError(
                "metadata registry only supports Schema publication; use SchemaAPI for reads"
            )
        if (
            method in {"put", "get", "patch", "delete", "append"}
            and "queryPlan" not in operation.input
        ):
            return self.dml.compile(
                method,
                operation.resources[0],
                cast(Mapping[str, object], operation.input),
                request.context,
            )
        self.dml._scope_values(request.context)
        query_operation = self._query_operation(request, method)
        if set(query_operation.resources) != set(operation.resources):
            raise ValueError("query plan Resources differ from the enclosing Operation")
        context = TranslationContext(
            binding_id=request.binding_id,
            plan_fingerprint=query_operation.fingerprint,
            registry_fingerprint=request.registry_fingerprint,
            schema_fingerprints={
                ref.canonical: self.settings.layout(ref).schema_fingerprint
                for ref in query_operation.resources
            },
            scope_fingerprint=self._scope_fingerprint(request),
            deadline_ms=self._deadline_ms(request, query_operation.budget.deadline_ms),
        )
        return QueryCommand(self.translator.compile(query_operation, context), self.translator)

    def _publish_schema(self, request: ExecutionRequest) -> MetadataPublishCommand:
        operation = request.operation
        self.dml._scope_values(request.context)
        if (
            operation.catalog != "structured"
            or operation.read_only
            or len(operation.resources) != 1
            or operation.resources[0].canonical != "structured:meridian.registry"
            or self.settings.layout(operation.resources[0]).profile != "metadata-registry"
        ):
            raise ValueError("Schema publication requires the pinned structured metadata registry")
        values = operation.input
        required = {"namespace", "name", "version", "definition", "allowBreaking"}
        if required - set(values) or set(values) - (required | {"expectedRegistryRevision"}):
            raise ValueError("Schema publication contains unknown or missing arguments")
        if (
            any(not isinstance(values[key], str) for key in ("namespace", "name", "version"))
            or not isinstance(values["definition"], Mapping)
            or type(values["allowBreaking"]) is not bool
        ):
            raise ValueError("Schema publication arguments have invalid types")
        expected = values.get("expectedRegistryRevision")
        if expected is not None and (type(expected) is not int or expected < 0):
            raise ValueError("Schema expected revision must be a nonnegative integer")
        document = SchemaDocument.from_definition(
            catalog="structured",
            namespace=cast(str, values["namespace"]),
            name=cast(str, values["name"]),
            version=cast(str, values["version"]),
            definition=cast(Mapping[str, object], values["definition"]),
        )
        return MetadataPublishCommand(document, expected, values["allowBreaking"])

    def _query_operation(self, request: ExecutionRequest, method: str) -> QueryOperation:
        raw_plan = request.operation.input.get("queryPlan")
        if raw_plan is not None:
            if not isinstance(raw_plan, Mapping):
                raise TypeError("queryPlan must be a released QueryOperation mapping")
            return QueryOperation.from_mapping(cast(Mapping[str, object], raw_plan))
        values = cast(Mapping[str, object], request.operation.input)
        resource = request.operation.resources[0]
        layout = self.settings.layout(resource)
        target = QueryTarget(resource)
        where = values.get("where", {})
        if not isinstance(where, Mapping):
            raise TypeError("structured query where must be an object")
        predicate = parse_filter(where)
        configured_result_limit = request.operation.input.get("resultByteLimit", 16 * 1024 * 1024)
        result_limit = (
            configured_result_limit
            if isinstance(configured_result_limit, int)
            and not isinstance(configured_result_limit, bool)
            else 16 * 1024 * 1024
        )
        budget = SafetyBudget(
            deadline_ms=self._deadline_ms(request, 30_000),
            max_result_values=10_000,
            max_normalized_bytes=min(result_limit, 16 * 1024 * 1024),
        )
        if method == "query":
            return self._structured_query(values, target, predicate, budget, layout)
        if method == "search":
            return self._structured_search(values, target, predicate, budget, layout)
        if method == "aggregate":
            return self._structured_aggregate(values, target, predicate, budget)
        if method == "traverse":
            return self._structured_traverse(request, values, target, budget)
        raise ValueError(f"unsupported {request.operation.catalog} Operation contract: {method!r}")

    @staticmethod
    def _structured_query(
        values: Mapping[str, object],
        target: QueryTarget,
        predicate: Any,
        budget: SafetyBudget,
        layout: Any,
    ) -> QueryOperation:
        select = values.get("select", ())
        order_by = values.get("orderBy", ())
        limit = values.get("limit", 50)
        if (
            not isinstance(select, Sequence)
            or isinstance(select, (str, bytes))
            or not isinstance(order_by, Sequence)
            or isinstance(order_by, (str, bytes))
            or isinstance(limit, bool)
            or not isinstance(limit, int)
        ):
            raise TypeError("structured query select, orderBy, or limit has an invalid type")
        projection = tuple(Projection(Field(cast(str, name)), cast(str, name)) for name in select)
        sorts: list[Sort] = []
        for item in order_by:
            if not isinstance(item, Mapping) or set(item) - {"field", "direction", "nulls"}:
                raise ValueError("orderBy entries require field/direction/nulls")
            field_name = item.get("field")
            if not isinstance(field_name, str) or field_name not in layout.field_map:
                raise ValueError("orderBy references an unknown field")
            sorts.append(
                Sort(
                    Field(field_name),
                    cast(str, item.get("direction", "asc")),
                    cast(str, item.get("nulls", "last")),
                )
            )
        return QueryOperation(
            catalog=target.resource.catalog,
            targets=(target,),
            operation="scan",
            result=ResultSpec("records", projection),
            filter=predicate,
            order=tuple(sorts),
            page=PageSpec(limit, cast(str | None, values.get("cursor"))),
            budget=budget,
        )

    @staticmethod
    def _structured_search(
        values: Mapping[str, object],
        target: QueryTarget,
        predicate: Any,
        budget: SafetyBudget,
        layout: Any,
    ) -> QueryOperation:
        if values.get("facets") or values.get("highlights"):
            raise ValueError("this V1 profile does not advertise facets or highlights")
        full_text_fields = tuple(
            field for index in layout.indexes if index.kind == "full-text" for field in index.fields
        )
        if not full_text_fields:
            raise ValueError("search requires a pinned full-text index")
        query = values.get("query")
        if isinstance(query, Mapping):
            text = query.get("text")
            selected = query.get("fields", full_text_fields)
            if not isinstance(selected, Sequence) or isinstance(selected, (str, bytes)):
                raise TypeError("search fields must be an array")
            fields = tuple(cast(str, item) for item in selected)
        else:
            text = query
            fields = full_text_fields
        if not isinstance(text, str):
            raise TypeError("search query text must be a string")
        if (
            not fields
            or len(set(fields)) != len(fields)
            or any(field not in full_text_fields for field in fields)
        ):
            raise ValueError("search fields must be unique and backed by pinned full-text indexes")
        match = full_text(text, fields=fields)
        combined = match if predicate is None else predicate.and_(match)
        limit = values.get("limit", 50)
        if isinstance(limit, bool) or not isinstance(limit, int):
            raise TypeError("search limit must be an integer")
        return QueryOperation(
            catalog=target.resource.catalog,
            targets=(target,),
            operation="search",
            result=ResultSpec("search"),
            filter=combined,
            page=PageSpec(limit, cast(str | None, values.get("cursor"))),
            budget=budget,
        )

    @staticmethod
    def _structured_aggregate(
        values: Mapping[str, object],
        target: QueryTarget,
        predicate: Any,
        budget: SafetyBudget,
    ) -> QueryOperation:
        raw_grouping = values.get("groupBy", ())
        raw_metrics = values.get("metrics", ())
        if (
            not isinstance(raw_grouping, Sequence)
            or isinstance(raw_grouping, (str, bytes))
            or not isinstance(raw_metrics, Sequence)
            or isinstance(raw_metrics, (str, bytes))
        ):
            raise TypeError("aggregate groupBy and metrics must be arrays")
        grouping = tuple(Field(cast(str, name)) for name in raw_grouping)
        aggregates: list[NamedAggregate] = []
        for metric in raw_metrics:
            if not isinstance(metric, Mapping) or set(metric) - {
                "name",
                "function",
                "field",
                "distinct",
            }:
                raise ValueError("aggregate metric is not a closed V1 metric")
            name = metric.get("name")
            function = metric.get("function")
            field_name = metric.get("field")
            if not isinstance(name, str) or not isinstance(function, str):
                raise TypeError("aggregate name and function must be strings")
            distinct = metric.get("distinct", False)
            if not isinstance(distinct, bool):
                raise TypeError("aggregate distinct must be a boolean")
            operand = None if field_name is None else Field(cast(str, field_name))
            aggregates.append(
                NamedAggregate(
                    name,
                    Aggregate(function, operand, distinct),
                )
            )
        return QueryOperation(
            catalog="structured",
            targets=(target,),
            operation="aggregate",
            result=ResultSpec("aggregate"),
            filter=predicate,
            grouping=grouping,
            aggregates=tuple(aggregates),
            budget=budget,
        )

    @staticmethod
    def _structured_traverse(
        request: ExecutionRequest,
        values: Mapping[str, object],
        target: QueryTarget,
        budget: SafetyBudget,
    ) -> QueryOperation:
        start = values.get("start")
        if not isinstance(start, Mapping):
            raise TypeError("traversal start must be a RecordReference")
        relations = tuple(request.operation.resources[1:])
        all_neighbors = values.get("allNeighbors") is True
        registry_fingerprint = (
            cast(str, values.get("registryFingerprint")) if all_neighbors else None
        )
        max_depth = values.get("maxDepth", 1)
        if isinstance(max_depth, bool) or not isinstance(max_depth, int):
            raise TypeError("traversal maxDepth must be an integer")
        traversal = TraversalSpec(
            start=RecordReference.from_mapping(cast(Mapping[str, object], start)),
            relation_collections=relations,
            all_neighbors=all_neighbors,
            max_depth=max_depth,
            result_shape="records",
            registry_fingerprint=registry_fingerprint,
        )
        return QueryOperation(
            catalog="structured",
            targets=(target,),
            operation="traverse",
            result=ResultSpec("records"),
            traversal=traversal,
            page=PageSpec(min(budget.max_returned_paths, 500)),
            budget=budget,
        )

    def _scope_fingerprint(self, request: ExecutionRequest) -> str:
        return sha256_fingerprint(
            {
                "tenant": request.context.tenant,
                "scope": dict(sorted(request.context.scope.items())),
            }
        )

    @staticmethod
    def _deadline_ms(request: ExecutionRequest, default: int) -> int:
        remaining = request.context.remaining_seconds()
        if remaining is None:
            return default
        return max(1, min(default, int(remaining * 1000)))


__all__ = ["AdapterCommand", "OperationCompiler", "QueryCommand"]
