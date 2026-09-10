# SPDX-License-Identifier: Apache-2.0
"""Released Semantics 1.0.0 activation and logical-transfer facade."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from contextlib import AbstractContextManager
from typing import Any, cast

from meridian_storage.context import current_context
from meridian_storage.semantics import (
    ActivationPlan,
    ActivationResult,
    CollectionDocument,
    FrozenJson,
    JsonValue,
    RelationProfile,
    ResourceReference,
    SchemaDocument,
    SemanticsAdapter,
    canonical_json_bytes,
    sha256_fingerprint,
    validate_record,
    validate_schema,
)
from psycopg import Connection, sql
from psycopg.rows import dict_row

from .._settings import PostgreSQLSettings, ResourceLayout
from ..migration import LogicalTransfer, MigrationExecutor
from ..query._sql import ident
from ..query.dml import jsonable
from ..schema import SchemaCompiler

ConnectionProvider = Callable[[], AbstractContextManager[Connection[Any]]]

_EXPORT_FORMAT = "meridian.postgresql.logical-export.v1"
_PLAN_REQUIREMENTS = ("platform-migration-job", "postgis", "transactional-ddl")


class PostgreSQLSemanticsAdapter:
    """PostgreSQL implementation of the released provider-neutral Semantics SPI.

    The connection provider is deliberately supplied by the composition root. This
    object neither resolves credentials nor owns database lifecycle. Applying an
    activation is an explicit control-plane call; runtime startup never invokes it.
    """

    def __init__(
        self,
        settings: PostgreSQLSettings,
        connection_provider: ConnectionProvider,
    ) -> None:
        self.settings = settings
        self._connection_provider = connection_provider

    def validate_definition(
        self,
        schema: SchemaDocument,
        resource: CollectionDocument | None = None,
    ) -> None:
        validate_schema(schema)
        layout = self._resolve_layout(schema, resource)
        if schema.fingerprint != layout.schema_fingerprint:
            raise ValueError("Schema fingerprint does not match the Binding-pinned layout")
        if schema.semantic_kind.value != layout.profile:
            raise ValueError("Schema semantic kind does not match the physical profile")
        if tuple(schema.identity) != tuple(layout.identity):
            raise ValueError("Schema identity does not match the Binding-pinned layout")

        logical_fields = schema.field_map
        if set(logical_fields) != set(layout.field_map):
            raise ValueError("Schema fields do not match the Binding-pinned layout")
        for name, physical in layout.field_map.items():
            logical = logical_fields[name]
            if (
                logical.logical_type.kind.value != physical.logical_type
                or logical.cardinality.value != physical.cardinality
                or logical.nullable != physical.nullable
                or logical.mutable != physical.mutable
            ):
                raise ValueError(f"Schema field {name!r} differs from the pinned physical mapping")
            if physical.logical_type == "decimal" and (
                logical.logical_type.precision != physical.precision
                or logical.logical_type.scale != physical.scale
            ):
                raise ValueError(f"Schema decimal field {name!r} differs from its pinned mapping")

        logical_indexes = {
            (item.name, item.kind, tuple(item.fields), item.unique) for item in schema.indexes
        }
        physical_indexes = {
            (item.name, item.kind, tuple(item.fields), item.unique) for item in layout.indexes
        }
        if logical_indexes != physical_indexes:
            raise ValueError("Schema indexes do not match the Binding-pinned layout")
        self._validate_relation_profile(schema, layout)

    def plan_activation(
        self,
        resource: CollectionDocument,
        current_schema: SchemaDocument | None,
        target_schema: SchemaDocument,
    ) -> ActivationPlan:
        self.settings.require_writable()
        self.validate_definition(target_schema, resource)
        if resource.active_schema_ref != target_schema.ref:
            raise ValueError("Collection active Schema does not match the activation target")
        if current_schema is not None:
            validate_schema(current_schema)
            if current_schema.ref.schema_id != target_schema.ref.schema_id:
                raise ValueError("Schema activation cannot change logical Schema identity")
        layout = self.settings.layout(resource.ref.to_core())
        physical = SchemaCompiler(self.settings).compile()
        return ActivationPlan(
            resource_ref=resource.ref,
            current_schema_ref=None if current_schema is None else current_schema.ref,
            target_schema_ref=target_schema.ref,
            steps=(
                {
                    "kind": "activate-pinned-layout",
                    "resourceRef": resource.ref.canonical,
                    "schemaFingerprint": target_schema.fingerprint,
                },
            ),
            requirements=_PLAN_REQUIREMENTS,
            metadata={
                "adapterContract": "1.0.0",
                "migrationPlanFingerprint": physical.plan_fingerprint,
                "physicalFingerprint": physical.physical_fingerprint,
                "resourceFingerprint": layout.resource_fingerprint,
                "schemaFingerprint": target_schema.fingerprint,
                "statementCount": len(physical.statements),
            },
        )

    def apply_activation(self, plan: ActivationPlan) -> ActivationResult:
        self.settings.require_writable()
        layout = self.settings.layout(plan.resource_ref.to_core())
        physical = SchemaCompiler(self.settings).compile()
        expected_metadata: Mapping[str, FrozenJson] = {
            "adapterContract": "1.0.0",
            "migrationPlanFingerprint": physical.plan_fingerprint,
            "physicalFingerprint": physical.physical_fingerprint,
            "resourceFingerprint": layout.resource_fingerprint,
            "schemaFingerprint": layout.schema_fingerprint,
            "statementCount": len(physical.statements),
        }
        expected_steps: tuple[Mapping[str, FrozenJson], ...] = (
            {
                "kind": "activate-pinned-layout",
                "resourceRef": plan.resource_ref.canonical,
                "schemaFingerprint": layout.schema_fingerprint,
            },
        )
        if (
            dict(plan.metadata) != dict(expected_metadata)
            or tuple(dict(item) for item in plan.steps)
            != tuple(dict(item) for item in expected_steps)
            or plan.requirements != _PLAN_REQUIREMENTS
        ):
            raise ValueError("activation plan differs from the Binding-pinned migration")
        with self._connection_provider() as connection:
            evidence = MigrationExecutor(self.settings).apply(connection, physical)
            revision = self._read_registry_revision(connection)
        return ActivationResult(
            plan_fingerprint=plan.fingerprint,
            physical_fingerprint=evidence.physical_fingerprint,
            registry_revision=revision,
            provenance={
                "adapter": "postgresql",
                "migrationApplied": "true" if evidence.applied else "false",
                "migrationAuthority": "platform-iac",
            },
        )

    def read_registry_revision(self) -> str:
        with self._connection_provider() as connection:
            return self._read_registry_revision(connection)

    def encode_value(
        self,
        schema: SchemaDocument,
        value: Mapping[str, FrozenJson],
    ) -> object:
        self.validate_definition(schema)
        return validate_record(schema, cast(Mapping[str, object], value))

    def decode_value(
        self,
        schema: SchemaDocument,
        value: object,
    ) -> Mapping[str, FrozenJson]:
        self.validate_definition(schema)
        if not isinstance(value, Mapping):
            raise TypeError("encoded PostgreSQL Record must be a mapping")
        normalized = cast(Mapping[str, object], jsonable(value))
        return validate_record(schema, normalized)

    def export_logical(
        self,
        resources: Sequence[ResourceReference],
    ) -> Mapping[str, JsonValue]:
        context = current_context()
        assert context is not None
        if context.tenant is None:
            raise ValueError("logical export requires an OperationContext tenant")
        refs = tuple(ResourceReference.parse(item) for item in resources)
        canonical = tuple(item.canonical for item in refs)
        if len(set(canonical)) != len(canonical):
            raise ValueError("logical export Resources must be unique")
        for ref in canonical:
            self.settings.layout(ref)
        with self._connection_provider() as connection:
            records = tuple(
                json.loads(line)
                for line in LogicalTransfer(self.settings).export_rows(
                    connection,
                    canonical,
                    tenant=context.tenant,
                    scope=context.scope,
                )
            )
            revision = self._read_registry_revision(connection)
        payload: dict[str, JsonValue] = {
            "formatVersion": _EXPORT_FORMAT,
            "registryRevision": revision,
            "resources": [item.to_dict() for item in refs],
            "records": list(records),
        }
        # Fail closed if a future driver normalization produces non-JSON data.
        canonical_json_bytes(payload)
        return payload

    def import_logical(self, payload: Mapping[str, object]) -> None:
        self.settings.require_writable()
        if set(payload) != {"formatVersion", "registryRevision", "resources", "records"}:
            raise ValueError("logical import envelope contains unknown or missing fields")
        if payload["formatVersion"] != _EXPORT_FORMAT:
            raise ValueError("logical import format is unsupported")
        resources = payload["resources"]
        records = payload["records"]
        if (
            not isinstance(resources, Sequence)
            or isinstance(resources, (str, bytes))
            or not isinstance(records, Sequence)
            or isinstance(records, (str, bytes))
        ):
            raise TypeError("logical import Resources and Records must be arrays")
        refs = tuple(
            ResourceReference.parse(cast(Mapping[str, object], item))
            if isinstance(item, Mapping)
            else ResourceReference.parse(cast(str, item))
            for item in resources
        )
        allowed = {item.canonical for item in refs}
        if len(allowed) != len(refs):
            raise ValueError("logical import Resources must be unique")
        for ref in allowed:
            self.settings.layout(ref)
        encoded: list[str] = []
        for record in records:
            if not isinstance(record, Mapping) or set(record) != {"resource", "values"}:
                raise ValueError("logical import contains an invalid Record envelope")
            if record["resource"] not in allowed:
                raise ValueError("logical import Record references an undeclared Resource")
            encoded.append(
                json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
            )
        context = current_context()
        assert context is not None
        if context.tenant is None:
            raise ValueError("logical import requires an OperationContext tenant")
        with self._connection_provider() as connection:
            LogicalTransfer(self.settings).import_rows(
                connection,
                encoded,
                tenant=context.tenant,
                scope=context.scope,
            )

    def _resolve_layout(
        self,
        schema: SchemaDocument,
        resource: CollectionDocument | None,
    ) -> ResourceLayout:
        if resource is not None:
            if (
                resource.ref.catalog != schema.ref.catalog
                or resource.ref.namespace != schema.ref.namespace
            ):
                raise ValueError("Collection and Schema addresses are incompatible")
            layout = self.settings.layout(resource.ref.to_core())
            if resource.semantic_profile != layout.profile:
                raise ValueError("Collection semantic profile does not match its pinned layout")
            return layout
        matches = tuple(
            item
            for item in self.settings.resources.values()
            if item.schema_fingerprint == schema.fingerprint
        )
        if len(matches) != 1:
            raise ValueError("Schema does not resolve to exactly one Binding-pinned Resource")
        return matches[0]

    @staticmethod
    def _validate_relation_profile(schema: SchemaDocument, layout: ResourceLayout) -> None:
        profile = schema.profile
        if layout.relation is None:
            if isinstance(profile, RelationProfile):
                raise ValueError("Schema declares relation semantics for a non-relation layout")
            return
        if not isinstance(profile, RelationProfile):
            raise ValueError("relation layout requires a released Relation profile")
        if (
            profile.source_field != layout.relation.source_field
            or profile.target_field != layout.relation.target_field
            or profile.directed != layout.relation.directed
            or tuple(item.canonical for item in profile.source_collections)
            != layout.relation.source_collections
            or tuple(item.canonical for item in profile.target_collections)
            != layout.relation.target_collections
        ):
            raise ValueError("Relation profile differs from its Binding-pinned collection set")

    def _read_registry_revision(self, connection: Connection[Any]) -> str:
        table = ident(self.settings.physical_schema, "__meridian_resources")
        with connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                sql.SQL(
                    "SELECT resource_ref, resource_fingerprint, schema_fingerprint, profile, "
                    "physical_fingerprint FROM {} ORDER BY resource_ref"
                ).format(table)
            )
            rows = [dict(row) for row in cursor.fetchall()]
        if not rows:
            raise RuntimeError("physical registry metadata is empty")
        return sha256_fingerprint(cast(JsonValue, rows))

__all__ = ["ConnectionProvider", "PostgreSQLSemanticsAdapter", "SemanticsAdapter"]
