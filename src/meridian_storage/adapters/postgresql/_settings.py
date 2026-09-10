# SPDX-License-Identifier: Apache-2.0
"""Closed adapter-owned settings parsed from a Meridian Binding."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from types import MappingProxyType
from typing import Any, cast

from meridian_storage.registry.resources import (
    CapabilityRequirement,
    ResourceDefinition,
    ResourceRef,
    SchemaRef,
)

_IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")
_FINGERPRINT = re.compile(r"^sha256:[0-9a-f]{64}$")
_PROFILES = frozenset(
    {
        "postgresql-postgis-local-single-primary",
        "postgresql-postgis-cluster",
    }
)
_LOGICAL_TYPES = frozenset(
    {
        "boolean",
        "bytes",
        "date",
        "decimal",
        "duration",
        "enum",
        "float64",
        "int8",
        "int16",
        "int32",
        "int64",
        "json",
        "objectRef",
        "recordRef",
        "string",
        "utcTimestamp",
        "uuid",
        "wgs84Point",
    }
)
_STRUCTURED_PROFILES = frozenset(
    {"relational", "document", "key-value", "search", "geospatial", "time-series", "relation"}
)
_RESERVED_TABLES = frozenset(
    {
        "__meridian_migrations",
        "__meridian_resources",
        "__meridian_evidence_replay",
        "__meridian_outbox_state",
        "__meridian_outbox_checkpoint",
        "__meridian_schema_registry",
        "__meridian_schema_registry_migration",
    }
)
_RESERVED_COLUMNS = frozenset(
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


def identifier(value: object, path: str) -> str:
    if not isinstance(value, str) or _IDENTIFIER.fullmatch(value) is None:
        raise ValueError(f"{path} must be a lower-case PostgreSQL identifier")
    return value


def fingerprint(value: object, path: str) -> str:
    if not isinstance(value, str) or _FINGERPRINT.fullmatch(value) is None:
        raise ValueError(f"{path} must be a sha256 fingerprint")
    return value


def _mapping(value: object, path: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise TypeError(f"{path} must be an object")
    return cast(Mapping[str, object], value)


def _sequence(value: object, path: str) -> Sequence[object]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise TypeError(f"{path} must be an array")
    return cast(Sequence[object], value)


def _closed(
    value: object,
    path: str,
    *,
    required: frozenset[str],
    optional: frozenset[str] = frozenset(),
) -> Mapping[str, object]:
    result = _mapping(value, path)
    missing = required - set(result)
    unknown = set(result) - required - optional
    if missing or unknown:
        raise ValueError(f"{path} has missing={sorted(missing)!r}, unknown={sorted(unknown)!r}")
    return result


@dataclass(frozen=True, slots=True)
class FieldLayout:
    name: str
    column: str
    logical_type: str
    nullable: bool = False
    mutable: bool = True
    cardinality: str = "one"
    precision: int | None = None
    scale: int | None = None

    @classmethod
    def from_mapping(cls, value: object, path: str) -> FieldLayout:
        item = _closed(
            value,
            path,
            required=frozenset({"name", "column", "logicalType", "nullable", "mutable"}),
            optional=frozenset({"cardinality", "precision", "scale"}),
        )
        name = item["name"]
        logical_type = item["logicalType"]
        if not isinstance(name, str) or not name:
            raise ValueError(f"{path}.name must be non-empty")
        if not isinstance(logical_type, str) or logical_type not in _LOGICAL_TYPES:
            raise ValueError(f"{path}.logicalType is unsupported")
        nullable = item["nullable"]
        mutable = item["mutable"]
        if not isinstance(nullable, bool) or not isinstance(mutable, bool):
            raise TypeError(f"{path}.nullable and mutable must be booleans")
        cardinality = item.get("cardinality", "one")
        if cardinality not in {"one", "many"}:
            raise ValueError(f"{path}.cardinality must be one or many")
        precision = item.get("precision")
        scale = item.get("scale")
        if precision is not None and (
            isinstance(precision, bool) or not isinstance(precision, int)
        ):
            raise TypeError(f"{path}.precision must be an integer")
        if scale is not None and (isinstance(scale, bool) or not isinstance(scale, int)):
            raise TypeError(f"{path}.scale must be an integer")
        if logical_type == "decimal":
            if (
                precision is None
                or scale is None
                or not 1 <= precision <= 1000
                or not 0 <= scale <= precision
            ):
                raise ValueError(f"{path} decimal requires valid precision and scale")
        elif precision is not None or scale is not None:
            raise ValueError(f"{path} precision and scale are valid only for decimal")
        return cls(
            name=name,
            column=identifier(item["column"], f"{path}.column"),
            logical_type=logical_type,
            nullable=nullable,
            mutable=mutable,
            cardinality=cardinality,
            precision=precision,
            scale=scale,
        )


@dataclass(frozen=True, slots=True)
class IndexLayout:
    name: str
    kind: str
    fields: tuple[str, ...]
    unique: bool = False

    @classmethod
    def from_mapping(cls, value: object, path: str) -> IndexLayout:
        item = _closed(
            value,
            path,
            required=frozenset({"name", "kind", "fields", "unique"}),
        )
        kind = item["kind"]
        if kind not in {
            "btree",
            "hash",
            "full-text",
            "geospatial",
            "relation-endpoint",
            "time-series",
        }:
            raise ValueError(f"{path}.kind is unsupported")
        fields = tuple(cast(str, entry) for entry in _sequence(item["fields"], f"{path}.fields"))
        if not fields or len(set(fields)) != len(fields) or any(not field for field in fields):
            raise ValueError(f"{path}.fields must be non-empty and unique")
        if not isinstance(item["unique"], bool):
            raise TypeError(f"{path}.unique must be a boolean")
        unique = item["unique"]
        if unique and kind != "btree":
            raise ValueError(f"{path}.unique is supported only for btree indexes")
        return cls(
            identifier(item["name"], f"{path}.name"),
            kind,
            fields,
            unique,
        )


@dataclass(frozen=True, slots=True)
class RelationLayout:
    source_field: str
    target_field: str
    directed: bool
    source_collections: tuple[str, ...]
    target_collections: tuple[str, ...]

    @classmethod
    def from_mapping(cls, value: object, path: str) -> RelationLayout:
        item = _closed(
            value,
            path,
            required=frozenset(
                {"sourceField", "targetField", "directed", "sourceCollections", "targetCollections"}
            ),
        )
        source = item["sourceField"]
        target = item["targetField"]
        if not isinstance(source, str) or not isinstance(target, str) or source == target:
            raise ValueError(f"{path} requires distinct source and target fields")
        if not isinstance(item["directed"], bool):
            raise TypeError(f"{path}.directed must be a boolean")
        sources = tuple(
            sorted(
                cast(str, entry)
                for entry in _sequence(item["sourceCollections"], f"{path}.sourceCollections")
            )
        )
        targets = tuple(
            sorted(
                cast(str, entry)
                for entry in _sequence(item["targetCollections"], f"{path}.targetCollections")
            )
        )
        for ref in (*sources, *targets):
            ResourceRef.parse(ref, catalog="structured")
        if (
            not sources
            or not targets
            or len(set(sources)) != len(sources)
            or len(set(targets)) != len(targets)
        ):
            raise ValueError(f"{path} endpoint collections must be non-empty and unique")
        return cls(source, target, item["directed"], sources, targets)


@dataclass(frozen=True, slots=True)
class ResourceLayout:
    ref: ResourceRef
    table: str
    profile: str
    schema_fingerprint: str
    resource_fingerprint: str
    fields: tuple[FieldLayout, ...]
    identity: tuple[str, ...]
    indexes: tuple[IndexLayout, ...] = ()
    relation: RelationLayout | None = None

    def __post_init__(self) -> None:
        if self.profile == "metadata-registry":
            if (
                self.ref.canonical != "structured:meridian.registry"
                or self.table != "__meridian_schema_registry"
                or self.fields
                or self.identity
                or self.indexes
                or self.relation is not None
            ):
                raise ValueError(
                    "metadata registry requires its fixed Resource/table and no data layout"
                )
            return
        field_names = {field.name for field in self.fields}
        columns = {field.column for field in self.fields}
        if len(field_names) != len(self.fields) or len(columns) != len(self.fields):
            raise ValueError(f"{self.ref}: logical fields and physical columns must be unique")
        if (
            not self.identity
            or len(set(self.identity)) != len(self.identity)
            or not set(self.identity) <= field_names
        ):
            raise ValueError(f"{self.ref}: identity must reference declared fields")
        if self.ref.catalog not in {"structured", "evidence"}:
            raise ValueError(f"{self.ref}: PostgreSQL supports only structured and evidence")
        if self.table in _RESERVED_TABLES:
            raise ValueError(f"{self.ref}: table name is reserved for Adapter metadata")
        if any(column in _RESERVED_COLUMNS or column.startswith("__scope_") for column in columns):
            raise ValueError(f"{self.ref}: field column collides with an Adapter-owned column")
        if any(
            self.field_map[name].nullable
            or self.field_map[name].mutable
            or self.field_map[name].cardinality != "one"
            for name in self.identity
        ):
            raise ValueError(f"{self.ref}: identity fields must be immutable non-null scalars")
        if len({index.name for index in self.indexes}) != len(self.indexes):
            raise ValueError(f"{self.ref}: index names must be unique")
        for index in self.indexes:
            if not set(index.fields) <= field_names:
                raise ValueError(f"{self.ref}: index references an undeclared field")
            selected = tuple(self.field_map[name] for name in index.fields)
            if index.kind == "full-text" and (
                len(selected) != 1
                or selected[0].logical_type != "string"
                or selected[0].cardinality != "one"
            ):
                raise ValueError(f"{self.ref}: full-text index requires one scalar string field")
            if index.kind == "geospatial" and (
                len(selected) != 1
                or selected[0].logical_type != "wgs84Point"
                or selected[0].cardinality != "one"
            ):
                raise ValueError(f"{self.ref}: geospatial index requires one WGS84 point field")
            if index.kind == "relation-endpoint" and self.relation is None:
                raise ValueError(f"{self.ref}: relation endpoint index requires a relation layout")
            if index.kind == "relation-endpoint" and (
                len(index.fields) != 1
                or self.relation is None
                or index.fields[0] not in {self.relation.source_field, self.relation.target_field}
            ):
                raise ValueError(f"{self.ref}: relation endpoint index must select one endpoint")
        if self.ref.catalog == "structured" and self.profile not in _STRUCTURED_PROFILES:
            raise ValueError(f"{self.ref}: structured profile is unsupported")
        if self.ref.catalog == "evidence" and self.profile != "append-only-evidence":
            raise ValueError(f"{self.ref}: evidence profile must be append-only-evidence")
        if (
            self.relation is not None
            and not {
                self.relation.source_field,
                self.relation.target_field,
            }
            <= field_names
        ):
            raise ValueError(f"{self.ref}: relation endpoints must reference declared fields")
        if (self.relation is None) != (self.profile != "relation"):
            raise ValueError(f"{self.ref}: relation profile and endpoint mapping must agree")
        if self.relation is not None:
            endpoints = (
                self.field_map[self.relation.source_field],
                self.field_map[self.relation.target_field],
            )
            if any(
                field.logical_type != "recordRef" or field.cardinality != "one" or field.nullable
                for field in endpoints
            ):
                raise ValueError(f"{self.ref}: relation endpoints must be non-null RecordRefs")

    @classmethod
    def from_mapping(cls, value: object, path: str) -> ResourceLayout:
        item = _closed(
            value,
            path,
            required=frozenset(
                {
                    "ref",
                    "table",
                    "profile",
                    "schemaFingerprint",
                    "resourceFingerprint",
                    "fields",
                    "identity",
                    "indexes",
                    "relation",
                }
            ),
        )
        fields = tuple(
            FieldLayout.from_mapping(entry, f"{path}.fields[{index}]")
            for index, entry in enumerate(_sequence(item["fields"], f"{path}.fields"))
        )
        indexes = tuple(
            IndexLayout.from_mapping(entry, f"{path}.indexes[{index}]")
            for index, entry in enumerate(_sequence(item["indexes"], f"{path}.indexes"))
        )
        identity_values = tuple(
            cast(str, entry) for entry in _sequence(item["identity"], f"{path}.identity")
        )
        relation = item["relation"]
        return cls(
            ref=ResourceRef.parse(cast(str | Mapping[str, object], item["ref"])),
            table=identifier(item["table"], f"{path}.table"),
            profile=cast(str, item["profile"]),
            schema_fingerprint=fingerprint(item["schemaFingerprint"], f"{path}.schemaFingerprint"),
            resource_fingerprint=fingerprint(
                item["resourceFingerprint"], f"{path}.resourceFingerprint"
            ),
            fields=fields,
            identity=identity_values,
            indexes=indexes,
            relation=None
            if relation is None
            else RelationLayout.from_mapping(relation, f"{path}.relation"),
        )

    @property
    def field_map(self) -> Mapping[str, FieldLayout]:
        return MappingProxyType({field.name: field for field in self.fields})


def _plain_json(value: object) -> object:
    if isinstance(value, Mapping):
        return {key: _plain_json(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_plain_json(item) for item in value]
    return value


def _resource_definition(value: object, path: str) -> ResourceDefinition:
    """Parse the exact public Core serialization; never accept opaque hash aliases."""
    item = _closed(
        value,
        path,
        required=frozenset(
            {
                "formatVersion",
                "ref",
                "profile",
                "schema",
                "labels",
                "requirements",
                "requiredScope",
                "relatedResources",
                "extensions",
            }
        ),
    )
    resource = ResourceDefinition(
        ref=ResourceRef.parse(_mapping(item["ref"], f"{path}.ref")),
        profile=cast(str, item["profile"]),
        schema=None
        if item["schema"] is None
        else SchemaRef.parse(_mapping(item["schema"], f"{path}.schema")),
        labels=cast(Mapping[str, str], _mapping(item["labels"], f"{path}.labels")),
        requirements=tuple(
            CapabilityRequirement.from_mapping(_mapping(entry, f"{path}.requirements[]"))
            for entry in _sequence(item["requirements"], f"{path}.requirements")
        ),
        required_scope=cast(
            tuple[str, ...], tuple(_sequence(item["requiredScope"], f"{path}.requiredScope"))
        ),
        related_resources=tuple(
            ResourceRef.parse(_mapping(entry, f"{path}.relatedResources[]"))
            for entry in _sequence(item["relatedResources"], f"{path}.relatedResources")
        ),
        extensions=cast(Any, _mapping(item["extensions"], f"{path}.extensions")),
    )
    if resource.to_dict() != _plain_json(item):
        raise ValueError(f"{path} must use the canonical ResourceDefinition serialization")
    return resource


@dataclass(frozen=True, slots=True)
class ReadCompatibility:
    stored_resource: ResourceDefinition
    reader_resource: ResourceDefinition

    def __post_init__(self) -> None:
        stored = self.stored_resource
        if stored.ref.catalog != "structured" or stored.profile not in _STRUCTURED_PROFILES:
            raise ValueError("read compatibility requires a structured data Resource")
        put = next(
            (
                item
                for item in stored.requirements
                if item.operation_contract == "meridian.structured.put"
            ),
            None,
        )
        if put is None or put.operation_version != "1.0.0":
            raise ValueError("read compatibility requires stored structured.put 1.0.0")
        expected = replace(
            stored,
            requirements=tuple(
                replace(item, operation_version="2.0.0") if item is put else item
                for item in stored.requirements
            ),
        )
        # Core's canonical hash preserves JSON type distinctions (True != 1),
        # unlike Python mapping equality.
        if expected.fingerprint != self.reader_resource.fingerprint:
            raise ValueError("read compatibility permits only structured.put 1.0.0 to 2.0.0")

    @classmethod
    def from_mapping(cls, value: object, path: str) -> ReadCompatibility:
        item = _closed(value, path, required=frozenset({"storedResource", "readerResource"}))
        return cls(
            _resource_definition(item["storedResource"], f"{path}.storedResource"),
            _resource_definition(item["readerResource"], f"{path}.readerResource"),
        )


@dataclass(frozen=True, slots=True)
class PostgreSQLSettings:
    physical_schema: str
    engine_profile: str
    scope_keys: tuple[str, ...]
    resources: Mapping[str, ResourceLayout]
    application_name: str = "meridian-storage-postgresql"
    expected_standbys: int = 0
    require_tls: bool = False
    read_compatibility: Mapping[str, ReadCompatibility] = field(
        default_factory=lambda: MappingProxyType({})
    )

    @property
    def read_only(self) -> bool:
        return bool(self.read_compatibility)

    def require_writable(self) -> None:
        if self.read_only:
            raise ValueError("read compatibility Binding is read-only")

    @classmethod
    def from_binding(cls, binding: Any) -> PostgreSQLSettings:
        if binding.engine_profile not in _PROFILES:
            raise ValueError(f"unsupported PostgreSQL Engine profile: {binding.engine_profile!r}")
        raw = _closed(
            binding.settings,
            "binding.settings",
            required=frozenset({"formatVersion", "scopeKeys", "resources", "topology"}),
            optional=frozenset({"applicationName", "readCompatibility"}),
        )
        if raw["formatVersion"] != "meridian.postgresql.settings.v1":
            raise ValueError("binding.settings.formatVersion is unsupported")
        scope_keys = tuple(
            sorted(
                identifier(item, "binding.settings.scopeKeys[]")
                for item in _sequence(raw["scopeKeys"], "binding.settings.scopeKeys")
            )
        )
        if len(set(scope_keys)) != len(scope_keys):
            raise ValueError("scope keys must be unique")
        layouts = tuple(
            ResourceLayout.from_mapping(entry, f"binding.settings.resources[{index}]")
            for index, entry in enumerate(_sequence(raw["resources"], "binding.settings.resources"))
        )
        if not layouts or len({layout.ref.canonical for layout in layouts}) != len(layouts):
            raise ValueError("resource layouts must be non-empty and unique")
        if len({layout.table for layout in layouts}) != len(layouts):
            raise ValueError("resource layouts must use unique physical tables")
        compatibility: dict[str, ReadCompatibility] = {}
        if "readCompatibility" in raw:
            fingerprint(
                binding.required_physical_fingerprint, "binding.requiredPhysicalFingerprint"
            )
            selected = _closed(
                raw["readCompatibility"],
                "binding.settings.readCompatibility",
                required=frozenset({"formatVersion", "resources"}),
            )
            if selected["formatVersion"] != "meridian.postgresql.read-compatibility.v1":
                raise ValueError("readCompatibility.formatVersion is unsupported")
            for entry in _sequence(selected["resources"], "readCompatibility.resources"):
                proof = ReadCompatibility.from_mapping(entry, "readCompatibility.resources[]")
                ref = proof.reader_resource.ref.canonical
                if ref in compatibility:
                    raise ValueError("read compatibility Resources must be unique")
                compatibility[ref] = proof
            if set(compatibility) != {layout.ref.canonical for layout in layouts}:
                raise ValueError("read compatibility must cover exactly every Binding Resource")
            for layout in layouts:
                reader = compatibility[layout.ref.canonical].reader_resource
                if (
                    reader.fingerprint != layout.resource_fingerprint
                    or reader.profile != layout.profile
                    or not set(reader.required_scope) <= set(scope_keys)
                ):
                    raise ValueError("read compatibility differs from the pinned layout or scope")
        topology = _closed(
            raw["topology"],
            "binding.settings.topology",
            required=frozenset({"expectedStandbys"}),
        )
        expected = topology["expectedStandbys"]
        if isinstance(expected, bool) or not isinstance(expected, int) or expected < 0:
            raise TypeError("expectedStandbys must be a non-negative integer")
        if binding.engine_profile.endswith("local-single-primary") and expected != 0:
            raise ValueError("local single-primary profile cannot expect standbys")
        if binding.engine_profile.endswith("cluster") and expected < 2:
            raise ValueError("cluster profile requires at least two expected standbys")
        application_name = raw.get("applicationName", "meridian-storage-postgresql")
        if (
            not isinstance(application_name, str)
            or not application_name
            or len(application_name) > 63
        ):
            raise ValueError("applicationName must be a bounded non-empty string")
        return cls(
            physical_schema=identifier(binding.physical_namespace, "binding.physicalNamespace"),
            engine_profile=binding.engine_profile,
            scope_keys=scope_keys,
            resources=MappingProxyType(
                {
                    layout.ref.canonical: layout
                    for layout in sorted(layouts, key=lambda item: item.ref.canonical)
                }
            ),
            application_name=application_name,
            expected_standbys=expected,
            require_tls=binding.tls.mode != "disabled",
            read_compatibility=MappingProxyType(compatibility),
        )

    def layout(self, ref: ResourceRef | str) -> ResourceLayout:
        canonical = (
            ref.canonical if isinstance(ref, ResourceRef) else ResourceRef.parse(ref).canonical
        )
        try:
            return self.resources[canonical]
        except KeyError as exc:
            raise KeyError(f"no physical mapping for {canonical}") from exc
