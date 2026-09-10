# SPDX-License-Identifier: Apache-2.0
"""Authenticated readiness, topology, and read-only physical verification."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from meridian_storage.spi.adapters import (
    AdapterProbe,
    PhysicalResource,
    PhysicalVerification,
)
from psycopg import Connection, sql
from psycopg.rows import dict_row

from .._settings import PostgreSQLSettings, ResourceLayout
from ..descriptor import manifest
from ..projection._storage import is_outbox, verify_storage
from ..query._sql import ident
from ..schema import _sql_type
from ..schema_registry import MIGRATION_TABLE, verify_schema_repository


@dataclass(frozen=True, slots=True)
class HealthStatus:
    ready: bool
    role: str
    server_major: str
    postgis_version: str
    tls: bool
    streaming_standbys: int


class ProbeService:
    def __init__(
        self,
        settings: PostgreSQLSettings,
        *,
        engine_version: str,
    ) -> None:
        self.settings = settings
        self.engine_version = engine_version

    def probe(self, connection: Connection[Any]) -> tuple[AdapterProbe, HealthStatus]:
        if connection.pgconn.protocol_version != 3:
            raise RuntimeError("PostgreSQL wire protocol 3 is required")
        with connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                "SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'postgis') AS present"
            )
            extension = cursor.fetchone()
            if not extension or not extension["present"]:
                raise RuntimeError("required PostgreSQL extension unavailable: postgis")
            cursor.execute(
                "SELECT current_setting('server_version_num') AS server_version_num, "
                "PostGIS_Lib_Version() AS postgis_version, current_database() AS database_name, "
                "session_user AS session_user, pg_is_in_recovery() AS recovery, "
                "current_setting('transaction_isolation') AS isolation, "
                "current_setting('server_version') AS server_version, "
                "current_setting('transaction_read_only') AS read_only"
            )
            row = cursor.fetchone()
            if row is None:
                raise RuntimeError("PostgreSQL probe returned no row")
            cursor.execute(
                "SELECT COALESCE(bool_or(ssl), false) AS tls "
                "FROM pg_stat_ssl WHERE pid = pg_backend_pid()"
            )
            tls_row = cursor.fetchone()
            tls = bool(tls_row and tls_row["tls"])
            cursor.execute(
                "SELECT count(*)::integer AS count FROM pg_stat_replication "
                "WHERE state = 'streaming'"
            )
            replication = cursor.fetchone()
            standbys = int(replication["count"] if replication else 0)
            cursor.execute(
                "SELECT has_database_privilege(current_database(), 'CONNECT') AS database_connect, "
                "has_schema_privilege(%s, 'USAGE') AS schema_usage",
                (self.settings.physical_schema,),
            )
            privileges = cursor.fetchone()
        server_major = str(int(row["server_version_num"]) // 10000)
        server_version = str(row["server_version"]).split(" ", 1)[0]
        postgis_version = str(row["postgis_version"])
        # Preserve numeric legacy deployment expectations without turning image
        # labels/digests or our historical tested list into release gates.
        expected = re.fullmatch(r"(\d+(?:\.\d+)*)-postgis-(\d+(?:\.\d+)*)", self.engine_version)
        if expected is not None:
            for selected, observed in zip(
                expected.groups(), (server_version, postgis_version), strict=True
            ):
                if observed.split(".")[: len(selected.split("."))] != selected.split("."):
                    raise RuntimeError(
                        "authenticated Engine version does not match the Binding pin"
                    )
        if str(row["isolation"]) != "read committed":
            raise RuntimeError("PostgreSQL default transaction isolation must be READ COMMITTED")
        if self.settings.require_tls and not tls:
            raise RuntimeError("Binding requires TLS but the authenticated session is plaintext")
        if not privileges or not privileges["database_connect"] or not privileges["schema_usage"]:
            raise RuntimeError("runtime identity lacks database CONNECT or physical-schema USAGE")
        recovery = bool(row["recovery"])
        role = "standby" if recovery else "primary"
        if recovery:
            raise RuntimeError("the Adapter write endpoint must resolve to a primary")
        if self.settings.read_only and row["read_only"] != "on":
            raise RuntimeError("read compatibility requires read-only transactions")
        if not self.settings.read_only and row["read_only"] != "off":
            raise RuntimeError("the Adapter write endpoint must permit read-write transactions")
        if (
            self.settings.engine_profile.endswith("cluster")
            and standbys < self.settings.expected_standbys
        ):
            raise RuntimeError("cluster has fewer streaming standbys than the pinned profile")
        if self.settings.engine_profile.endswith("local-single-primary") and standbys:
            raise RuntimeError("local single-primary profile unexpectedly exposes standbys")
        self._verify_required_features(connection)
        # Exercise begin/rollback without DDL or writes. The transaction id remains unassigned.
        with connection.transaction(force_rollback=True):
            rolled_back_probe = connection.execute(
                "SELECT txid_current_if_assigned() IS NULL AS passed"
            ).fetchone()
            if not rolled_back_probe or not rolled_back_probe["passed"]:
                raise RuntimeError(
                    "read-only rollback probe unexpectedly assigned a transaction id"
                )
        health = HealthStatus(True, role, server_major, postgis_version, tls, standbys)
        evidence = {
            "adapter": "postgresql",
            "database": str(row["database_name"]),
            "engineProfile": self.settings.engine_profile,
            "engineVersion": self.engine_version,  # Legacy selection field, never observation.
            "selectedEngineVersion": self.engine_version,
            "observedEngineVersion": f"{server_version}-postgis-{postgis_version}",
            "postgresqlVersion": server_version,
            "requiredFeatures": "passed",
            "protocolVersion": str(connection.pgconn.protocol_version),
            "postgisVersion": postgis_version,
            "role": role,
            "rollbackProbe": "passed",
            "serverMajor": server_major,
            "streamingStandbys": str(standbys),
            "tls": "enabled" if tls else "disabled",
            "accessMode": "read-only" if self.settings.read_only else "read-write",
        }
        return AdapterProbe(
            manifest(self.settings.engine_profile, self.engine_version),
            evidence,
            observed_engine_version=f"{server_version}-postgis-{postgis_version}",
        ), health

    def verify_physical(
        self,
        connection: Connection[Any],
        resources: tuple[PhysicalResource, ...],
    ) -> PhysicalVerification:
        requested = {resource.resource_ref.canonical: resource for resource in resources}
        if len(requested) != len(resources):
            raise ValueError("physical verification resources must be unique")
        configured = set(self.settings.resources)
        if set(requested) - configured:
            raise ValueError("physical verification requested an unpinned Resource")
        table = ident(self.settings.physical_schema, "__meridian_resources")
        rows: list[dict[str, object]] = []
        physical_shapes: list[dict[str, object]] = []
        mappings: dict[str, str] = {}
        for canonical, expected in sorted(requested.items()):
            with connection.cursor(row_factory=dict_row) as cursor:
                cursor.execute(
                    sql.SQL(
                        "SELECT resource_ref, resource_fingerprint, schema_fingerprint, profile, "
                        "physical_fingerprint FROM {} WHERE resource_ref = %s"
                    ).format(table),
                    (canonical,),
                )
                row = cursor.fetchone()
                if row is None:
                    raise RuntimeError(f"physical metadata is absent for {canonical}")
                layout = self.settings.resources[canonical]
                cursor.execute(
                    "SELECT to_regclass(%s) IS NOT NULL AS present",
                    (f"{self.settings.physical_schema}.{layout.table}",),
                )
                exists = cursor.fetchone()
            if not exists or not exists["present"]:
                raise RuntimeError(f"physical table is absent for {canonical}")
            physical_shapes.append(self._verify_table_shape(connection, layout))
            proof = self.settings.read_compatibility.get(canonical)
            stored_fingerprint = (
                layout.resource_fingerprint if proof is None else proof.stored_resource.fingerprint
            )
            checks = {
                "resource fingerprint": str(row["resource_fingerprint"]) == stored_fingerprint
                and expected.resource_fingerprint == layout.resource_fingerprint,
                "Schema fingerprint": str(row["schema_fingerprint"])
                == expected.schema_fingerprint
                == layout.schema_fingerprint,
                "profile": str(row["profile"]) == expected.profile == layout.profile,
            }
            failed = [name for name, valid in checks.items() if not valid]
            if failed:
                raise RuntimeError(
                    f"physical metadata mismatch for {canonical}: {', '.join(failed)}"
                )
            rows.append(dict(row))
            mappings[canonical] = f"{self.settings.physical_schema}.{layout.table}"
        encoded = json.dumps(
            {"metadata": rows, "physicalShapes": physical_shapes},
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
        state_fingerprint = "sha256:" + hashlib.sha256(encoded.encode("utf-8")).hexdigest()
        physical_plans = {str(row["physical_fingerprint"]) for row in rows}
        fingerprint = next(iter(physical_plans)) if len(physical_plans) == 1 else state_fingerprint
        return PhysicalVerification(
            fingerprint=fingerprint,
            mappings=mappings,
            evidence={
                "adapter": "postgresql",
                "physicalPlan": next(iter(physical_plans), state_fingerprint)
                if len(physical_plans) <= 1
                else "divergent",
                "resourceCount": str(len(rows)),
                "stateFingerprint": state_fingerprint,
                "verification": "read-only",
                "readCompatibility": "structured.put.v1-v2" if self.settings.read_only else "none",
            },
        )

    def _verify_table_shape(
        self,
        connection: Connection[Any],
        layout: ResourceLayout,
    ) -> dict[str, object]:
        if layout.profile == "metadata-registry":
            fingerprint = verify_schema_repository(
                connection,
                physical_namespace=self.settings.physical_schema,
            )
            return {"resourceRef": layout.ref.canonical, "migrationFingerprint": fingerprint}
        with connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                "SELECT a.attname AS name, format_type(a.atttypid, a.atttypmod) AS type, "
                "a.attnotnull AS not_null, a.attgenerated AS generated "
                "FROM pg_attribute AS a "
                "JOIN pg_class AS c ON c.oid = a.attrelid "
                "JOIN pg_namespace AS n ON n.oid = c.relnamespace "
                "WHERE n.nspname = %s AND c.relname = %s "
                "AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum",
                (self.settings.physical_schema, layout.table),
            )
            columns = [dict(row) for row in cursor.fetchall()]
            cursor.execute(
                "SELECT indexname FROM pg_indexes "
                "WHERE schemaname = %s AND tablename = %s ORDER BY indexname",
                (self.settings.physical_schema, layout.table),
            )
            indexes = tuple(str(row["indexname"]) for row in cursor.fetchall())

        expected_columns: list[tuple[str, str, bool, str]] = [
            ("__tenant", "text", True, ""),
            *((f"__scope_{key}", "text", True, "") for key in self.settings.scope_keys),
            *(
                (
                    field.column,
                    _sql_type(field).as_string(None).replace(", ", ","),
                    not field.nullable,
                    "",
                )
                for field in layout.fields
            ),
            ("__record_version", "bigint", True, ""),
            ("__created_at", "timestamp with time zone", True, ""),
            ("__updated_at", "timestamp with time zone", True, ""),
        ]
        if layout.relation is not None:
            expected_columns.extend(
                (
                    ("__source_collection", "text", False, "s"),
                    ("__source_record_id", "text", False, "s"),
                    ("__target_collection", "text", False, "s"),
                    ("__target_record_id", "text", False, "s"),
                )
            )
        observed_columns = sorted(
            [
                (
                    str(row["name"]),
                    str(row["type"]),
                    bool(row["not_null"]),
                    str(row["generated"]),
                )
                for row in columns
            ]
        )
        expected_columns.sort()
        if observed_columns != expected_columns:
            raise RuntimeError(f"physical column drift detected for {layout.ref.canonical}")

        expected_indexes = {f"{layout.table}_pkey"}
        expected_indexes.update(
            f"{layout.table}_{item.name}"
            for item in layout.indexes
            if item.kind != "relation-endpoint"
        )
        if layout.relation is not None:
            expected_indexes.update(
                {
                    f"{layout.table}__source_endpoint",
                    f"{layout.table}__target_endpoint",
                }
            )
        if set(indexes) != expected_indexes:
            raise RuntimeError(f"physical index drift detected for {layout.ref.canonical}")
        return {
            "resourceRef": layout.ref.canonical,
            "columns": [list(item) for item in observed_columns],
            "indexes": list(indexes),
        }

    def validate_table_privileges(
        self,
        connection: Connection[Any],
        *,
        privileges: Iterable[str] = ("SELECT", "INSERT", "UPDATE", "DELETE"),
    ) -> None:
        if any(layout.ref.catalog == "evidence" for layout in self.settings.resources.values()):
            self._verify_evidence_replay(connection)
        if any(is_outbox(layout) for layout in self.settings.resources.values()):
            verify_storage(connection, self.settings.physical_schema)
        for layout in self.settings.resources.values():
            selected_privileges = ("SELECT",) if self.settings.read_only else privileges
            if layout.profile == "metadata-registry":
                verify_schema_repository(
                    connection, physical_namespace=self.settings.physical_schema
                )
                selected_privileges = ("SELECT", "INSERT", "UPDATE")
                marker = f"{self.settings.physical_schema}.{MIGRATION_TABLE}"
                row = connection.execute(
                    "SELECT has_table_privilege(%s, 'SELECT') AS allowed", (marker,)
                ).fetchone()
                if not row or not row["allowed"]:
                    raise RuntimeError("runtime identity lacks SELECT on registry migration marker")
            qualified = f"{self.settings.physical_schema}.{layout.table}"
            for privilege in selected_privileges:
                row = connection.execute(
                    "SELECT has_table_privilege(%s, %s) AS allowed",
                    (qualified, privilege),
                ).fetchone()
                if not row or not row["allowed"]:
                    raise RuntimeError(
                        f"runtime identity lacks {privilege} on pinned Resource {layout.ref}"
                    )

    def _verify_evidence_replay(self, connection: Connection[Any]) -> None:
        qualified = f"{self.settings.physical_schema}.__meridian_evidence_replay"
        with connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                "SELECT a.attname AS name, format_type(a.atttypid, a.atttypmod) AS type, "
                "a.attnotnull AS not_null FROM pg_attribute a "
                "WHERE a.attrelid = to_regclass(%s) AND a.attnum > 0 AND NOT a.attisdropped "
                "ORDER BY a.attnum",
                (qualified,),
            )
            columns = [(r["name"], r["type"], r["not_null"]) for r in cursor.fetchall()]
            if columns != [
                ("replay_key", "text", True),
                ("request_fingerprint", "text", True),
                ("result", "jsonb", False),
            ]:
                raise RuntimeError("Evidence replay storage requires an explicit migration")
            cursor.execute(
                "SELECT pg_get_constraintdef(oid) AS definition FROM pg_constraint "
                "WHERE conrelid = to_regclass(%s) AND contype = 'p'",
                (qualified,),
            )
            primary = cursor.fetchone()
            if not primary or primary["definition"] != "PRIMARY KEY (replay_key)":
                raise RuntimeError("Evidence replay storage requires its unique replay key")
            for privilege in ("SELECT", "INSERT", "UPDATE"):
                cursor.execute(
                    "SELECT has_table_privilege(%s, %s) AS allowed", (qualified, privilege)
                )
                row = cursor.fetchone()
                if not row or not row["allowed"]:
                    raise RuntimeError("runtime identity lacks Evidence replay storage privileges")

    @staticmethod
    def _verify_required_features(connection: Connection[Any]) -> None:
        # Read-only semantic smoke probes, independent of release membership.
        # Missing extension/type/function/privilege fails before readiness.
        statements = {
            "PostGIS geography distance": (
                "SELECT ST_DWithin(ST_SetSRID(ST_MakePoint(0, 0), 4326)::geography, "
                "ST_SetSRID(ST_MakePoint(0, 0), 4326)::geography, 1) AS passed"
            ),
            "JSONB mutation": (
                "SELECT jsonb_set('{\"a\": 1}'::jsonb, '{a}', '2'::jsonb) "
                "= '{\"a\": 2}'::jsonb AS passed"
            ),
            "recursive traversal": (
                "WITH RECURSIVE walk(n) AS (VALUES (1) UNION ALL "
                "SELECT n + 1 FROM walk WHERE n < 2) SELECT max(n) = 2 AS passed FROM walk"
            ),
            "migration lock hashing": (
                "SELECT hashtextextended('meridian-feature-probe', 0) IS NOT NULL AS passed"
            ),
        }
        for feature, statement in statements.items():
            try:
                row = connection.execute(statement).fetchone()
            except Exception as exc:
                raise RuntimeError(
                    f"required PostgreSQL/PostGIS feature unavailable: {feature}"
                ) from exc
            if not row or not row["passed"]:
                raise RuntimeError(f"required PostgreSQL/PostGIS feature failed: {feature}")


__all__ = ["HealthStatus", "ProbeService"]
