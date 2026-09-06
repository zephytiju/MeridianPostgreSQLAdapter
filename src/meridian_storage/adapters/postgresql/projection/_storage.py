# SPDX-License-Identifier: Apache-2.0
"""Deployment-owned outbox metadata and immutable intent layout contract."""

from __future__ import annotations

from typing import Any

from psycopg import Connection, sql
from psycopg.rows import dict_row

from .._settings import ResourceLayout
from ..query._sql import BoundStatement, ident

# JSON preserves scalar source identity/version types, including null.
INTENT_FIELDS = {
    "formatVersion": ("string", False),
    "eventId": ("string", False),
    "sourceCatalog": ("string", False),
    "sourceResource": ("string", False),
    "sourceSchema": ("string", False),
    "sourceIdentity": ("json", True),
    "sourceVersion": ("json", True),
    "mutationKind": ("string", False),
    "payload": ("json", True),
    "immutableReference": ("json", True),
    "targetLabels": ("json", False),
    "occurredAt": ("utcTimestamp", False),
    "operationContext": ("json", False),
    "digest": ("string", False),
}
STATE_TABLE = "__meridian_outbox_state"
CHECKPOINT_TABLE = "__meridian_outbox_checkpoint"
_BASE = [("scope", "jsonb", True), ("resource_ref", "text", True), ("projection", "text", True)]
SHAPES = {
    STATE_TABLE: [
        *_BASE,
        ("event_id", "text", True),
        ("state", "text", True),
        ("attempt", "bigint", True),
        ("owner", "text", False),
        ("acquired_at", "timestamp with time zone", False),
        ("expires_at", "timestamp with time zone", False),
        ("failure", "jsonb", False),
        ("target_fingerprint", "text", False),
        ("completed_at", "timestamp with time zone", False),
    ],
    CHECKPOINT_TABLE: [
        *_BASE,
        ("partition_key", "text", True),
        ("revision", "bigint", True),
        ("event_id", "text", True),
        ("source_version", "jsonb", False),
        ("target_fingerprint", "text", True),
        ("advanced_at", "timestamp with time zone", True),
    ],
}


def is_outbox(layout: ResourceLayout) -> bool:
    return layout.ref.catalog == "structured" and set(INTENT_FIELDS) <= set(layout.field_map)


def validate_layout(layout: ResourceLayout) -> None:
    if not is_outbox(layout) or layout.identity != ("eventId",):
        raise ValueError(
            "outbox Resource requires immutable OutboxDataV1 fields and eventId identity"
        )
    for name, (kind, nullable) in INTENT_FIELDS.items():
        field = layout.field_map[name]
        if (field.logical_type, field.nullable, field.mutable, field.cardinality) != (
            kind,
            nullable,
            False,
            "one",
        ):
            raise ValueError(f"outbox field {name} has an incompatible storage layout")


def migration_statements(namespace: str) -> tuple[BoundStatement, ...]:
    statements = []
    for table, shape in SHAPES.items():
        key = "event_id" if table == STATE_TABLE else "partition_key"
        columns: list[sql.Composable] = [
            sql.Identifier(name) + sql.SQL(" " + kind + (" NOT NULL" if required else ""))
            for name, kind, required in shape
        ]
        columns.append(sql.SQL(f"PRIMARY KEY (scope, resource_ref, projection, {key})"))
        if table == STATE_TABLE:
            columns.extend(
                [
                    sql.SQL("CHECK (state IN ('LEASED','COMPLETED','RETRYABLE','QUARANTINED'))"),
                    sql.SQL("CHECK (attempt > 0)"),
                    sql.SQL(
                        "CHECK ((state = 'LEASED' AND owner IS NOT NULL AND acquired_at "
                        "IS NOT NULL "
                        "AND expires_at IS NOT NULL AND expires_at > acquired_at) OR "
                        "(state <> 'LEASED' AND owner IS NULL "
                        "AND acquired_at IS NULL AND expires_at IS NULL))"
                    ),
                ]
            )
        else:
            columns.append(sql.SQL("CHECK (revision > 0)"))
        statements.append(
            BoundStatement(
                sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(
                    ident(namespace, table), sql.SQL(", ").join(columns)
                )
            )
        )
    return tuple(statements)


def verify_storage(connection: Connection[Any], namespace: str) -> None:
    """Read-only validation. Never bootstrap missing durable state at startup."""
    with connection.cursor(row_factory=dict_row) as cursor:
        for table, shape in SHAPES.items():
            qualified = f"{namespace}.{table}"
            cursor.execute(
                "SELECT attname AS name, format_type(atttypid, atttypmod) AS type, "
                "attnotnull AS required FROM pg_attribute WHERE attrelid = to_regclass(%s) "
                "AND attnum > 0 AND NOT attisdropped ORDER BY attnum",
                (qualified,),
            )
            if [(r["name"], r["type"], r["required"]) for r in cursor.fetchall()] != shape:
                raise RuntimeError("outbox storage requires an explicit deployment migration")
            cursor.execute(
                "SELECT pg_get_constraintdef(oid) AS definition FROM pg_constraint "
                "WHERE conrelid = to_regclass(%s) AND contype = 'p'",
                (qualified,),
            )
            primary = cursor.fetchone()
            key = "event_id" if table == STATE_TABLE else "partition_key"
            if not primary or primary["definition"] != (
                f"PRIMARY KEY (scope, resource_ref, projection, {key})"
            ):
                raise RuntimeError("outbox storage requires its scoped unique key")
            for privilege in ("SELECT", "INSERT", "UPDATE"):
                cursor.execute(
                    "SELECT has_table_privilege(%s, %s) AS allowed", (qualified, privilege)
                )
                row = cursor.fetchone()
                if not row or not row["allowed"]:
                    raise RuntimeError("runtime identity lacks outbox storage privileges")
