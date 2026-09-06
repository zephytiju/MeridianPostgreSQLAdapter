# SPDX-License-Identifier: Apache-2.0
"""Durable scoped OutboxPort. SQL and Engine connections stay in the adapter."""

from __future__ import annotations

import math
import re
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

from meridian_storage.errors import ErrorCode, ValidationError
from meridian_storage.projection import ProjectionSpec
from meridian_storage.projection.errors import CheckpointConflict, LeaseLostError
from meridian_storage.projection.outbox import (
    Checkpoint,
    OutboxDataV1,
    OutboxFailure,
    OutboxLease,
    OutboxRecord,
    OutboxState,
    ProjectionLag,
)
from psycopg import Connection, Error, sql
from psycopg.types.json import Jsonb
from psycopg_pool import PoolTimeout

from meridian_storage import MeridianError, NotFoundError, OperationContext, ResourceRef

from .._errors import map_postgresql_error
from ..query._sql import BoundStatement, conjunction, ident, json_size
from ._storage import CHECKPOINT_TABLE, INTENT_FIELDS, STATE_TABLE, validate_layout, verify_storage

if TYPE_CHECKING:
    from .._runtime import PostgreSQLAdapterRuntime


class PostgreSQLOutbox:
    """Inject into ProjectionRunner from deployment composition, using an opened runtime.

    Runtime owns the pool; this object neither creates DDL nor starts a worker.
    Owner strings must not overlap across live host attempts: this released port
    does not carry a generation token. Inspectors are deployment diagnostics.
    """

    def __init__(
        self,
        runtime: PostgreSQLAdapterRuntime,
        *,
        resource: str,
        spec: ProjectionSpec,
        context: OperationContext,
        poison_threshold: int = 5,
        max_batch_size: int = 1000,
    ) -> None:
        for name, value in (
            ("poison_threshold", poison_threshold),
            ("max_batch_size", max_batch_size),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"{name} must be a positive integer")
        if not context.tenant or set(context.scope) != set(runtime.settings.scope_keys):
            raise ValueError("outbox requires the configured tenant and exact scope keys")
        if any(not isinstance(v, str) or not v for v in context.scope.values()):
            raise ValueError("outbox scope values must be non-empty strings")
        self._runtime = runtime
        self.spec = spec
        self._layout = runtime.settings.layout(ResourceRef.parse(resource, catalog="structured"))
        validate_layout(self._layout)
        runtime.settings.layout(ResourceRef.parse(spec.source, catalog=spec.source_catalog))
        self._threshold, self._max_batch = poison_threshold, max_batch_size
        self._scope = {"tenant": context.tenant, "scope": dict(context.scope)}
        self._scope_values = (
            context.tenant,
            *(context.scope[k] for k in runtime.settings.scope_keys),
        )
        self._table = ident(runtime.settings.physical_schema, self._layout.table)
        self._state = ident(runtime.settings.physical_schema, STATE_TABLE)
        self._checkpoint = ident(runtime.settings.physical_schema, CHECKPOINT_TABLE)
        with self._connection() as connection:
            verify_storage(connection, runtime.settings.physical_schema)

    @contextmanager
    def _connection(self) -> Iterator[Connection[Any]]:
        try:
            with self._runtime._semantics_connection() as connection, connection.transaction():
                connection.execute(
                    "SELECT set_config('statement_timeout', %s, true)",
                    (str(self._runtime._context.binding.client.operation_timeout_ms),),
                )
                yield connection
        except (Error, PoolTimeout) as error:
            raise map_postgresql_error(error) from None

    def _column(self, alias: str, name: str) -> sql.Composed:
        return ident(alias, self._layout.field_map[name].column)

    def _selection(self, alias: str) -> BoundStatement:
        conditions = [
            BoundStatement(sql.SQL("{} = %s").format(ident(alias, column)), (value,))
            for column, value in zip(
                ("__tenant", *(f"__scope_{k}" for k in self._runtime.settings.scope_keys)),
                self._scope_values,
                strict=True,
            )
        ]
        for name, value in (
            ("sourceCatalog", self.spec.source_catalog),
            ("sourceResource", self.spec.source),
            ("sourceSchema", self.spec.source_schema),
        ):
            conditions.append(
                BoundStatement(sql.SQL("{} = %s").format(self._column(alias, name)), (value,))
            )
        conditions.append(
            BoundStatement(
                sql.SQL("{} @> %s").format(self._column(alias, "targetLabels")),
                (Jsonb(list(self.spec.target_labels)),),
            )
        )
        return conjunction(conditions)

    def _key(self, alias: str) -> BoundStatement:
        return BoundStatement(
            sql.SQL("{}.scope = %s AND {}.resource_ref = %s AND {}.projection = %s").format(
                *(sql.Identifier(alias) for _ in range(3))
            ),
            self._key_values,
        )

    @property
    def _key_values(self) -> tuple[object, ...]:
        return Jsonb(self._scope), self._layout.ref.canonical, self.spec.name

    def _join(self, row: str, state: str) -> BoundStatement:
        key = self._key(state)
        return BoundStatement(
            sql.SQL("LEFT JOIN {} {} ON {} AND {}.event_id = {}").format(
                self._state,
                sql.Identifier(state),
                key.command,
                sql.Identifier(state),
                self._column(row, "eventId"),
            ),
            key.parameters,
        )

    def _fields(self, alias: str) -> sql.Composed:
        return sql.SQL(", ").join(
            sql.SQL("{} AS {}").format(self._column(alias, name), sql.Identifier(name))
            for name in INTENT_FIELDS
        )

    @staticmethod
    def _now(connection: Connection[Any], now: datetime | None) -> datetime:
        if now is not None:
            if now.tzinfo is None or now.utcoffset() != timedelta(0):
                raise ValueError("now must be UTC")
            return now
        row = connection.execute("SELECT clock_timestamp() AS now").fetchone()
        assert row is not None
        return row["now"]  # type: ignore[no-any-return]

    @staticmethod
    def _data(row: Mapping[str, Any]) -> OutboxDataV1:
        return OutboxDataV1(
            source_catalog=row["sourceCatalog"],
            source_resource=row["sourceResource"],
            source_schema=row["sourceSchema"],
            source_identity=row["sourceIdentity"],
            source_version=row["sourceVersion"],
            mutation_kind=row["mutationKind"],
            payload=row["payload"],
            immutable_reference=row["immutableReference"],
            target_labels=tuple(row["targetLabels"]),
            occurred_at=row["occurredAt"],
            operation_context=row["operationContext"],
            event_id=row["eventId"],
            digest=row["digest"],
            format_version=row["formatVersion"],
        )

    @classmethod
    def _record(cls, row: Mapping[str, Any]) -> OutboxRecord:
        state = OutboxState(row.get("state") or "PENDING")
        failure = row.get("failure")
        return OutboxRecord(
            data=cls._data(row),
            state=state,
            attempt_count=row.get("attempt") or 0,
            lease=OutboxLease(row["owner"], row["acquired_at"], row["expires_at"], row["attempt"])
            if state is OutboxState.LEASED
            else None,
            failure=OutboxFailure(
                failure["code"], failure["cause"], datetime.fromisoformat(failure["at"])
            )
            if failure
            else None,
            target_fingerprint=row.get("target_fingerprint"),
            completed_at=row.get("completed_at"),
        )

    def atomic_claim(
        self, *, owner: str, limit: int, lease_duration: timedelta, now: datetime | None = None
    ) -> tuple[OutboxRecord, ...]:
        if not isinstance(owner, str) or not owner:
            raise ValueError("owner must be non-empty")
        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or not 1 <= limit <= self._max_batch
        ):
            raise ValueError("limit must be positive and within max_batch_size")
        if lease_duration <= timedelta(0) or not math.isfinite(lease_duration.total_seconds()):
            raise ValueError("lease_duration must be positive")
        selected, previous = self._selection("i"), self._selection("p")
        joined, prior_joined = self._join("i", "s"), self._join("p", "ps")

        # Numeric source versions take precedence within one identity. Opaque
        # versions use occurrence/event order; independent identities remain concurrent.
        def order(alias: str) -> sql.Composable:
            version = self._column(alias, "sourceVersion")
            return sql.SQL(
                "(CASE WHEN jsonb_typeof({}) = 'number' THEN 0 ELSE 1 END, "
                "CASE WHEN jsonb_typeof({}) = 'number' THEN ({} #>> '{{}}')::numeric "
                "ELSE 0 END, {}, {})"
            ).format(
                version,
                version,
                version,
                self._column(alias, "occurredAt"),
                self._column(alias, "eventId"),
            )

        statement = sql.SQL(
            "SELECT {}, s.* FROM {} i {} WHERE {} AND "
            "(s.state IS NULL OR s.state = 'RETRYABLE' OR (s.state = 'LEASED' AND "
            "s.expires_at <= %s)) "
            "AND NOT EXISTS (SELECT 1 FROM {} p {} WHERE {} AND "
            "{} IS NOT DISTINCT FROM {} AND {} < {} AND "
            "COALESCE(ps.state, 'PENDING') <> 'COMPLETED') "
            "ORDER BY {}, {} LIMIT %s FOR UPDATE OF i SKIP LOCKED"
        ).format(
            self._fields("i"),
            self._table,
            joined.command,
            selected.command,
            self._table,
            prior_joined.command,
            previous.command,
            self._column("p", "sourceIdentity"),
            self._column("i", "sourceIdentity"),
            order("p"),
            order("i"),
            self._column("i", "occurredAt"),
            self._column("i", "eventId"),
        )
        result = []
        with self._connection() as connection:
            current = self._now(connection, now)
            rows = connection.execute(
                statement,
                (
                    *joined.parameters,
                    *selected.parameters,
                    current,
                    *prior_joined.parameters,
                    *previous.parameters,
                    limit,
                ),
            ).fetchall()
            result_bytes = 0
            for row in rows:
                data = self._data(row)
                result_bytes += json_size(data.to_mapping())
                if result_bytes > self._runtime._context.binding.client.max_result_bytes:
                    raise ValidationError(
                        ErrorCode.OPERATION_RESULT_LIMIT,
                        "outbox claim exceeds the configured result byte limit",
                    )
                current = self._now(connection, now)
                acquired = connection.execute(
                    sql.SQL(
                        "INSERT INTO {} AS s (scope, resource_ref, projection, event_id, "
                        "state, attempt, "
                        "owner, acquired_at, expires_at) VALUES (%s,%s,%s,%s,'LEASED',1,%s,%s,%s) "
                        "ON CONFLICT (scope, resource_ref, projection, event_id) DO UPDATE SET "
                        "state = 'LEASED', attempt = s.attempt + 1, owner = EXCLUDED.owner, "
                        "acquired_at = EXCLUDED.acquired_at, expires_at = EXCLUDED.expires_at, "
                        "failure = NULL WHERE s.state = 'RETRYABLE' OR "
                        "(s.state = 'LEASED' AND s.expires_at <= EXCLUDED.acquired_at) RETURNING *"
                    ).format(self._state),
                    (*self._key_values, data.event_id, owner, current, current + lease_duration),
                ).fetchone()
                # A fresh statement rechecks state after waiting on an intent row
                # modified by another completed claim under READ COMMITTED.
                if acquired is not None:
                    result.append(self._record({**row, **acquired}))
        return tuple(result)

    def _load(self, connection: Connection[Any], event_id: str, *, lock: bool) -> dict[str, Any]:
        selected, joined = self._selection("i"), self._join("i", "s")
        row = connection.execute(
            sql.SQL("SELECT {}, s.* FROM {} i {} WHERE {} AND {} = %s {}").format(
                self._fields("i"),
                self._table,
                joined.command,
                selected.command,
                self._column("i", "eventId"),
                sql.SQL("FOR UPDATE OF i" if lock else ""),
            ),
            (*joined.parameters, *selected.parameters, event_id),
        ).fetchone()
        if row is None:
            raise NotFoundError("MERIDIAN_OUTBOX_NOT_FOUND", "outbox event was not found in scope")
        # The join snapshot may precede an overlapping completion/reclaim. Read
        # mutable state again *after* holding the intent lock, in a new statement.
        if lock:
            key = self._key("s")
            state = connection.execute(
                sql.SQL("SELECT s.* FROM {} s WHERE {} AND event_id = %s").format(
                    self._state, key.command
                ),
                (*key.parameters, event_id),
            ).fetchone()
            if state is not None:
                row.update(state)
        return dict(row)

    def _leased(
        self, connection: Connection[Any], event_id: str, owner: str, now: datetime | None
    ) -> tuple[dict[str, Any], datetime]:
        row = self._load(connection, event_id, lock=True)
        current = self._now(connection, now)
        if row.get("state") != "LEASED" or row["owner"] != owner or row["expires_at"] <= current:
            raise LeaseLostError("outbox lease requires the current non-expired owner")
        return row, current

    @staticmethod
    def _progress(row: Mapping[str, Any], partition_key: str) -> Checkpoint:
        return Checkpoint(
            partition_key,
            row["revision"],
            row["event_id"],
            row["source_version"],
            row["target_fingerprint"],
            row["advanced_at"],
        )

    def complete(
        self,
        event_id: str,
        *,
        owner: str,
        acknowledged_source_version: str | int | None,
        target_fingerprint: str,
        now: datetime | None = None,
    ) -> Checkpoint:
        if re.fullmatch(r"sha256:[0-9a-f]{64}", target_fingerprint) is None:
            raise ValueError("target_fingerprint must be a SHA256 fingerprint")
        with self._connection() as connection:
            row, current = self._leased(connection, event_id, owner, now)
            data = self._data(row)
            if (
                type(acknowledged_source_version) is not type(data.source_version)
                or acknowledged_source_version != data.source_version
            ):
                raise CheckpointConflict(
                    "target acknowledgement does not match the exact source version"
                )
            previous = self._read_checkpoint(connection, data.partition_key)
            values = (
                *self._key_values,
                data.partition_key,
                previous.revision + 1,
                event_id,
                Jsonb(data.source_version),
                target_fingerprint,
                current,
            )
            saved = connection.execute(
                sql.SQL(
                    "INSERT INTO {} AS c (scope, resource_ref, projection, partition_key, "
                    "revision, "
                    "event_id, source_version, target_fingerprint, advanced_at) VALUES "
                    "(%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT "
                    "(scope, resource_ref, projection, partition_key) DO UPDATE SET "
                    "revision=EXCLUDED.revision, event_id=EXCLUDED.event_id, "
                    "source_version=EXCLUDED.source_version, "
                    "target_fingerprint=EXCLUDED.target_fingerprint, "
                    "advanced_at=EXCLUDED.advanced_at WHERE c.revision=%s RETURNING *"
                ).format(self._checkpoint),
                (*values, previous.revision),
            ).fetchone()
            if saved is None:
                raise CheckpointConflict("checkpoint revision changed concurrently")
            key = self._key("s")
            changed = connection.execute(
                sql.SQL(
                    "UPDATE {} s SET state='COMPLETED', owner=NULL, acquired_at=NULL, "
                    "expires_at=NULL, "
                    "failure=NULL, target_fingerprint=%s, completed_at=%s WHERE {} AND event_id=%s "
                    "AND state='LEASED' AND owner=%s "
                    "AND expires_at > COALESCE(%s::timestamptz, clock_timestamp()) "
                    "RETURNING event_id"
                ).format(self._state, key.command),
                (target_fingerprint, current, *key.parameters, event_id, owner, now),
            ).fetchone()
            if changed is None:
                raise LeaseLostError("lease expired before completion; checkpoint rolled back")
            return self._progress(saved, data.partition_key)

    def release(
        self,
        event_id: str,
        *,
        owner: str,
        error: BaseException,
        retryable: bool,
        now: datetime | None = None,
    ) -> OutboxRecord:
        with self._connection() as connection:
            row, current = self._leased(connection, event_id, owner, now)
            # Persist only classification, never exception text, URLs or payloads.
            code = str(error.code) if isinstance(error, MeridianError) else type(error).__name__
            failure = Jsonb(
                {
                    "code": code[:128],
                    "cause": "projection failed (details redacted)",
                    "at": current.isoformat(),
                }
            )
            state = "RETRYABLE" if retryable and row["attempt"] < self._threshold else "QUARANTINED"
            key = self._key("s")
            saved = connection.execute(
                sql.SQL(
                    "UPDATE {} s SET state=%s, owner=NULL, acquired_at=NULL, "
                    "expires_at=NULL, failure=%s "
                    "WHERE {} AND event_id=%s AND state='LEASED' AND owner=%s "
                    "AND expires_at > COALESCE(%s::timestamptz, clock_timestamp()) RETURNING *"
                ).format(self._state, key.command),
                (state, failure, *key.parameters, event_id, owner, now),
            ).fetchone()
            if saved is None:
                raise LeaseLostError("lease expired before release")
            return self._record({**row, **saved})

    def get(self, event_id: str) -> OutboxRecord:
        with self._connection() as connection:
            return self._record(self._load(connection, event_id, lock=False))

    def _read_checkpoint(self, connection: Connection[Any], partition_key: str) -> Checkpoint:
        key = self._key("c")
        row = connection.execute(
            sql.SQL("SELECT * FROM {} c WHERE {} AND partition_key=%s").format(
                self._checkpoint, key.command
            ),
            (*key.parameters, partition_key),
        ).fetchone()
        return self._progress(row, partition_key) if row else Checkpoint(partition_key, 0)

    def checkpoint(self, partition_key: str) -> Checkpoint:
        with self._connection() as connection:
            return self._read_checkpoint(connection, partition_key)

    def lag(self, *, now: datetime | None = None) -> ProjectionLag:
        selected, joined = self._selection("i"), self._join("i", "s")
        with self._connection() as connection:
            row = connection.execute(
                sql.SQL(
                    "SELECT count(*) AS count, min({}) AS oldest FROM {} i {} WHERE {} "
                    "AND COALESCE(s.state,'PENDING') <> 'COMPLETED'"
                ).format(
                    self._column("i", "occurredAt"), self._table, joined.command, selected.command
                ),
                (*joined.parameters, *selected.parameters),
            ).fetchone()
            assert row is not None
            current = self._now(connection, now)
            oldest = row["oldest"]
            return ProjectionLag(
                row["count"],
                oldest,
                max(0.0, (current - oldest).total_seconds()) if oldest else 0.0,
                self.spec.eventual_visibility_seconds,
            )
