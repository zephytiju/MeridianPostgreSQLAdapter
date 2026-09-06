# SPDX-License-Identifier: Apache-2.0
"""One Meridian transaction to one PostgreSQL connection and transaction."""

from __future__ import annotations

import json
from collections.abc import Mapping
from threading import RLock
from typing import Any, Protocol, cast

from meridian_storage.errors import (
    ConflictError,
    ErrorCode,
    MeridianError,
    MeridianTimeoutError,
    TransactionError,
    ValidationError,
)
from meridian_storage.semantics import sha256_fingerprint
from meridian_storage.spi.adapters import ExecutionRequest, ExecutionResult
from psycopg import Connection, sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

from .._errors import map_postgresql_error
from .._operation import OperationCompiler, QueryCommand
from ..query._values import decode_query_value
from ..query.dml import AppendBatchCommand, DMLCommand, jsonable


class _Pool(Protocol):
    def getconn(self, timeout: float | None = None) -> Connection[Any]: ...

    def putconn(self, conn: Connection[Any]) -> None: ...


class PostgreSQLAdapterSession:
    def __init__(
        self,
        pool: ConnectionPool[Connection[Any]] | _Pool,
        compiler: OperationCompiler,
        *,
        binding_id: str,
        transactional: bool,
        acquire_timeout_ms: int,
        operation_timeout_ms: int,
        max_result_bytes: int,
    ) -> None:
        self._pool = pool
        self._compiler = compiler
        self._binding_id = binding_id
        self._transactional = transactional
        self._acquire_timeout = acquire_timeout_ms / 1000
        self._operation_timeout_ms = operation_timeout_ms
        self._max_result_bytes = max_result_bytes
        self._connection: Connection[Any] | None = None
        self._begun = False
        self._closed = False
        self._lock = RLock()

    def begin(self) -> None:
        with self._lock:
            self._require_open()
            if not self._transactional or self._begun:
                raise TransactionError(
                    ErrorCode.TRANSACTION_STATE,
                    "begin requires a new transactional Adapter session",
                )
            connection = self._pool.getconn(timeout=self._acquire_timeout)
            try:
                connection.execute("BEGIN ISOLATION LEVEL READ COMMITTED")
            except Exception:
                self._pool.putconn(connection)
                raise
            self._connection = connection
            self._begun = True

    def execute(self, request: ExecutionRequest) -> ExecutionResult:
        with self._lock:
            self._require_open()
            if self._transactional and not self._begun:
                raise TransactionError(
                    ErrorCode.TRANSACTION_STATE,
                    "transactional Adapter session must begin before execute",
                )
            if self._transactional:
                assert self._connection is not None
                return self._execute(self._connection, request)
            connection = self._pool.getconn(timeout=self._acquire_timeout)
            try:
                with connection.transaction():
                    return self._execute(connection, request)
            finally:
                self._pool.putconn(connection)

    def commit(self) -> None:
        with self._lock:
            self._require_transaction()
            assert self._connection is not None
            connection = self._connection
            try:
                connection.commit()
            finally:
                self._connection = None
                self._begun = False
                self._pool.putconn(connection)

    def rollback(self) -> None:
        with self._lock:
            self._require_transaction()
            assert self._connection is not None
            connection = self._connection
            try:
                connection.rollback()
            finally:
                self._connection = None
                self._begun = False
                self._pool.putconn(connection)

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            if self._connection is not None:
                connection = self._connection
                try:
                    connection.rollback()
                finally:
                    self._pool.putconn(connection)
            self._connection = None
            self._begun = False
            self._closed = True

    def _execute(
        self,
        connection: Connection[Any],
        request: ExecutionRequest,
    ) -> ExecutionResult:
        remaining = request.context.remaining_seconds()
        if remaining is not None and remaining <= 0:
            raise MeridianTimeoutError(
                ErrorCode.DEADLINE_EXCEEDED,
                "operation deadline expired before PostgreSQL execution",
                request_id=request.request_id,
                execution_id=request.execution_id,
            )
        timeout_ms = self._operation_timeout_ms
        if remaining is not None:
            timeout_ms = max(1, min(timeout_ms, int(remaining * 1000)))
        try:
            if request.binding_id != self._binding_id:
                raise ValueError("execution request belongs to a different Binding")
            command = self._compiler.compile(request)
            query_limit: int | None = None
            if isinstance(command, QueryCommand):
                envelope = cast(Mapping[str, object], command.compiled.command)
                deadline = envelope.get("deadlineMs")
                result_limit = envelope.get("maxResultBytes")
                if isinstance(deadline, int) and not isinstance(deadline, bool):
                    timeout_ms = max(1, min(timeout_ms, deadline))
                if isinstance(result_limit, int) and not isinstance(result_limit, bool):
                    query_limit = result_limit
            connection.execute(
                "SELECT set_config('statement_timeout', %s, true)",
                (f"{timeout_ms}ms",),
            )
            if isinstance(command, QueryCommand):
                data, provenance = self._execute_query(connection, command, request)
            elif isinstance(command, AppendBatchCommand) or command.method == "append":
                data, provenance = self._execute_append(connection, command, request)
            else:
                data, provenance = self._execute_dml(connection, command, request)
            encoded = json.dumps(
                data,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
            ).encode("utf-8")
            maximum_result_bytes = self._max_result_bytes
            if query_limit is not None:
                maximum_result_bytes = min(maximum_result_bytes, query_limit)
            if len(encoded) > maximum_result_bytes:
                raise ValidationError(
                    ErrorCode.OPERATION_RESULT_LIMIT,
                    "PostgreSQL result exceeds the configured byte limit",
                    request_id=request.request_id,
                    execution_id=request.execution_id,
                )
            return ExecutionResult(
                data=data,
                result_bytes=len(encoded),
                provenance={
                    "adapter": "postgresql",
                    "consistency": "strong",
                    "transactionIsolation": "read-committed",
                    **provenance,
                },
            )
        except MeridianError:
            raise
        except (TypeError, ValueError, KeyError) as exc:
            raise ValidationError(
                ErrorCode.OPERATION_INVALID,
                str(exc),
                operation_contract=request.operation.operation_contract,
                request_id=request.request_id,
                execution_id=request.execution_id,
            ) from exc
        except Exception as exc:
            raise map_postgresql_error(
                exc,
                operation_contract=request.operation.operation_contract,
                request_id=request.request_id,
                execution_id=request.execution_id,
            ) from exc

    def _execute_append(
        self,
        connection: Connection[Any],
        command: DMLCommand | AppendBatchCommand,
        request: ExecutionRequest,
    ) -> tuple[Any, dict[str, str]]:
        if request.operation.input.get("requireAtomic") is True and not self._transactional:
            raise TransactionError(
                ErrorCode.TRANSACTION_STATE,
                "required Evidence append must join an explicit Binding transaction",
            )
        commands = command.commands if isinstance(command, AppendBatchCommand) else (command,)
        key = request.operation.input.get("idempotencyKey") or request.context.idempotency_key
        if key is None and request.operation.idempotent:
            raw = request.operation.input.get("data")
            records = (raw,) if isinstance(raw, Mapping) else raw
            if isinstance(records, (tuple, list)) and records:
                identities = [
                    {name: item[name] for name in ("evidenceId", "checkpointKey") if name in item}
                    for item in records
                    if isinstance(item, Mapping)
                ]
                if len(identities) == len(records) and all(identities):
                    key = {"recordIdentities": identities}
        replay_key = (
            None
            if key is None
            else sha256_fingerprint(
                {
                    "binding": request.binding_id,
                    "principal": request.context.principal_ref,
                    "tenant": request.context.tenant,
                    "scope": dict(request.context.scope),
                    "resource": commands[0].layout.ref.canonical,
                    "contract": request.operation.operation_contract,
                    "key": key,
                }
            )
        )
        table = sql.Identifier(
            self._compiler.settings.physical_schema, "__meridian_evidence_replay"
        )
        # A savepoint also makes a caught batch failure all-or-nothing inside
        # an explicit transaction. The caller still owns the outer rollback.
        with connection.transaction():
            if replay_key is not None:
                claimed = connection.execute(
                    sql.SQL(
                        "INSERT INTO {} (replay_key, request_fingerprint) VALUES (%s, %s) "
                        "ON CONFLICT DO NOTHING RETURNING replay_key"
                    ).format(table),
                    (replay_key, request.operation.request_fingerprint),
                ).fetchone()
                if claimed is None:
                    with connection.cursor(row_factory=dict_row) as cursor:
                        cursor.execute(
                            sql.SQL(
                                "SELECT request_fingerprint, result FROM {} WHERE replay_key = %s"
                            ).format(table),
                            (replay_key,),
                        )
                        previous = cursor.fetchone()
                    if (
                        previous is None
                        or previous["result"] is None
                        or previous["request_fingerprint"] != request.operation.request_fingerprint
                    ):
                        raise ConflictError(
                            ErrorCode.IDEMPOTENCY_CONFLICT,
                            "Evidence idempotency key has a different request fingerprint",
                        )
                    return previous["result"], {"mutation": "append", "replay": "true"}
            rows = [self._execute_dml(connection, item, request)[0] for item in commands]
            data = rows if isinstance(command, AppendBatchCommand) else rows[0]
            if (
                len(
                    json.dumps(
                        data, sort_keys=True, separators=(",", ":"), ensure_ascii=False
                    ).encode("utf-8")
                )
                > self._max_result_bytes
            ):
                raise ValidationError(
                    ErrorCode.OPERATION_RESULT_LIMIT,
                    "PostgreSQL result exceeds the configured byte limit",
                )
            if replay_key is not None:
                connection.execute(
                    sql.SQL("UPDATE {} SET result = %s WHERE replay_key = %s").format(table),
                    (Jsonb(data), replay_key),
                )
            return data, {"mutation": "append"}

    def _execute_query(
        self,
        connection: Connection[Any],
        command: QueryCommand,
        request: ExecutionRequest,
    ) -> tuple[Any, dict[str, str]]:
        raw_envelope = command.compiled.command
        if not isinstance(raw_envelope, dict) and not hasattr(raw_envelope, "get"):
            raise TypeError("compiled PostgreSQL command is invalid")
        envelope = cast(Mapping[str, Any], raw_envelope)
        order = envelope.get("parameterOrder", ())
        if not isinstance(order, (tuple, list)):
            raise TypeError("compiled PostgreSQL parameter order is invalid")
        parameters: list[object] = []
        for name in order:
            value = command.compiled.parameters[str(name)]
            marker = cast(Mapping[str, object], value) if isinstance(value, Mapping) else None
            if marker is not None and set(marker) == {"$scope"}:
                value = self._scope_parameter(str(marker["$scope"]), request)
            parameters.append(decode_query_value(value))
        with connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(str(envelope["sql"]), tuple(parameters))
            rows = [jsonable(row) for row in cursor.fetchall()]
        normalized = command.translator.normalize_result(command.compiled, rows)
        return normalized.operation_data(), dict(normalized.provenance)

    @staticmethod
    def _scope_parameter(column: str, request: ExecutionRequest) -> str:
        if column == "__tenant":
            if request.context.tenant is None:
                raise ValueError("operation tenant is required")
            return request.context.tenant
        prefix = "__scope_"
        if not column.startswith(prefix):
            raise ValueError("compiled scope marker is invalid")
        key = column.removeprefix(prefix)
        try:
            return request.context.scope[key]
        except KeyError as exc:
            raise ValueError(f"operation scope is missing {key!r}") from exc

    @staticmethod
    def _execute_dml(
        connection: Connection[Any],
        command: DMLCommand,
        request: ExecutionRequest,
    ) -> tuple[Any, dict[str, str]]:
        with connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                cast(sql.SQL | sql.Composed, command.statement.command),
                command.statement.parameters,
            )
            rows = [jsonable(row) for row in cursor.fetchall()]
        if command.conditional and not rows:
            raise ConflictError(
                ErrorCode.IDEMPOTENCY_CONFLICT,
                "conditional mutation did not match the expected record version",
                operation_contract=request.operation.operation_contract,
                request_id=request.request_id,
                execution_id=request.execution_id,
            )
        if command.method == "get" and len(rows) > 1:
            raise ConflictError(
                ErrorCode.OPERATION_INVALID,
                "structured get matched more than one record",
                operation_contract=request.operation.operation_contract,
                request_id=request.request_id,
                execution_id=request.execution_id,
            )
        data: Any = rows[0] if command.single and rows else None if command.single else rows
        return data, {"mutation": command.method}

    def _require_open(self) -> None:
        if self._closed:
            raise TransactionError(ErrorCode.RUNTIME_CLOSED, "Adapter session is closed")

    def _require_transaction(self) -> None:
        self._require_open()
        if not self._transactional or not self._begun or self._connection is None:
            raise TransactionError(
                ErrorCode.TRANSACTION_STATE,
                "commit or rollback requires an active PostgreSQL transaction",
            )


__all__ = ["PostgreSQLAdapterSession"]
