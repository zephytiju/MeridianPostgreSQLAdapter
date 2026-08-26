# SPDX-License-Identifier: Apache-2.0
"""Credential-free PostgreSQL error mapping."""

from __future__ import annotations

from meridian_storage.errors import (
    AuthenticationError,
    AuthorizationError,
    ConflictError,
    ConstraintError,
    ErrorCode,
    MeridianError,
    MeridianTimeoutError,
    SafeCause,
    TransactionError,
    TransientError,
    UnavailableError,
    ValidationError,
)


def map_postgresql_error(
    exc: BaseException,
    *,
    operation_contract: str | None = None,
    request_id: str | None = None,
    execution_id: str | None = None,
) -> MeridianError:
    """Map SQLSTATE class without exposing SQL, values, hosts, or credentials."""

    state = getattr(exc, "sqlstate", None)
    details = {
        "operation_contract": operation_contract,
        "request_id": request_id,
        "execution_id": execution_id,
        "adapter_provenance": {
            "adapter": "postgresql",
            **({"sqlstate": state} if isinstance(state, str) else {}),
        },
        "cause": SafeCause(type=type(exc).__name__, code=state),
    }
    if state in {"28P01", "28000"}:
        return AuthenticationError(
            ErrorCode.ADAPTER_FAILURE, "PostgreSQL authentication failed", **details
        )
    if state == "42501":
        return AuthorizationError(
            ErrorCode.ADAPTER_FAILURE, "PostgreSQL authorization failed", **details
        )
    if state == "23505":
        return ConflictError(
            ErrorCode.IDEMPOTENCY_CONFLICT, "a unique value already exists", **details
        )
    if isinstance(state, str) and state.startswith("23"):
        return ConstraintError(
            ErrorCode.OPERATION_INVALID, "a PostgreSQL constraint rejected the operation", **details
        )
    if state in {"40001", "40P01", "55P03"}:
        return TransientError(
            ErrorCode.ADAPTER_FAILURE, "the PostgreSQL transaction must be retried", **details
        )
    if state == "57014":
        return MeridianTimeoutError(
            ErrorCode.DEADLINE_EXCEEDED, "the PostgreSQL statement deadline expired", **details
        )
    if isinstance(state, str) and state.startswith("08"):
        return UnavailableError(
            ErrorCode.ADAPTER_FAILURE,
            "the PostgreSQL service is unavailable",
            retryable=True,
            **details,
        )
    if isinstance(state, str) and state.startswith("25"):
        return TransactionError(
            ErrorCode.TRANSACTION_STATE, "PostgreSQL transaction state is invalid", **details
        )
    if state in {"42P01", "42703", "42883"}:
        return ValidationError(
            ErrorCode.PHYSICAL_FINGERPRINT,
            "the pinned PostgreSQL physical schema is absent or incompatible",
            **details,
        )
    return UnavailableError(
        ErrorCode.ADAPTER_FAILURE, "PostgreSQL rejected the adapter operation", **details
    )


__all__ = ["map_postgresql_error"]
