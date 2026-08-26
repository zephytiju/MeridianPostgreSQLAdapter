# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import pytest
from meridian_storage.errors import (
    AuthenticationError,
    AuthorizationError,
    ConflictError,
    ConstraintError,
    MeridianTimeoutError,
    TransactionError,
    TransientError,
    UnavailableError,
    ValidationError,
)

from meridian_storage.adapters.postgresql._errors import map_postgresql_error


class DriverFailure(Exception):
    def __init__(self, sqlstate: str) -> None:
        self.sqlstate = sqlstate


def test_sqlstate_mapping_is_stable_and_redacted() -> None:
    conflict = map_postgresql_error(DriverFailure("23505"))
    retry = map_postgresql_error(DriverFailure("40001"))
    timeout = map_postgresql_error(DriverFailure("57014"))
    assert isinstance(conflict, ConflictError)
    assert isinstance(retry, TransientError) and retry.retryable
    assert isinstance(timeout, MeridianTimeoutError)
    assert "SQL" not in conflict.message
    assert conflict.cause is not None and conflict.cause.code == "23505"


@pytest.mark.parametrize(
    ("sqlstate", "expected"),
    [
        ("28P01", AuthenticationError),
        ("42501", AuthorizationError),
        ("23503", ConstraintError),
        ("40P01", TransientError),
        ("08006", UnavailableError),
        ("25001", TransactionError),
        ("42P01", ValidationError),
        ("XX000", UnavailableError),
    ],
)
def test_every_sqlstate_class_is_safely_mapped(
    sqlstate: str,
    expected: type[Exception],
) -> None:
    mapped = map_postgresql_error(
        DriverFailure(sqlstate),
        operation_contract="meridian.structured.query",
        request_id="request",
        execution_id="execution",
    )
    assert isinstance(mapped, expected)
    assert mapped.cause is not None and mapped.cause.code == sqlstate


def test_non_driver_failure_has_no_fabricated_sqlstate() -> None:
    mapped = map_postgresql_error(RuntimeError("contains secret material"))
    assert isinstance(mapped, UnavailableError)
    assert "secret material" not in mapped.message
    assert mapped.adapter_provenance == {"adapter": "postgresql"}
