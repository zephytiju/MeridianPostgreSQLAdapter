# SPDX-License-Identifier: Apache-2.0
"""Synchronous libpq wait with one inherited budget; no detached work."""

from __future__ import annotations

import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager, suppress
from contextvars import ContextVar
from selectors import BaseSelector, DefaultSelector
from typing import Any, TypeVar, cast

from meridian_storage.context import OperationContext, current_context
from meridian_storage.errors import ErrorCode, MeridianTimeoutError
from psycopg import Connection
from psycopg.abc import PQGen

T = TypeVar("T")


class OperationBudget:
    def __init__(self, timeout: float, context: OperationContext | None = None) -> None:
        self.expires = time.monotonic() + timeout
        self.contexts: list[OperationContext] = []
        self.include(context or current_context(required=False))

    def include(self, context: OperationContext | None) -> None:
        if context is not None and all(context is not c for c in self.contexts):
            self.contexts.append(context)
            remaining = context.remaining_seconds()
            if remaining is not None:
                self.expires = min(self.expires, time.monotonic() + remaining)

    def remaining(self) -> float:
        remaining = self.expires - time.monotonic()
        for context in self.contexts:
            value = context.remaining_seconds()
            if value is not None:
                remaining = min(remaining, value)
        if remaining <= 0:
            raise MeridianTimeoutError(
                ErrorCode.DEADLINE_EXCEEDED, "PostgreSQL operation expired or cancelled"
            )
        return remaining


_io_budget: ContextVar[OperationBudget | None] = ContextVar(
    "meridian_postgresql_io_budget", default=None
)


@contextmanager
def bounded_io(budget: OperationBudget) -> Iterator[None]:
    token = _io_budget.set(budget)
    try:
        yield
    finally:
        _io_budget.reset(token)


def consume[T](
    gen: PQGen[T],
    fileno: int,
    budget: OperationBudget,
    selector_factory: Callable[[], BaseSelector] = DefaultSelector,
) -> T:
    """Drive nonblocking libpq generators, bounding every readiness wait.

    libpq flush/consume calls are nonblocking; the selector is the I/O wait.
    A timeout never invokes psycopg's additional cancel-and-drain wait.
    """
    budget.remaining()
    try:
        state = next(gen)
        with selector_factory() as selector:
            selector.register(fileno, state)
            while True:
                ready = selector.select(min(budget.remaining(), 0.05))
                budget.remaining()
                next_state = gen.send(ready[0][1] if ready else 0)
                if next_state != state:
                    selector.modify(fileno, next_state)
                    state = next_state
    except StopIteration as result:
        return cast(T, result.value)


class DeadlineConnection(Connection[Any]):
    """Connection used by the owner pool, including cursor/savepoint I/O."""

    def wait(self, gen: PQGen[T], interval: float = 0.1, timeout: float | None = None) -> T:
        budget = _io_budget.get()
        if budget is None and timeout is None:
            return super().wait(gen, interval=interval)
        if timeout is not None:
            limited = OperationBudget(timeout)
            if budget is not None:
                limited.expires = min(limited.expires, budget.expires)
                for context in budget.contexts:
                    limited.include(context)
            budget = limited
        assert budget is not None
        try:
            return consume(gen, self.pgconn.socket, budget)
        except BaseException:
            # Discard the transport without a blocking cancel or rollback.
            # A COMMIT already sent is ambiguous, never proof of rollback.
            with suppress(BaseException):
                self.pgconn.finish()
            with suppress(BaseException):
                gen.close()
            raise
