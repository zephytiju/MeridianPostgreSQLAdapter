# SPDX-License-Identifier: Apache-2.0
"""Small typed SQL AST; user data can only enter through bound parameters."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass

from psycopg import sql

type SQLValue = object


@dataclass(frozen=True, slots=True)
class BoundStatement:
    command: sql.Composable
    parameters: tuple[SQLValue, ...] = ()

    def with_suffix(self, suffix: sql.Composable, *values: SQLValue) -> BoundStatement:
        return BoundStatement(self.command + suffix, (*self.parameters, *values))


def ident(*parts: str) -> sql.Composed:
    return sql.SQL(".").join(sql.Identifier(part) for part in parts)


def placeholders(count: int) -> sql.Composed:
    if count < 1:
        raise ValueError("at least one placeholder is required")
    return sql.SQL(", ").join(sql.Placeholder() for _ in range(count))


def conjunction(parts: Iterable[BoundStatement]) -> BoundStatement:
    selected = tuple(parts)
    if not selected:
        return BoundStatement(sql.SQL("TRUE"))
    return BoundStatement(
        sql.SQL("(") + sql.SQL(") AND (").join(item.command for item in selected) + sql.SQL(")"),
        tuple(value for item in selected for value in item.parameters),
    )


def disjunction(parts: Iterable[BoundStatement]) -> BoundStatement:
    selected = tuple(parts)
    if not selected:
        return BoundStatement(sql.SQL("FALSE"))
    return BoundStatement(
        sql.SQL("(") + sql.SQL(") OR (").join(item.command for item in selected) + sql.SQL(")"),
        tuple(value for item in selected for value in item.parameters),
    )


def render(statement: BoundStatement) -> tuple[str, tuple[SQLValue, ...]]:
    return statement.command.as_string(None), statement.parameters


def json_size(value: object) -> int:
    import json

    return len(
        json.dumps(value, separators=(",", ":"), sort_keys=True, default=str).encode("utf-8")
    )


def rows_to_records(
    rows: Sequence[Mapping[str, object]],
    *,
    internal_columns: frozenset[str],
) -> list[dict[str, object]]:
    return [
        {key: value for key, value in row.items() if key not in internal_columns} for row in rows
    ]


__all__ = [
    "BoundStatement",
    "SQLValue",
    "conjunction",
    "disjunction",
    "ident",
    "json_size",
    "placeholders",
    "render",
    "rows_to_records",
]
