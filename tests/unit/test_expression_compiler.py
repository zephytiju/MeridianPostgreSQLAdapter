# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections.abc import Mapping

import pytest
from psycopg import sql

from meridian_storage.adapters.postgresql.query import _ExpressionCompiler
from meridian_storage.adapters.postgresql.query._sql import (
    BoundStatement,
    conjunction,
    disjunction,
    ident,
)


def field(name: str, resource: str | None = None) -> dict[str, object]:
    result: dict[str, object] = {"kind": "field", "name": name}
    if resource is not None:
        result["resource"] = resource
    return result


def literal(value: object) -> dict[str, object]:
    return {"kind": "literal", "value": value}


def expression_compiler(settings: object) -> _ExpressionCompiler:
    people = settings.layout("structured:example.people")
    return _ExpressionCompiler(
        settings,
        {people.ref.canonical: "p"},
        {people.ref.canonical: people},
    )


def render(statement: BoundStatement) -> str:
    return statement.command.as_string(None)


@pytest.mark.parametrize(
    ("kind", "token"),
    [
        ("eq", " = "),
        ("ne", " <> "),
        ("lt", " < "),
        ("lte", " <= "),
        ("gt", " > "),
        ("gte", " >= "),
        ("add", " + "),
        ("subtract", " - "),
        ("multiply", " * "),
        ("divide", " / "),
        ("modulo", " % "),
    ],
)
def test_binary_expression_matrix(settings: object, kind: str, token: str) -> None:
    compiled = expression_compiler(settings).compile(
        {"kind": kind, "left": field("age"), "right": literal(2)}
    )
    assert token in render(compiled)
    assert compiled.parameters == (2,)


def test_expression_families_are_parameterized(settings: object) -> None:
    compiler = expression_compiler(settings)
    expressions: tuple[tuple[Mapping[str, object], str], ...] = (
        ({"kind": "not", "operand": field("age")}, "NOT"),
        ({"kind": "negate", "operand": field("age")}, "-"),
        (
            {
                "kind": "and",
                "operands": [
                    {"kind": "eq", "left": field("age"), "right": literal(1)},
                    {"kind": "isNull", "operand": field("name"), "expected": False},
                ],
            },
            "AND",
        ),
        (
            {
                "kind": "or",
                "operands": [
                    {"kind": "eq", "left": field("age"), "right": literal(1)},
                    {"kind": "eq", "left": field("age"), "right": literal(2)},
                ],
            },
            "OR",
        ),
        ({"kind": "prefix", "left": field("name"), "right": literal("A%_")}, "LIKE"),
        (
            {"kind": "contains", "left": field("name"), "right": literal("Ada")},
            "position",
        ),
        (
            {"kind": "contains", "left": field("document"), "right": literal({"a": 1})},
            "@>",
        ),
        ({"kind": "isNull", "operand": field("age"), "expected": True}, "IS NULL"),
        (
            {"kind": "in", "operand": field("age"), "values": [literal(1), literal(2)]},
            " IN ",
        ),
        (
            {"kind": "notIn", "operand": field("age"), "values": [literal(1)]},
            " NOT IN ",
        ),
        (
            {
                "kind": "timestampRange",
                "operand": field("name"),
                "start": literal("a"),
                "end": literal("z"),
                "includeStart": True,
                "includeEnd": False,
            },
            "AND",
        ),
        (
            {"kind": "documentPath", "document": field("document"), "pointer": "/a~1b/~0c"},
            "#>",
        ),
        (
            {"kind": "fullText", "fields": [field("name")], "query": "Ada"},
            "websearch_to_tsquery",
        ),
        (
            {
                "kind": "distance",
                "left": field("location"),
                "right": {"kind": "point", "coordinates": [-122.4, 37.7]},
            },
            "ST_Distance",
        ),
        (
            {
                "kind": "distanceWithin",
                "left": field("location"),
                "right": {"kind": "point", "coordinates": [-122.4, 37.7]},
                "distance": 100,
            },
            "ST_DWithin",
        ),
        ({"kind": "aggregate", "function": "count", "operand": None}, "count(*)"),
        (
            {
                "kind": "aggregate",
                "function": "sum",
                "operand": field("age"),
                "distinct": True,
            },
            "sum(DISTINCT",
        ),
    )
    for expression, expected in expressions:
        compiled = compiler.compile(expression)
        assert expected in render(compiled)
    assert compiler.compile(field("name", "example.people")).parameters == ()


@pytest.mark.parametrize(
    ("expression", "message"),
    [
        ([], "canonical mapping"),
        ({"kind": "parameter"}, "unbound"),
        ({"kind": "point", "coordinates": [1]}, "longitude"),
        ({"kind": "and", "operands": 1}, "array"),
        ({"kind": "in", "operand": field("age"), "values": []}, "candidates"),
        (
            {"kind": "documentPath", "document": field("document"), "pointer": "not-a-pointer"},
            "RFC 6901",
        ),
        ({"kind": "fullText", "fields": [], "query": "x"}, "fields"),
        ({"kind": "aggregate", "function": "median"}, "unsupported aggregate"),
        (
            {"kind": "aggregate", "function": "percentile", "operand": field("age")},
            "explicit percentile",
        ),
        ({"kind": "unknown"}, "unsupported query expression"),
        ({"kind": "field", "name": "missing"}, "absent"),
        ({"kind": "field", "name": "name", "resource": 1}, "must be a string"),
    ],
)
def test_expression_fail_closed(
    settings: object,
    expression: object,
    message: str,
) -> None:
    with pytest.raises((TypeError, ValueError), match=message):
        expression_compiler(settings).compile(expression)


def test_expression_resource_ambiguity_and_sql_helpers(settings: object) -> None:
    layout = settings.layout("structured:example.people")
    compiler = _ExpressionCompiler(
        settings,
        {
            "structured:example.people": "p",
            "structured:other.people": "q",
        },
        {
            "structured:example.people": layout,
            "structured:other.people": layout,
        },
    )
    with pytest.raises(ValueError, match="unambiguous"):
        compiler.compile(field("name", "people"))

    assert render(conjunction(())) == "TRUE"
    assert render(disjunction(())) == "FALSE"
    joined = conjunction((BoundStatement(sql.SQL("a"), (1,)), BoundStatement(sql.SQL("b"), (2,))))
    assert render(joined) == "(a) AND (b)"
    assert joined.parameters == (1, 2)
    assert ident("schema", "table").as_string(None) == '"schema"."table"'
