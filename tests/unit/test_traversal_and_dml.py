# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace

import pytest
from conftest import fp, sample_settings_mapping
from meridian_storage.context import OperationContext
from meridian_storage.query.adapter import TranslationContext
from meridian_storage.query.ast import Field
from meridian_storage.query.wire import QueryOperation, QueryTarget, ResultSpec, TraversalSpec
from meridian_storage.registry.resources import ResourceRef
from meridian_storage.semantics import RecordReference, ResourceReference

from meridian_storage.adapters.postgresql._settings import PostgreSQLSettings
from meridian_storage.adapters.postgresql.query import PostgreSQLQueryTranslator
from meridian_storage.adapters.postgresql.query.dml import DMLCompiler


def test_traversal_uses_only_explicit_relation_collections(
    settings: PostgreSQLSettings,
) -> None:
    people = ResourceRef.parse("structured:example.people")
    friendships = ResourceRef.parse("structured:example.friendships")
    traversal = TraversalSpec(
        start=RecordReference(
            ResourceReference.parse("structured:example.people"),
            "00000000-0000-0000-0000-000000000001",
        ),
        relation_collections=(friendships,),
        relation_predicates={friendships.canonical: Field("label").eq("friend")},
        direction="outbound",
        max_depth=3,
        result_shape="paths",
    )
    operation = QueryOperation(
        catalog="structured",
        targets=(QueryTarget(people),),
        operation="traverse",
        result=ResultSpec("paths"),
        traversal=traversal,
    )
    translation_context = TranslationContext(
        binding_id="postgresql-test",
        plan_fingerprint=fp("traversal-plan"),
        registry_fingerprint=fp("registry"),
        schema_fingerprints={
            people.canonical: settings.layout(people).schema_fingerprint,
            friendships.canonical: settings.layout(friendships).schema_fingerprint,
        },
        scope_fingerprint=fp("scope"),
        deadline_ms=30_000,
    )
    compiled = PostgreSQLQueryTranslator(settings).compile(operation, translation_context)
    sql_text = compiled.command["sql"]
    assert "WITH RECURSIVE edges" in sql_text
    assert '"friendships"' in sql_text
    assert "pg_catalog" not in sql_text
    assert "information_schema" not in sql_text
    assert "ANY(w.path)" in sql_text
    assert '"r"."label"' in sql_text
    assert "friend" in compiled.parameters.values()

    stale = replace(
        operation,
        traversal=replace(
            traversal,
            all_neighbors=True,
            registry_fingerprint=fp("stale-registry"),
        ),
    )
    with pytest.raises(ValueError, match="closure is stale"):
        PostgreSQLQueryTranslator(settings).compile(stale, translation_context)
    filtered = replace(
        operation,
        traversal=replace(traversal, record_filter=Field("name").eq("Ada")),
    )
    with pytest.raises(ValueError, match="record filters"):
        PostgreSQLQueryTranslator(settings).compile(filtered, translation_context)
    too_deep = replace(operation, traversal=replace(traversal, max_depth=9))
    with pytest.raises(ValueError, match="depth bound"):
        PostgreSQLQueryTranslator(settings).compile(too_deep, translation_context)


def test_atomic_claim_uses_skip_locked_and_scope(settings: PostgreSQLSettings) -> None:
    context = OperationContext(
        principal_ref="test",
        tenant="tenant-a",
        scope={"workspace": "workspace-a"},
    )
    command = DMLCompiler(settings).atomic_claim(
        ResourceRef.parse("structured:example.work"),
        where={"state": "ready"},
        changes={"state": "claimed", "owner": "worker-a"},
        limit=10,
        context=context,
    )
    sql_text = command.statement.command.as_string(None)
    assert "FOR UPDATE SKIP LOCKED" in sql_text
    assert '"__tenant" = %s' in sql_text
    assert '"__scope_workspace" = %s' in sql_text
    assert "worker-a" not in sql_text


def test_relation_endpoints_are_registry_pinned(settings: PostgreSQLSettings) -> None:
    context = OperationContext(
        principal_ref="test",
        tenant="tenant-a",
        scope={"workspace": "workspace-a"},
    )
    compiler = DMLCompiler(settings)
    endpoint = {
        "collectionRef": {
            "catalog": "structured",
            "namespace": "example",
            "name": "people",
        },
        "recordId": "00000000-0000-0000-0000-000000000001",
    }
    compiler.compile(
        "put",
        ResourceRef.parse("structured:example.friendships"),
        {
            "data": {
                "id": "00000000-0000-0000-0000-000000000010",
                "source": endpoint,
                "target": endpoint,
                "label": "friend",
            }
        },
        context,
    )
    invalid = {
        **endpoint,
        "collectionRef": {**endpoint["collectionRef"], "name": "work"},
    }
    with pytest.raises(ValueError, match="outside its pin"):
        compiler.compile(
            "put",
            ResourceRef.parse("structured:example.friendships"),
            {
                "data": {
                    "id": "00000000-0000-0000-0000-000000000011",
                    "source": invalid,
                    "target": endpoint,
                    "label": "friend",
                }
            },
            context,
        )

    raw = deepcopy(sample_settings_mapping())
    relation = next(item for item in raw["resources"] if item["profile"] == "relation")
    source = next(item for item in relation["fields"] if item["name"] == "source")
    source["mutable"] = True
    mutable_settings = PostgreSQLSettings.from_binding(
        SimpleNamespace(
            engine_profile="postgresql-postgis-local-single-primary",
            settings=raw,
            physical_namespace="meridian_test",
            tls=SimpleNamespace(mode="disabled"),
        )
    )
    with pytest.raises(ValueError, match="outside its pin"):
        DMLCompiler(mutable_settings).compile(
            "patch",
            ResourceRef.parse("structured:example.friendships"),
            {
                "where": {"id": "00000000-0000-0000-0000-000000000010"},
                "changes": {"source": invalid},
            },
            context,
        )
