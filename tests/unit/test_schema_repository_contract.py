# SPDX-License-Identifier: Apache-2.0
"""Fail-closed repository construction does not acquire a database connection."""

from dataclasses import replace

import pytest
from meridian_storage.errors import MeridianTimeoutError
from meridian_storage.semantics import SemanticsSchemaProvider

from meridian_storage import OperationContext
from meridian_storage.adapters.postgresql import PostgreSQLSchemaRepository
from meridian_storage.adapters.postgresql._settings import ResourceLayout


def forbidden_connection():
    pytest.fail("invalid repository configuration must not acquire a connection")


@pytest.mark.parametrize(
    "catalogs", [(), ("cache",), ("query",), ("structured", "structured"), "structured"]
)
def test_invalid_catalog_selection_does_not_connect(catalogs):
    with pytest.raises(ValueError):
        PostgreSQLSchemaRepository(
            connection_factory=forbidden_connection,
            physical_namespace="metadata",
            context=OperationContext(tenant="tenant", principal_ref="test"),
            catalogs=catalogs,
        )


@pytest.mark.parametrize(
    "changes",
    [
        {"physical_namespace": "metadata;drop"},
        {"operation_timeout_ms": 0},
        {"operation_timeout_ms": True},
        {"context": OperationContext(principal_ref="test")},
    ],
)
def test_invalid_deployment_context_does_not_connect(changes):
    arguments = dict(
        connection_factory=forbidden_connection,
        physical_namespace="metadata",
        context=OperationContext(tenant="tenant", principal_ref="test"),
    )
    with pytest.raises(ValueError):
        PostgreSQLSchemaRepository(**(arguments | changes))


def test_expired_deadline_does_not_connect():
    from datetime import UTC, datetime, timedelta

    repository = PostgreSQLSchemaRepository(
        connection_factory=forbidden_connection,
        physical_namespace="metadata",
        context=OperationContext(
            tenant="tenant", principal_ref="test", deadline=datetime.now(UTC) - timedelta(seconds=1)
        ),
    )
    with pytest.raises(MeridianTimeoutError):
        repository.snapshot()


def test_metadata_resource_layout_is_closed():
    bundle = SemanticsSchemaProvider().load()
    resource = next(item for item in bundle.resources if item.ref.catalog == "structured")
    layout = ResourceLayout(
        ref=resource.ref,
        table="__meridian_schema_registry",
        profile="metadata-registry",
        schema_fingerprint=bundle.schemas[0].fingerprint,
        resource_fingerprint=resource.fingerprint,
        fields=(),
        identity=(),
    )
    for changes in (
        {"table": "customer"},
        {"identity": ("id",)},
        {"ref": replace(resource.ref, name="other")},
    ):
        with pytest.raises(ValueError):
            replace(layout, **changes)
