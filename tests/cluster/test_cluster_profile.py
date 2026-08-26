# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import os
import time

import pytest
from conftest import make_binding, physical_resources
from meridian_storage.spi.adapters import AdapterCreateContext, SecretValue
from psycopg import connect

from meridian_storage.adapters.postgresql import PostgreSQLAdapterFactory
from meridian_storage.adapters.postgresql._settings import PostgreSQLSettings
from meridian_storage.adapters.postgresql.migration import MigrationExecutor
from meridian_storage.adapters.postgresql.schema import SchemaCompiler

pytestmark = pytest.mark.cluster


def cluster_endpoints() -> tuple[str, tuple[str, ...]]:
    endpoint = os.environ.get("MERIDIAN_POSTGRESQL_CLUSTER_DSN")
    standby_value = os.environ.get("MERIDIAN_POSTGRESQL_CLUSTER_STANDBY_DSNS")
    if not endpoint or not standby_value:
        pytest.skip("PostgreSQL cluster DSNs are not configured")
    standbys = tuple(item for item in standby_value.split(",") if item)
    if len(standbys) < 2:
        pytest.fail("cluster conformance requires at least two standby DSNs")
    return endpoint, standbys


def adapter_context(
    endpoint: str,
    *,
    expected_standbys: int = 2,
) -> tuple[AdapterCreateContext, PostgreSQLSettings]:
    binding, user, password = make_binding(
        endpoint,
        engine_profile="postgresql-postgis-cluster",
        expected_standbys=expected_standbys,
    )
    return (
        AdapterCreateContext(
            binding=binding,
            identity=SecretValue(user.encode()),
            credential=SecretValue(password.encode()),
        ),
        PostgreSQLSettings.from_binding(binding),
    )


def test_cluster_profile_replays_and_routes_only_to_primary() -> None:
    endpoint, standbys = cluster_endpoints()
    create_context, settings = adapter_context(endpoint)
    plan = SchemaCompiler(settings).compile()
    with connect(endpoint) as connection:
        assert MigrationExecutor(settings).apply(connection, plan).applied

    deadline = time.monotonic() + 30
    for standby in standbys:
        while True:
            try:
                with connect(standby) as connection:
                    row = connection.execute(
                        "SELECT pg_is_in_recovery(), "
                        "to_regclass('meridian_test.__meridian_migrations') IS NOT NULL, "
                        "pg_last_wal_replay_lsn() IS NOT NULL"
                    ).fetchone()
                if row == (True, True, True):
                    break
            except Exception:
                if time.monotonic() >= deadline:
                    raise
            if time.monotonic() >= deadline:
                pytest.fail("standby did not replay the Meridian migration before the deadline")
            time.sleep(0.1)

    runtime = PostgreSQLAdapterFactory().create(create_context)
    runtime.open()
    try:
        probe = runtime.probe()
        assert probe.evidence["role"] == "primary"
        assert int(probe.evidence["streamingStandbys"]) >= 2
        verification = runtime.verify_physical(physical_resources(settings))
        assert verification.fingerprint == plan.physical_fingerprint
    finally:
        runtime.close()

    standby_context, _ = adapter_context(standbys[0])
    standby_runtime = PostgreSQLAdapterFactory().create(standby_context)
    standby_runtime.open()
    try:
        with pytest.raises(RuntimeError, match="write endpoint must resolve to a primary"):
            standby_runtime.probe()
    finally:
        standby_runtime.close()

    oversized_context, _ = adapter_context(endpoint, expected_standbys=3)
    oversized_runtime = PostgreSQLAdapterFactory().create(oversized_context)
    oversized_runtime.open()
    try:
        with pytest.raises(RuntimeError, match="fewer streaming standbys"):
            oversized_runtime.probe()
    finally:
        oversized_runtime.close()
