# SPDX-License-Identifier: Apache-2.0
"""Authenticated release provenance and retained feature/security/drift gates."""

from dataclasses import replace

import pytest
from psycopg import connect, errors, sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg.rows import dict_row

from meridian_storage.adapters.postgresql import PostgreSQLAdapterFactory
from meridian_storage.adapters.postgresql.descriptor import manifest
from meridian_storage.adapters.postgresql.probe import ProbeService

pytestmark = pytest.mark.integration


def test_observed_provenance_is_independent_of_unlisted_selection(
    integration_context, postgresql_dsn
):
    context, settings, _ = integration_context
    selected = "deployment-owned-postgis-image"
    binding = replace(
        context.binding,
        engine_version=selected,
        required_capability_fingerprint=manifest(settings.engine_profile, selected).fingerprint,
    )
    runtime = PostgreSQLAdapterFactory().create(replace(context, binding=binding))
    runtime.open()
    try:
        probe = runtime.probe()
        with connect(postgresql_dsn) as conn:
            server, postgis = conn.execute(
                "SELECT current_setting('server_version'), PostGIS_Lib_Version()"
            ).fetchone()
        observed = f"{server.split(' ', 1)[0]}-postgis-{postgis}"
        assert probe.observed_engine_version == observed
        assert probe.evidence["selectedEngineVersion"] == selected
        assert probe.evidence["observedEngineVersion"] == observed != selected
        assert probe.evidence["requiredFeatures"] == "passed"
    finally:
        runtime.close()


def test_missing_extension_and_function_fail_even_for_unlisted_metadata(
    integration_context, postgresql_dsn
):
    _, settings, _ = integration_context
    service = ProbeService(settings, engine_version="deployment-owned-postgis-image")
    with (
        connect(postgresql_dsn, row_factory=dict_row) as conn,
        conn.transaction(force_rollback=True),
    ):
        conn.execute("DROP EXTENSION postgis CASCADE")
        with pytest.raises(
            RuntimeError, match="required PostgreSQL extension unavailable: postgis"
        ):
            service.probe(conn)
    with (
        connect(postgresql_dsn, row_factory=dict_row) as conn,
        conn.transaction(force_rollback=True),
    ):
        conn.execute(
            "ALTER FUNCTION public.st_dwithin(geography, geography, double precision, boolean) "
            "RENAME TO unavailable_dwithin"
        )
        with pytest.raises(RuntimeError, match="feature unavailable: PostGIS geography distance"):
            service.probe(conn)


def test_tls_readonly_and_declared_release_drift_remain_failures(
    integration_context, postgresql_dsn
):
    _, settings, _ = integration_context
    with (
        connect(postgresql_dsn, row_factory=dict_row) as conn,
        pytest.raises(RuntimeError, match="requires TLS"),
    ):
        ProbeService(replace(settings, require_tls=True), engine_version="custom-image").probe(conn)
    with connect(postgresql_dsn, row_factory=dict_row) as conn:
        conn.execute("SET TRANSACTION READ ONLY")
        with pytest.raises(RuntimeError, match="read-write transactions"):
            ProbeService(settings, engine_version="custom-image").probe(conn)
    with (
        connect(postgresql_dsn, row_factory=dict_row) as conn,
        pytest.raises(RuntimeError, match="does not match the Binding pin"),
    ):
        ProbeService(settings, engine_version="99-postgis-99.99").probe(conn)


def test_authentication_and_schema_authorization_remain_failures(
    integration_context, postgresql_dsn
):
    _, settings, _ = integration_context
    parsed = conninfo_to_dict(postgresql_dsn)
    parsed.update(password="incorrect-disposable-test-password", connect_timeout="2")
    with pytest.raises(errors.OperationalError), connect(make_conninfo("", **parsed)):
        pytest.fail("incorrect credentials authenticated")
    with (
        connect(postgresql_dsn, row_factory=dict_row) as conn,
        conn.transaction(force_rollback=True),
    ):
        conn.execute("CREATE ROLE meridian_t100624_restricted")
        conn.execute(
            sql.SQL("REVOKE ALL ON SCHEMA {} FROM PUBLIC").format(
                sql.Identifier(settings.physical_schema)
            )
        )
        conn.execute("SET ROLE meridian_t100624_restricted")
        with pytest.raises(RuntimeError, match="physical-schema USAGE"):
            ProbeService(settings, engine_version="custom-image").probe(conn)
