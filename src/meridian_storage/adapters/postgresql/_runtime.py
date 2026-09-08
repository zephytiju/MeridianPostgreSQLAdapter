# SPDX-License-Identifier: Apache-2.0
"""Psycopg runtime implementation for the released Meridian Adapter SPI."""

from __future__ import annotations

import hashlib
import hmac
import math
import os
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from threading import RLock
from typing import Any

from meridian_storage.errors import (
    CompatibilityError,
    ConfigurationError,
    ErrorCode,
    LifecycleError,
)
from meridian_storage.query.cursor import CursorSigner
from meridian_storage.spi.adapters import (
    AdapterCreateContext,
    AdapterProbe,
    PhysicalResource,
    PhysicalVerification,
)
from psycopg import Connection
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from ._operation import OperationCompiler
from ._settings import PostgreSQLSettings
from .descriptor import ADAPTER_CONTRACT_VERSION, ADAPTER_ID, ENGINE_VERSIONS, manifest
from .probe import ProbeService
from .query import PostgreSQLQueryTranslator
from .semantics import PostgreSQLSemanticsAdapter
from .transactions import PostgreSQLAdapterSession


def _configure_connection(connection: Connection[Any]) -> None:
    connection.execute("SET TIME ZONE 'UTC'")
    connection.execute("SET default_transaction_isolation TO 'read committed'")
    connection.commit()


class PostgreSQLAdapterRuntime:
    def __init__(self, context: AdapterCreateContext) -> None:
        self._context = context
        self.settings = PostgreSQLSettings.from_binding(context.binding)
        self._temporary_directory: tempfile.TemporaryDirectory[str] | None = None
        self._conninfo = self._build_conninfo(context)
        cursor_key = hmac.new(
            context.credential.reveal(),
            f"meridian-postgresql-cursor:{context.binding.id}".encode(),
            hashlib.sha256,
        ).digest()
        signer = CursorSigner({"binding-v1": cursor_key}, active_key_id="binding-v1")
        self.translator = PostgreSQLQueryTranslator(self.settings, cursor_signer=signer)
        self.compiler = OperationCompiler(self.settings, self.translator)
        self.probes = ProbeService(
            self.settings,
            engine_version=context.binding.engine_version,
        )
        self.semantics = PostgreSQLSemanticsAdapter(self.settings, self._semantics_connection)
        self._pool: ConnectionPool[Connection[Any]] | None = None
        self._opened = False
        self._closed = False
        self._lock = RLock()

    def open(self) -> None:
        with self._lock:
            if self._closed:
                raise LifecycleError(ErrorCode.RUNTIME_CLOSED, "PostgreSQL Adapter is closed")
            if self._opened:
                return
            client = self._context.binding.client
            pool: ConnectionPool[Connection[Any]] = ConnectionPool(
                conninfo=self._conninfo,
                min_size=client.min_size,
                max_size=client.max_size,
                timeout=client.acquire_timeout_ms / 1000,
                max_idle=client.idle_timeout_ms / 1000,
                kwargs={"autocommit": False, "row_factory": dict_row},
                configure=_configure_connection,
                check=ConnectionPool.check_connection,
                open=False,
                name=f"meridian-{self._context.binding.id}",
            )
            try:
                pool.open(wait=True, timeout=client.acquire_timeout_ms / 1000)
            except Exception:
                pool.close()
                raise
            self._pool = pool
            self._opened = True

    def probe(self) -> AdapterProbe:
        pool = self._require_pool()
        with pool.connection(
            timeout=self._context.binding.client.acquire_timeout_ms / 1000
        ) as connection:
            result, _ = self.probes.probe(connection)
            self.probes.validate_table_privileges(connection)
            return result

    def verify_physical(self, resources: tuple[PhysicalResource, ...]) -> PhysicalVerification:
        pool = self._require_pool()
        with pool.connection(
            timeout=self._context.binding.client.acquire_timeout_ms / 1000
        ) as connection:
            return self.probes.verify_physical(connection, resources)

    def open_session(self, *, transactional: bool) -> PostgreSQLAdapterSession:
        pool = self._require_pool()
        client = self._context.binding.client
        return PostgreSQLAdapterSession(
            pool,
            self.compiler,
            binding_id=self._context.binding.id,
            transactional=transactional,
            acquire_timeout_ms=client.acquire_timeout_ms,
            operation_timeout_ms=client.operation_timeout_ms,
            max_result_bytes=client.max_result_bytes,
        )

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            if self._pool is not None:
                self._pool.close()
            self._pool = None
            self._conninfo = ""
            if self._temporary_directory is not None:
                self._temporary_directory.cleanup()
                self._temporary_directory = None
            self._opened = False
            self._closed = True

    def _require_pool(self) -> ConnectionPool[Connection[Any]]:
        with self._lock:
            if self._closed:
                raise LifecycleError(ErrorCode.RUNTIME_CLOSED, "PostgreSQL Adapter is closed")
            if not self._opened or self._pool is None:
                raise LifecycleError(
                    ErrorCode.RUNTIME_STATE,
                    "PostgreSQL Adapter must be opened before use",
                )
            return self._pool

    @contextmanager
    def _semantics_connection(self) -> Iterator[Connection[Any]]:
        pool = self._require_pool()
        with pool.connection(
            timeout=self._context.binding.client.acquire_timeout_ms / 1000
        ) as connection:
            yield connection

    def _build_conninfo(self, context: AdapterCreateContext) -> str:
        binding = context.binding
        if binding.endpoint is None:
            raise ConfigurationError(
                ErrorCode.CONFIG_INVALID,
                "Platform must resolve serviceRef to an endpoint before Adapter creation",
            )
        parsed = conninfo_to_dict(binding.endpoint)
        if "password" in parsed or "user" in parsed:
            raise ConfigurationError(
                ErrorCode.CONFIG_INVALID,
                "Binding endpoint cannot embed identity or credentials",
            )
        try:
            identity = context.identity.reveal().decode("utf-8", errors="strict")
            credential = context.credential.reveal().decode("utf-8", errors="strict")
        except UnicodeDecodeError as exc:
            raise ConfigurationError(
                ErrorCode.CONFIG_SECRET_REFERENCE,
                "PostgreSQL identity and credential must be UTF-8",
            ) from exc
        if not identity or "\x00" in identity or "\x00" in credential:
            raise ConfigurationError(
                ErrorCode.CONFIG_SECRET_REFERENCE,
                "PostgreSQL identity or credential is invalid",
            )
        options: dict[str, str | int | None] = {
            "application_name": self.settings.application_name,
            "connect_timeout": max(
                1,
                math.ceil(binding.client.acquire_timeout_ms / 1000),
            ),
            "password": credential,
            "user": identity,
        }
        if binding.tls.mode == "disabled":
            options["sslmode"] = "disable"
        else:
            if context.tls_ca is None:
                raise ConfigurationError(
                    ErrorCode.CONFIG_SECRET_REFERENCE,
                    "authenticated TLS requires resolved CA material",
                )
            directory = tempfile.TemporaryDirectory(prefix="meridian-postgresql-tls-")
            self._temporary_directory = directory
            ca_path = Path(directory.name) / "ca.pem"
            ca_path.write_bytes(context.tls_ca.reveal())
            os.chmod(ca_path, 0o600)
            options.update({"sslmode": "verify-full", "sslrootcert": str(ca_path)})
            endpoint_host = parsed.get("host")
            if binding.tls.server_name is not None and endpoint_host != binding.tls.server_name:
                raise ConfigurationError(
                    ErrorCode.CONFIG_INVALID,
                    "TLS serverName must match the endpoint host",
                )
            if binding.tls.mode == "mutual":
                if context.tls_client_certificate is None:
                    raise ConfigurationError(
                        ErrorCode.CONFIG_SECRET_REFERENCE,
                        "mutual TLS requires resolved client certificate material",
                    )
                client_path = Path(directory.name) / "client.pem"
                client_path.write_bytes(context.tls_client_certificate.reveal())
                os.chmod(client_path, 0o600)
                options.update({"sslcert": str(client_path), "sslkey": str(client_path)})
        return make_conninfo(binding.endpoint, **options)


class PostgreSQLAdapterFactory:
    @property
    def adapter_id(self) -> str:
        return ADAPTER_ID

    def create(self, context: AdapterCreateContext) -> PostgreSQLAdapterRuntime:
        binding = context.binding
        if binding.adapter_id != ADAPTER_ID or binding.adapter_contract != ADAPTER_CONTRACT_VERSION:
            raise CompatibilityError(
                ErrorCode.ADAPTER_CONTRACT,
                "Binding does not select the PostgreSQL Adapter V1 contract",
            )
        if binding.engine_profile not in ENGINE_VERSIONS:
            raise CompatibilityError(
                ErrorCode.ADAPTER_CONTRACT,
                "Binding selects an unsupported PostgreSQL/PostGIS Engine profile",
            )
        selected_manifest = manifest(binding.engine_profile, binding.engine_version)
        if binding.required_capability_fingerprint != selected_manifest.fingerprint:
            raise CompatibilityError(
                ErrorCode.CAPABILITY_FINGERPRINT,
                "PostgreSQL capability fingerprint differs from the Binding pin",
            )
        # Compare the installed artifact to the deployment's selection, never our
        # historical build recipe. The installer owns full artifact lock validation.
        for package in (
            "meridian-storage-core",
            "meridian-storage-semantics",
            "meridian-storage-query",
            "meridian-storage-projection",
            "meridian-storage-postgresql",
        ):
            selected = binding.compatibility_pins.get(package)
            if selected is None:
                continue
            try:
                installed = version(package)
            except PackageNotFoundError as exc:
                raise CompatibilityError(
                    ErrorCode.ADAPTER_CONTRACT,
                    f"deployment-selected package {package} is not installed",
                ) from exc
            if installed != selected:
                raise CompatibilityError(
                    ErrorCode.ADAPTER_CONTRACT,
                    f"deployment lock drift for {package}: "
                    f"selected {selected}, installed {installed}",
                )
        return PostgreSQLAdapterRuntime(context)


__all__ = ["PostgreSQLAdapterFactory", "PostgreSQLAdapterRuntime"]
