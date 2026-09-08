# SPDX-License-Identifier: Apache-2.0
"""Meridian V1 PostgreSQL/PostGIS Adapter."""

from ._runtime import PostgreSQLAdapterFactory, PostgreSQLAdapterRuntime
from ._settings import PostgreSQLSettings
from ._version import __version__
from .descriptor import DESCRIPTOR, QUERY_CAPABILITIES
from .migration import LogicalTransfer, MigrationExecutor, RecoveryHook
from .projection import PostgreSQLOutbox
from .query import PostgreSQLQueryTranslator
from .query.dml import DMLCompiler
from .schema import MigrationPlan, SchemaCompiler
from .schema_registry import (
    PostgreSQLSchemaRepository,
    migrate_schema_repository,
    verify_schema_repository,
)
from .semantics import PostgreSQLSemanticsAdapter

__all__ = [
    "DESCRIPTOR",
    "QUERY_CAPABILITIES",
    "DMLCompiler",
    "LogicalTransfer",
    "MigrationExecutor",
    "MigrationPlan",
    "PostgreSQLAdapterFactory",
    "PostgreSQLAdapterRuntime",
    "PostgreSQLOutbox",
    "PostgreSQLQueryTranslator",
    "PostgreSQLSchemaRepository",
    "PostgreSQLSemanticsAdapter",
    "PostgreSQLSettings",
    "RecoveryHook",
    "SchemaCompiler",
    "__version__",
    "migrate_schema_repository",
    "verify_schema_repository",
]
