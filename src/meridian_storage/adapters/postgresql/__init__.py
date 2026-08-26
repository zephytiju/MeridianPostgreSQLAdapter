# SPDX-License-Identifier: Apache-2.0
"""Meridian V1 PostgreSQL/PostGIS Adapter."""

from ._runtime import PostgreSQLAdapterFactory, PostgreSQLAdapterRuntime
from ._version import __version__
from .descriptor import DESCRIPTOR, QUERY_CAPABILITIES
from .migration import LogicalTransfer, MigrationExecutor, RecoveryHook
from .query import PostgreSQLQueryTranslator
from .query.dml import DMLCompiler
from .schema import MigrationPlan, SchemaCompiler
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
    "PostgreSQLQueryTranslator",
    "PostgreSQLSemanticsAdapter",
    "RecoveryHook",
    "SchemaCompiler",
    "__version__",
]
