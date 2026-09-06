# SPDX-License-Identifier: Apache-2.0
"""Deployment composition for the released owner-based OutboxPort."""

from .outbox import PostgreSQLOutbox

__all__ = ["PostgreSQLOutbox"]
