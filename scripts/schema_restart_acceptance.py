# SPDX-License-Identifier: Apache-2.0
"""Two-process downloaded-package check, with a real database restart between phases."""

from __future__ import annotations

import json
import os
import sys
from contextlib import contextmanager
from pathlib import Path

from meridian_storage.semantics import SchemaAPI
from psycopg import connect

from meridian_storage import OperationContext
from meridian_storage.adapters.postgresql import (
    PostgreSQLSchemaRepository,
    migrate_schema_repository,
)


@contextmanager
def connections():
    with connect(os.environ["MERIDIAN_POSTGRESQL_TEST_DSN"]) as connection:
        yield connection


def main():
    phase, receipt_path = sys.argv[1:]
    namespace = "meridian_schema_restart_acceptance"
    repository = PostgreSQLSchemaRepository(
        connection_factory=connections,
        physical_namespace=namespace,
        context=OperationContext(
            tenant="restart-tenant", principal_ref="acceptance", scope={"workspace": "restart"}
        ),
    )
    api = SchemaAPI(repository)
    if phase == "write":
        with connections() as connection:
            migrate_schema_repository(connection, physical_namespace=namespace)
        api.publish(
            namespace="acceptance",
            name="restart",
            version="1.0.0",
            definition={
                "semanticKind": "relational",
                "fields": [
                    {"name": "id", "logicalType": "string", "nullable": False},
                ],
                "identity": ["id"],
                "extensions": {
                    "org.prism/json-schema-2020-12.v1": {
                        "$schema": "https://json-schema.org/draft/2020-12/schema",
                        "type": "object",
                        "properties": {
                            "title": {"type": "string", "description": "Unicode 中文 🚀"}
                        },
                    }
                },
            },
        )
        receipt = repository.snapshot().to_dict()
        Path(receipt_path).write_text(json.dumps(receipt, ensure_ascii=False, indent=2) + "\n")
    elif phase == "read":
        expected = json.loads(Path(receipt_path).read_text())
        assert repository.snapshot().to_dict() == expected
        first = expected["schemas"][0]
        read = api.read(
            namespace="acceptance",
            name="restart",
            version="1.0.0",
            expected_fingerprint=first["schema"]["fingerprint"],
        )
        assert read.to_dict() == first
        replay = api.publish(
            namespace="acceptance", name="restart", version="1.0.0", definition=first["schema"]
        )
        assert replay.idempotent and replay.publication.to_dict() == first
        assert repository.snapshot().to_dict() == expected
    else:
        raise ValueError("phase must be write or read")
    print(
        json.dumps(
            {
                "phase": phase,
                "revision": repository.revision,
                "fingerprint": repository.snapshot().fingerprint,
            }
        )
    )


if __name__ == "__main__":
    main()
