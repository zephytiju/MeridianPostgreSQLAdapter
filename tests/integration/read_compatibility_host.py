# SPDX-License-Identifier: Apache-2.0
"""Separate-process acceptance using normally installed old and candidate wheels."""

from __future__ import annotations

import json
import os
import sys
from copy import deepcopy
from dataclasses import replace
from importlib.metadata import version
from pathlib import Path

from meridian_storage.context import OperationContext
from meridian_storage.runtime.config import BindingConfig
from meridian_storage.runtime.operations import Operation
from meridian_storage.spi.adapters import AdapterCreateContext, ExecutionRequest, SecretValue
from psycopg import connect, sql
from psycopg.conninfo import conninfo_to_dict

from meridian_storage.adapters.postgresql import PostgreSQLAdapterFactory
from meridian_storage.adapters.postgresql._settings import PostgreSQLSettings
from meridian_storage.adapters.postgresql.descriptor import manifest
from meridian_storage.adapters.postgresql.migration import MigrationExecutor
from meridian_storage.adapters.postgresql.schema import SchemaCompiler

# Only fixture construction is shared. Adapter/runtime behavior comes from the
# installed distributions in each process, never from another checkout.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from conftest import fp, physical_resources, read_compatible_binding

NAMESPACE = "meridian_released_writer_compatibility"
IDENTITY = "00000000-0000-0000-0000-000000000001"


def packages():
    return {
        name: version(name)
        for name in (
            "meridian-storage-core",
            "meridian-storage-semantics",
            "meridian-storage-postgresql",
        )
    }


def runtime(binding):
    credentials = conninfo_to_dict(os.environ["MERIDIAN_POSTGRESQL_TEST_DSN"])
    result = PostgreSQLAdapterFactory().create(
        AdapterCreateContext(
            binding=binding,
            identity=SecretValue(credentials["user"].encode()),
            credential=SecretValue(credentials["password"].encode()),
        )
    )
    result.open()
    return result


def execute(selected, method, *, name="Legacy writer", workspace="workspace-a"):
    operation = Operation(
        catalog="structured",
        operation_contract=f"meridian.structured.{method}",
        operation_version="1.0.0",
        resources=(next(iter(selected.settings.resources.values())).ref,),
        input={"data": {"id": IDENTITY, "name": name}}
        if method == "put"
        else {"where": {"id": IDENTITY}},
        read_only=method == "get",
        idempotent=True,
    )
    request = ExecutionRequest(
        operation=operation,
        context=OperationContext(
            principal_ref="acceptance", tenant="tenant-a", scope={"workspace": workspace}
        ),
        request_id=method,
        execution_id=method,
        binding_id="postgresql-test",
        registry_revision=1,
        registry_fingerprint=fp("registry"),
        attempt=1,
    )
    session = selected.open_session(transactional=False)
    try:
        return session.execute(request).data
    finally:
        session.close()


def ledger():
    with connect(os.environ["MERIDIAN_POSTGRESQL_TEST_DSN"]) as connection:
        rows = connection.execute(
            sql.SQL("SELECT * FROM {}.__meridian_resources ORDER BY resource_ref").format(
                sql.Identifier(NAMESPACE)
            )
        ).fetchall()
        return json.loads(json.dumps(rows, default=str))


def main():
    phase, path = sys.argv[1:]
    receipt_path = Path(path)
    import meridian_storage.adapters.postgresql as installed

    assert (
        not Path(installed.__file__)
        .resolve()
        .is_relative_to(Path(__file__).resolve().parents[2] / "src")
    )
    if phase == "write":
        assert packages()["meridian-storage-postgresql"] == "1.0.0"
        candidate = replace(
            read_compatible_binding(os.environ["MERIDIAN_POSTGRESQL_TEST_DSN"]),
            physical_namespace=NAMESPACE,
        )
        raw = candidate.to_dict()["settings"]
        proof = raw.pop("readCompatibility")
        from meridian_storage.registry.resources import ResourceDefinition

        # The exact stored fingerprint is computed by the installed old Core.
        definition = proof["resources"][0]["storedResource"]
        from meridian_storage.registry.resources import (
            CapabilityRequirement,
            ResourceRef,
            SchemaRef,
        )

        stored = ResourceDefinition(
            ref=ResourceRef.parse(definition["ref"]),
            profile=definition["profile"],
            schema=SchemaRef.parse(definition["schema"]),
            required_scope=tuple(definition["requiredScope"]),
            requirements=tuple(
                CapabilityRequirement.from_mapping(r) for r in definition["requirements"]
            ),
        )
        assert stored.to_dict() == definition
        raw["resources"][0]["resourceFingerprint"] = stored.fingerprint
        binding = replace(candidate, settings=raw)
        settings = PostgreSQLSettings.from_binding(binding)
        plan = SchemaCompiler(settings).compile()
        with connect(os.environ["MERIDIAN_POSTGRESQL_TEST_DSN"]) as connection:
            connection.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(NAMESPACE))
            )
            MigrationExecutor(settings).apply(connection, plan)
        binding = replace(binding, required_physical_fingerprint=plan.physical_fingerprint)
        selected = runtime(binding)
        try:
            selected.probe()
            assert (
                selected.verify_physical(physical_resources(settings)).fingerprint
                == plan.physical_fingerprint
            )
            execute(selected, "put")
            execute(selected, "put", name="Other owner", workspace="workspace-b")
            assert execute(selected, "get")["name"] == "Legacy writer"
        finally:
            selected.close()
        receipt = {
            "binding": binding.to_dict(),
            "proof": proof,
            "ledger": ledger(),
            "writerPackages": packages(),
        }
        receipt_path.write_text(json.dumps(receipt, indent=2) + "\n")
    else:
        receipt = json.loads(receipt_path.read_text())
        raw = deepcopy(receipt["binding"])
        if phase == "read":
            assert packages()["meridian-storage-postgresql"] != "1.0.0"
            expected_proof = read_compatible_binding().to_dict()["settings"]["readCompatibility"]
            assert receipt["proof"] == expected_proof
            raw["settings"]["readCompatibility"] = expected_proof
            current_layout = read_compatible_binding().to_dict()["settings"]["resources"][0]
            assert (
                raw["settings"]["resources"][0]["schemaFingerprint"]
                == current_layout["schemaFingerprint"]
            )
            raw["settings"]["resources"][0]["resourceFingerprint"] = current_layout[
                "resourceFingerprint"
            ]
            raw["requiredCapabilityFingerprint"] = manifest(
                raw["engineProfile"], raw["engineVersion"]
            ).fingerprint
        elif phase != "restart-writer":
            raise ValueError("unsupported acceptance phase")
        binding = BindingConfig.from_mapping(raw, "released-writer")
        selected = runtime(binding)
        try:
            selected.probe()
            assert (
                selected.verify_physical(physical_resources(selected.settings)).fingerprint
                == raw["requiredPhysicalFingerprint"]
            )
            assert execute(selected, "get")["name"] == "Legacy writer"
            assert execute(selected, "get", workspace="workspace-b")["name"] == "Other owner"
            assert execute(selected, "get", workspace="unrelated") is None
            if phase == "restart-writer":
                assert packages() == receipt["writerPackages"]
                execute(selected, "put", name="Legacy writer restarted")
                assert execute(selected, "get")["name"] == "Legacy writer restarted"
        finally:
            selected.close()
        assert ledger() == receipt["ledger"]
    print(
        json.dumps(
            {
                "phase": phase,
                "passed": True,
                "packages": packages(),
                "installedAdapter": installed.__file__,
            }
        )
    )


if __name__ == "__main__":
    main()
