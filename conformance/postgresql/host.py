# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Disposable conformance host; owns stop, crash injection and bounded test callbacks."""

# -I excludes even this harness directory; load only the example by absolute path.
import importlib.util
import json
import os
import sys
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from threading import Event
from time import monotonic, sleep

from meridian_storage.projection import ProjectionRunner, ProjectionSpec
from meridian_storage.registry.resources import (
    NamespaceDefinition,
    ResourceBundle,
    ResourceDefinition,
    ResourceRef,
    SchemaDefinition,
    SchemaRef,
)
from meridian_storage.runtime.config import BindingConfig, RuntimeConfig
from meridian_storage.spi.adapters import AdapterCreateContext, SecretValue

from meridian_storage import Meridian, OperationContext
from meridian_storage.adapters.postgresql import PostgreSQLAdapterFactory, PostgreSQLOutbox

example_path = Path(__file__).resolve().parents[2] / "examples/versioned_target.py"
module_spec = importlib.util.spec_from_file_location("versioned_target_example", example_path)
example = importlib.util.module_from_spec(module_spec)
module_spec.loader.exec_module(example)
payload = json.load(sys.stdin)
bundle = payload["bundle"]
bundle = ResourceBundle(
    bundle["providerId"],
    bundle["providerVersion"],
    bundle["providerContractVersion"],
    namespaces=tuple(NamespaceDefinition(**n) for n in bundle["namespaces"]),
    schemas=tuple(
        SchemaDefinition(SchemaRef.parse(s["ref"]), s["definition"]) for s in bundle["schemas"]
    ),
    resources=tuple(
        ResourceDefinition(
            ResourceRef.parse(r["ref"]),
            r["profile"],
            schema=SchemaRef.parse(r["schema"]),
            required_scope=tuple(r["requiredScope"]),
        )
        for r in bundle["resources"]
    ),
)


class Schemas:
    provider_id = bundle.provider_id
    provider_contract_version = "1.0.0"

    def load(self):
        return bundle


class Secrets:
    def resolve(self, ref):
        return SecretValue(
            payload["identity" if ref.reference == "identity" else "credential"].encode()
        )


runtime = PostgreSQLAdapterFactory().create(
    AdapterCreateContext(
        binding=BindingConfig.from_mapping(payload["binding"], "binding"),
        identity=SecretValue(payload["identity"].encode()),
        credential=SecretValue(payload["credential"].encode()),
    )
)
runtime.open()
context = OperationContext(principal_ref="test:host", tenant="a", scope={"workspace": "a"})
port = PostgreSQLOutbox(
    runtime,
    resource="example.outbox",
    spec=ProjectionSpec(**payload["spec"]),
    context=context,
    poison_threshold=2,
)

meridian = Meridian(
    RuntimeConfig.from_mapping(payload["config"]),
    schema_providers=[Schemas()],
    secret_resolver=Secrets(),
)
meridian.start()
stop = Event()
phase = payload["phase"]
project = example.make_projector(
    meridian, target="example.target", scope={"tenant": "a", "workspace": "a"}
)
stop_at = None


def intercepted_project(source, context):
    global stop_at
    if phase == "after-claim":
        os._exit(91)
    if phase == "stuck":
        stop.set()
        print("entered", flush=True)
        Event().wait()
    if phase == "graceful" and not stop.is_set():
        stop_at = monotonic()
        stop.set()
    # This test callback has a 0.1 s admission allowance.
    sleep(0.01)
    return project(source, context)


def acknowledge(result, source):
    assert type(result.data["sourceVersion"]) is int
    assert result.data["sourceVersion"] == source.source_version
    if phase == "after-target":
        os._exit(92)
    return result.data["sourceVersion"], result.operation_fingerprint


class CrashBeforeCheckpoint:
    # Fault injection delegates all real storage operations to the released port.
    def atomic_claim(self, **kw):
        return port.atomic_claim(**kw)

    def complete(self, *args, **kw):
        os._exit(93)

    def release(self, *args, **kw):
        return port.release(*args, **kw)

    def lag(self, **kw):
        return port.lag(**kw)


runner = ProjectionRunner(
    meridian=meridian,
    spec=ProjectionSpec(**payload["spec"]),
    project=intercepted_project,
    acknowledgement=acknowledge,
    outbox=CrashBeforeCheckpoint() if phase == "before-checkpoint" else port,
    worker_id="child-host",
    batch_size=2 if phase == "graceful" else 1,
    lease_seconds=120 if phase == "graceful" else 1,
    clock=lambda: datetime(2026, 1, 1, tzinfo=UTC),
)
with meridian.context(context):
    outcome = runner.run_until_stopped(stop, poll_interval_seconds=0)
print(json.dumps({"outcome": asdict(outcome), "drain_seconds": monotonic() - stop_at}), flush=True)
meridian.close()
runtime.close()
