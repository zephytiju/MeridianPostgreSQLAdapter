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

"""Acceptance of the approved versioned target and host-drain composition."""

from __future__ import annotations

import inspect
import json
import selectors
import subprocess
import sys
from dataclasses import replace
from datetime import timedelta
from importlib.metadata import version
from pathlib import Path

import pytest
from conftest import NOW, ctx, intent
from example import composition
from meridian_storage.projection import (
    InMemoryEvidenceSink,
    ProjectionContext,
    ProjectionRunner,
    TransactionalOutboxWriter,
)
from meridian_storage.projection.testing import OutboxConformanceTarget, run_outbox_conformance

SCOPE = {"tenant": "a", "workspace": "a"}
HOST = Path(__file__).with_name("host.py")


def context(data):
    return ProjectionContext(
        "example-projection",
        data.event_id,
        data.source_catalog,
        data.source_resource,
        data.source_schema,
        data.source_identity,
        data.source_version,
        data.mutation_kind,
        data.occurred_at,
        data.operation_context,
    )


def write(h, identity="case", number=1, *, deleted=False, name="first"):
    payload = {"id": identity, "name": name, "deleted": deleted}
    data = intent(
        f"{identity}-v{number}", source_identity=identity, source_version=number, payload=payload
    )
    with h.meridian.context(ctx()):
        result = TransactionalOutboxWriter(h.meridian, outbox_resource="example.outbox").commit(
            h.meridian.catalog("structured").put(
                resource="example.source",
                data=payload,
                mode="if_absent" if number == 1 else "update",
            ),
            data,
        )
    assert result.data["recordVersion"] == number
    return data


def projector(h):
    return composition.make_projector(h.meridian, target="example.target", scope=SCOPE)


def runner(h, port, **kw):
    return ProjectionRunner(
        meridian=h.meridian,
        spec=h.spec,
        project=projector(h),
        outbox=port,
        clock=lambda: NOW + timedelta(seconds=2),
        batch_size=1,
        **kw,
    )


def run(h, port, **kw):
    with h.meridian.context(ctx()):
        return runner(h, port, **kw).run_once()


def rows(h):
    # Complete every page before latest/tombstone/business filtering. The fixture
    # is quiescent and bounded; live pagination is not a snapshot guarantee.
    all_rows, cursor = [], None
    with h.meridian.context(ctx()):
        while True:
            page = h.meridian.execute(
                h.meridian.catalog("structured").query(
                    resource="example.target",
                    limit=1,
                    cursor=cursor,
                )
            ).data
            all_rows.extend(page["items"])
            cursor = page["cursor"]
            if not cursor:
                return all_rows


def child(h, phase):
    process = subprocess.Popen(
        [sys.executable, "-I", str(HOST)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert process.stdin is not None
    process.stdin.write(json.dumps({**h.child_config, "phase": phase}))
    process.stdin.close()
    process.stdin = None
    return process


def exited_child(h, phase, code):
    process = child(h, phase)
    try:
        out, err = process.communicate(timeout=30)
        assert process.returncode == code, (out, err)
        return out
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)


def test_production_imports_are_exact_released_artifacts():
    from meridian_storage.adapters.postgresql import PostgreSQLOutbox

    for cls in (ProjectionRunner, TransactionalOutboxWriter, PostgreSQLOutbox):
        assert "site-packages" in Path(inspect.getfile(cls)).parts
    selected = dict(
        line.split("==", 1)
        for line in Path(__file__).with_name("requirements.txt").read_text().splitlines()
        if line and not line.startswith("#")
    )
    assert {name: version(name) for name in selected} == selected



@pytest.mark.parametrize(
    "phase,code,has_target",
    [
        ("after-claim", 91, False),
        ("after-target", 92, True),
        ("before-checkpoint", 93, True),
    ],
)
def test_abrupt_crash_boundaries_preserve_intent_and_recover(durable, phase, code, has_target):
    data = write(durable)
    exited_child(durable, phase, code)
    port = durable.reopen()
    assert bool(rows(durable)) is has_target
    assert port.get(data.event_id).data == data
    assert port.get(data.event_id).state.value == "LEASED"
    assert port.checkpoint(data.partition_key).revision == 0
    assert run(durable, port).completed == 1
    assert port.get(data.event_id).attempt_count == 2
    assert port.checkpoint(data.partition_key).source_version == 1
    assert run(durable, port).claimed == 0


def test_replay_v1_after_v2_restart_and_latest_tombstone_reads(durable):
    first = write(durable)
    exited_child(durable, "after-target", 92)
    second = write(durable, number=2, name="second")
    with durable.meridian.context(ctx()):
        newer = durable.meridian.execute(projector(durable)(second.payload, context(second))).data
    assert newer["sourceVersion"] == 2
    port = durable.reopen()  # both pools and Meridian idempotency cache are new
    evidence = InMemoryEvidenceSink()
    assert run(durable, port, evidence=evidence).completed == 1
    checkpoint = port.checkpoint(first.partition_key)
    assert checkpoint.source_version == 1 and checkpoint.revision == 1
    assert evidence.events[0].details["sourceVersion"] == 1
    assert port.get(first.event_id).data == first
    current = rows(durable)
    assert sorted(row["sourceVersion"] for row in current) == [1, 2]
    assert next(row for row in current if row["sourceVersion"] == 2) == newer
    assert composition.latest_visible(current)[0]["document"]["name"] == "second"
    assert (
        composition.latest_visible(current, matches=lambda r: r["document"]["name"] == "first")
        == []
    )
    assert run(durable, port).completed == 1  # acknowledge pending v2 as well
    tombstone = write(durable, number=3, deleted=True, name="deleted")
    assert run(durable, port).completed == 1
    durable.reopen()
    with durable.meridian.context(ctx()):
        replay = durable.meridian.execute(projector(durable)(first.payload, context(first))).data
    assert replay["sourceVersion"] == 1
    current = rows(durable)
    assert len(current) == 3
    assert composition.latest_visible(current) == []
    assert (
        composition.latest_visible(current, matches=lambda r: r["document"]["name"] == "first")
        == []
    )
    assert durable.port().get(tombstone.event_id).state.value == "COMPLETED"


def test_target_failure_lag_quarantine_and_unchanged_checkpoint(durable):
    data = write(durable)
    port = durable.port()
    evidence = InMemoryEvidenceSink()
    assert port.lag(now=NOW + timedelta(seconds=4)).lag_seconds == 4
    with durable.meridian.context(ctx()):
        expression = projector(durable)(data.payload, context(data))
        durable.meridian.execute(expression)

        def conflicting(source, source_context):
            return durable.meridian.catalog("structured").put(
                resource="example.target",
                data=expression.arguments["data"],
                mode="if_absent",
            )

        outcome = ProjectionRunner(
            meridian=durable.meridian,
            spec=durable.spec,
            project=conflicting,
            outbox=port,
            clock=lambda: NOW,
            evidence=evidence,
        ).run_once()
    assert outcome.quarantined == 1
    reopened = durable.reopen()
    assert reopened.get(data.event_id).state.value == "QUARANTINED"
    assert reopened.checkpoint(data.partition_key).revision == 0
    assert reopened.lag(now=NOW).incomplete_count == 1
    assert evidence.events[0].state == "QUARANTINED"


def test_released_owner_expiry_retry_and_quarantine_contract(durable):
    report = run_outbox_conformance(
        OutboxConformanceTarget(
            outbox=durable.port(),
            seed=durable.seed,
            inspect_record=lambda event_id: durable.port().get(event_id),
            checkpoint=lambda partition: durable.port().checkpoint(partition),
            reopen=durable.reopen,
            source_resource="example.source",
            source_schema="example.source@1.0.0",
        )
    )
    assert report.same_owner_completion == "accepted-indistinguishable-owner"


def test_bounded_admitted_batch_drains_and_starts_no_next_cycle(durable):
    events = [write(durable, identity=name) for name in ("a", "b", "c")]
    outcome = json.loads(exited_child(durable, "graceful", 0))
    # Host admission: claim 10s + 2*(loader/projector/ack/evidence .1s each,
    # target 10s, completion 10s) = 50.8s < drain 60s < lease 120s.
    assert 0 <= outcome["drain_seconds"] < 60
    assert outcome["outcome"]["completed"] == 2
    assert outcome["outcome"]["claimed"] == 2
    states = [durable.port().get(event.event_id).state.value for event in events]
    assert states.count("COMPLETED") == 2 and states.count("PENDING") == 1
    print(json.dumps({"graceful_drain": True, "deadline_seconds": 60, **outcome}))


def test_stuck_callback_fails_drain_then_host_terminates_and_recovers(durable):
    data = write(durable)
    process = child(durable, "stuck")
    try:
        with selectors.DefaultSelector() as ready:
            ready.register(process.stdout, selectors.EVENT_READ)
            assert ready.select(timeout=20), "child did not enter callback"
        assert process.stdout.readline().strip() == "entered"
        # An intentionally nonconforming callback exceeds its admission budget.
        with pytest.raises(subprocess.TimeoutExpired):
            process.wait(timeout=0.2)
        print(json.dumps({"graceful_drain": False, "reason": "host-deadline-exceeded"}))
        process.terminate()
        process.wait(timeout=5)
        assert process.returncode != 0
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
    port = durable.reopen()
    assert not rows(durable)
    assert port.get(data.event_id).data == data
    assert port.checkpoint(data.partition_key).revision == 0
    assert port.get(data.event_id).state.value == "LEASED"
    assert run(durable, port).completed == 1
    assert port.get(data.event_id).attempt_count == 2


def test_identity_is_deterministic_scoped_and_rejects_opaque_versions(durable):
    data = intent(source_identity=("account", 7))
    p = projector(durable)
    first = p(data.payload, context(data)).arguments["data"]
    retry = p(data.payload, replace(context(data), event_id="different-retry")).arguments["data"]
    assert first == retry
    other = composition.make_projector(
        durable.meridian, target="example.target", scope={"tenant": "b"}
    )
    assert other(data.payload, context(data)).arguments["data"]["id"] != first["id"]
    assert (
        p(data.payload, replace(context(data), source_version=2)).arguments["data"]["id"]
        != first["id"]
    )
    for value in (True, "1", None):
        with pytest.raises(ValueError, match="integer"):
            p(data.payload, replace(context(data), source_version=value))
