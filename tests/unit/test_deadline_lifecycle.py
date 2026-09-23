"""Deterministic offline owner tests: no sockets, services, sleeps or native I/O."""

from threading import Event
from types import SimpleNamespace

import pytest
from meridian_storage.context import bind_context
from psycopg_pool import PoolTimeout

from meridian_storage import CommitOutcomeError, CommitState, MeridianTimeoutError, OperationContext
from meridian_storage.adapters.postgresql import _deadline
from meridian_storage.adapters.postgresql._deadline import (
    DeadlineConnection,
    OperationBudget,
    bounded_io,
)
from meridian_storage.adapters.postgresql.transactions import PostgreSQLAdapterSession


@pytest.fixture
def clock(monkeypatch):
    value = [100.0]
    monkeypatch.setattr(_deadline.time, "monotonic", lambda: value[0])
    return value


class Selector:
    def __init__(self, clock, ready=False):
        self.clock, self.ready, self.waits = clock, ready, []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def register(self, *args):
        pass

    def modify(self, *args):
        pass

    def select(self, timeout):
        self.waits.append(timeout)
        self.clock[0] += timeout
        return [(None, 1)] if self.ready else []


def stalled():
    while True:
        yield 1


class Conn:
    def __init__(self, clock):
        self.clock = clock
        self.calls = []
        self.stall_phase = None
        self.late = False
        self.error = None
        self.closed = False

    def _run(self, phase):
        self.calls.append(phase)
        if self.stall_phase == phase:
            _deadline.consume(
                stalled(), 123, _deadline._io_budget.get(), lambda: Selector(self.clock)
            )
        if phase == "commit" and self.error:
            raise self.error
        if phase == "commit" and self.late:
            self.clock[0] += 3

    def execute(self, *args):
        self._run("begin")

    def commit(self):
        self._run("commit")

    def rollback(self):
        self._run("rollback")

    def close(self):
        self.closed = True
        self.calls.append("close")


class Pool:
    def __init__(self, conn):
        self.conn, self.timeouts, self.returned = conn, [], []
        self.stall = False
        self.late = False
        self.fail_return = False

    def getconn(self, timeout):
        self.timeouts.append(timeout)
        if self.stall:
            self.conn.clock[0] += timeout
            raise PoolTimeout()
        if self.late:
            self.conn.clock[0] += 3
        return self.conn

    def putconn(self, conn):
        self.returned.append(conn)
        if self.fail_return:
            raise RuntimeError("cleanup failure")


def session(pool):
    return PostgreSQLAdapterSession(
        pool,
        None,
        binding_id="primary",
        transactional=True,
        acquire_timeout_ms=30000,
        operation_timeout_ms=30000,
        max_result_bytes=10000,
    )


def ctx(clock, cancellation=None):
    return OperationContext(
        "synthetic", request_id="req", monotonic_deadline=clock[0] + 2, cancellation=cancellation
    )


@pytest.mark.parametrize("phase", ["begin", "commit", "rollback"])
def test_stalled_lifecycle_consumes_only_inherited_budget(clock, phase):
    conn = Conn(clock)
    pool = Pool(conn)
    adapter = session(pool)
    with bind_context(ctx(clock)):
        if phase != "begin":
            adapter.begin()
            clock[0] += 1.4
        conn.stall_phase = phase
        with pytest.raises(MeridianTimeoutError) as error:
            getattr(adapter, phase)()
        assert clock[0] == pytest.approx(102)
        assert conn.closed
        if phase == "commit":
            assert error.value.commit_state == CommitState.UNKNOWN_COMMIT
            assert error.value.retryable is False
        else:
            assert adapter.commit_state == CommitState.KNOWN_NOT_COMMITTED
        adapter.close()
    assert len(pool.returned) == 1


def test_stalled_pool_acquire_is_bounded_without_begin(clock):
    conn = Conn(clock)
    pool = Pool(conn)
    pool.stall = True
    with bind_context(ctx(clock)), pytest.raises(MeridianTimeoutError):
        session(pool).begin()
    assert clock[0] == pytest.approx(102)
    assert conn.calls == []
    assert max(pool.timeouts) <= 0.05


def test_late_acquisition_is_discarded_before_begin(clock):
    conn = Conn(clock)
    pool = Pool(conn)
    pool.late = True
    with bind_context(ctx(clock)), pytest.raises(MeridianTimeoutError):
        session(pool).begin()
    assert conn.calls == ["close"]


@pytest.mark.parametrize("error", [OSError("lost ack"), KeyboardInterrupt()])
def test_ambiguous_ack_is_typed_nonretryable_and_never_rolled_back(clock, error):
    conn = Conn(clock)
    conn.error = error
    adapter = session(Pool(conn))
    with bind_context(ctx(clock)):
        adapter.begin()
        with pytest.raises(CommitOutcomeError) as caught:
            adapter.commit()
        adapter.close()
    assert caught.value.to_dict()["commitState"] == "unknown-commit"
    assert caught.value.retryable is False
    assert conn.calls == ["begin", "commit", "close"]


def test_late_ack_preserves_known_committed_and_denies_success(clock):
    conn = Conn(clock)
    conn.late = True
    adapter = session(Pool(conn))
    with bind_context(ctx(clock)):
        adapter.begin()
        with pytest.raises(CommitOutcomeError) as caught:
            adapter.commit()
    assert caught.value.commit_state == CommitState.KNOWN_COMMITTED
    assert "rollback" not in conn.calls


def test_success_has_typed_known_committed(clock):
    conn = Conn(clock)
    adapter = session(Pool(conn))
    with bind_context(ctx(clock)):
        adapter.begin()
        adapter.commit()
    assert adapter.commit_state == CommitState.KNOWN_COMMITTED


@pytest.mark.parametrize("phase", ["begin", "commit"])
def test_cancelled_admission_closes_known_uncommitted_effects(clock, phase):
    cancellation = Event()
    conn = Conn(clock)
    adapter = session(Pool(conn))
    with bind_context(ctx(clock, cancellation)):
        if phase == "commit":
            adapter.begin()
        cancellation.set()
        with pytest.raises(MeridianTimeoutError):
            getattr(adapter, phase)()
        adapter.close()
    assert phase not in conn.calls
    assert adapter.commit_state == CommitState.KNOWN_NOT_COMMITTED


def test_close_after_expiry_never_starts_rollback(clock):
    conn = Conn(clock)
    adapter = session(Pool(conn))
    with bind_context(ctx(clock)):
        adapter.begin()
        clock[0] += 3
        adapter.close()
    assert conn.calls == ["begin", "close"]


def test_commit_cleanup_failure_keeps_committed_state(clock):
    conn = Conn(clock)
    pool = Pool(conn)
    pool.fail_return = True
    adapter = session(pool)
    with bind_context(ctx(clock)):
        adapter.begin()
        with pytest.raises(CommitOutcomeError) as caught:
            adapter.commit()
    assert caught.value.commit_state == CommitState.KNOWN_COMMITTED


def test_commit_primary_unknown_survives_cleanup_failure(clock):
    conn = Conn(clock)
    conn.error = OSError("ack lost")
    pool = Pool(conn)
    pool.fail_return = True
    adapter = session(pool)
    with bind_context(ctx(clock)):
        adapter.begin()
        with pytest.raises(CommitOutcomeError) as caught:
            adapter.commit()
    assert caught.value.commit_state == CommitState.UNKNOWN_COMMIT
    assert isinstance(caught.value.__cause__, OSError)


def test_deadline_connection_discards_without_cancel_and_drain(clock, monkeypatch):
    calls = []
    original = _deadline.consume
    monkeypatch.setattr(
        _deadline, "consume", lambda g, fd, b: original(g, fd, b, lambda: Selector(clock))
    )
    fake = SimpleNamespace(
        pgconn=SimpleNamespace(socket=123, finish=lambda: calls.append("finish"))
    )
    with bounded_io(OperationBudget(2)), pytest.raises(MeridianTimeoutError):
        DeadlineConnection.wait(fake, stalled())
    assert calls == ["finish"]
    assert clock[0] == pytest.approx(102)


def test_ready_socket_does_not_bypass_expiry(clock):
    budget = OperationBudget(0.05)
    effects = []

    def command():
        yield 1
        effects.append("continued")

    with pytest.raises(MeridianTimeoutError):
        _deadline.consume(command(), 123, budget, lambda: Selector(clock, ready=True))
    assert effects == []


def test_nontransactional_execute_bounds_commit_and_does_not_retry(clock):
    conn = Conn(clock)
    conn.error = OSError("lost ack")
    adapter = session(Pool(conn))
    adapter._transactional = False
    calls = []
    adapter._execute = lambda *args: calls.append("execute") or object()
    request = SimpleNamespace(context=ctx(clock))
    with pytest.raises(CommitOutcomeError):
        adapter.execute(request)
    assert calls == ["execute"]
    assert conn.calls == ["begin", "commit", "close"]


def test_aborted_transaction_is_never_claimed_committed(clock):
    from psycopg.pq import TransactionStatus

    conn = Conn(clock)
    conn.info = SimpleNamespace(transaction_status=TransactionStatus.INERROR)
    adapter = session(Pool(conn))
    with bind_context(ctx(clock)):
        adapter.begin()
        with pytest.raises(CommitOutcomeError) as caught:
            adapter.commit()
    assert caught.value.commit_state == CommitState.KNOWN_NOT_COMMITTED
    assert "commit" not in conn.calls


def test_cancellation_during_io_does_not_resume_generator(clock):
    event = Event()
    budget = OperationBudget(30, ctx(clock, event))
    effects = []

    class CancelSelector(Selector):
        def select(self, timeout):
            event.set()
            return [(None, 1)]

    def command():
        yield 1
        effects.append("resumed")

    with pytest.raises(MeridianTimeoutError):
        _deadline.consume(command(), 123, budget, lambda: CancelSelector(clock))
    assert effects == []


def test_execute_io_and_commit_share_begin_budget(clock):
    conn = Conn(clock)
    adapter = session(Pool(conn))
    with bind_context(ctx(clock)):
        adapter.begin()
        clock[0] += 1.9

        def execute(connection, request):
            return _deadline.consume(
                stalled(), 123, _deadline._io_budget.get(), lambda: Selector(clock)
            )

        adapter._execute = execute
        with pytest.raises(MeridianTimeoutError):
            adapter.execute(SimpleNamespace(context=ctx(clock)))
        adapter.close()
    assert clock[0] == pytest.approx(102)
    assert "commit" not in conn.calls
    assert conn.closed


def test_runtime_admission_lock_is_bounded_by_context(clock):
    from meridian_storage.adapters.postgresql._runtime import PostgreSQLAdapterRuntime

    waits = []

    class Lock:
        def acquire(self, *, timeout):
            waits.append(timeout)
            return False

    runtime = SimpleNamespace(_lock=Lock())
    with bind_context(ctx(clock)), pytest.raises(MeridianTimeoutError):
        PostgreSQLAdapterRuntime._require_pool(runtime)
    assert waits == [2.0]


@pytest.mark.parametrize("timeout,elapsed", [(0, 0), (0.2, 0.2), (50, 2)])
def test_driver_wait_timeout_can_only_narrow_inherited_budget(clock, monkeypatch, timeout, elapsed):
    calls = []
    fake = SimpleNamespace(
        pgconn=SimpleNamespace(socket=123, finish=lambda: calls.append("finish"))
    )
    original = _deadline.consume
    monkeypatch.setattr(
        _deadline,
        "consume",
        lambda gen, fd, budget: original(gen, fd, budget, lambda: Selector(clock)),
    )
    budget = OperationBudget(2)
    with bounded_io(budget), pytest.raises(MeridianTimeoutError):
        DeadlineConnection.wait(fake, stalled(), timeout=timeout)
    assert clock[0] == pytest.approx(100 + elapsed)
    assert budget.expires == 102  # a per-call timeout does not mutate the parent budget
    assert calls == ["finish"]
