"""Round registry: shared executions, result cache, slots and supervision.

Cache / sharing model
---------------------
A submission is identified by ``CacheKey(upload batch, station, sql digest)``.

1. If the key is in the result cache, the round finishes immediately
   (``succeeded`` + ``cache_hit=true``) and consumes no execution slot.
2. Otherwise, if a *shared execution* with the same key is already in flight,
   the round attaches to it as a waiter (no extra slot, no extra query).
3. Otherwise a new shared execution is started, which requires a free slot.

Only complete successful CSV output is inserted into the bounded cache.
Failures, timeouts, cancellations and oversized results are never cached.
Cancelling a round only detaches that round; the shared query is interrupted
only when no attached round is left, and its outcome is then not cached.
Every round owns its result bytes, so cache eviction never breaks downloads
of finished rounds.
"""
from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Optional

from .cache import CacheKey, ResultCache
from .config import (
    MAX_CONCURRENT_ROUNDS,
    MAX_FINISHED_ROUNDS,
    MAX_RETAINED_RESULT_BYTES,
    QUERY_TIMEOUT_SECONDS,
)
from .worker import (
    MSG_DONE,
    MSG_TOO_LARGE,
    QueryWorker,
    WorkerResult,
)

# Public round states.
PENDING = "pending"        # accepted, not yet started (reserved for future use)
RUNNING = "running"        # this round started the shared execution
WAITING = "waiting"        # waiting for a shared execution started by another round
SUCCEEDED = "succeeded"    # result published, downloadable
FAILED = "failed"          # SQL/runtime error, timeout or oversized result
CANCELLED = "cancelled"    # cancellation took effect

_TERMINAL_STATES = {SUCCEEDED, FAILED, CANCELLED}


class SlotsFullError(RuntimeError):
    """Raised when MAX_CONCURRENT_ROUNDS executions are already active."""


class RoundNotFound(KeyError):
    pass


@dataclass
class Round:
    round_id: str
    station_id: str
    sql: str
    cache_key: CacheKey
    state: str = PENDING
    error: Optional[str] = None
    result_csv: Optional[bytes] = None
    result_bytes: int = 0
    row_count: int = 0
    created_at: float = field(default_factory=time.monotonic)
    finished_at: Optional[float] = None
    cancel_requested: bool = False
    cache_hit: bool = False
    # "initiator", "waiter", or None for a pure cache hit.
    shared_role: Optional[str] = None
    # Execution this round is attached to (not exposed in the API).
    _execution: Optional["SharedExecution"] = None

    def public_dict(self) -> dict:
        data = {
            "round_id": self.round_id,
            "station_id": self.station_id,
            "state": self.state,
            "cancel_requested": self.cancel_requested,
            "cache_hit": self.cache_hit,
            "shared_role": self.shared_role,
            "cache_key": self.cache_key.sql_digest[:12],
            "batch_id": self.cache_key.batch_id,
            "elapsed_ms": int(
                ((self.finished_at or time.monotonic()) - self.created_at) * 1000
            ),
        }
        if self.state == SUCCEEDED:
            data.update(
                result_bytes=self.result_bytes,
                row_count=self.row_count,
                download_url=f"/api/rounds/{self.round_id}/result",
            )
        if self.error:
            data["error"] = self.error
        return data


@dataclass
class SharedExecution:
    """One real query execution serving one or more rounds with the same key."""

    execution_id: str
    key: CacheKey
    station_id: str
    sql: str
    columns: dict[str, list]
    # Cache generation captured at submission; stale inserts are rejected.
    cache_epoch: int
    state: str = "running"  # running | finished
    rounds: set[str] = field(default_factory=set)
    worker: Optional[QueryWorker] = None
    watchdog: Optional[threading.Timer] = None
    timeout_happened: bool = False
    cancelled: bool = False  # interrupted because no interested round remained
    worker_result: Optional[WorkerResult] = None
    started_at: float = field(default_factory=time.monotonic)
    finished_at: Optional[float] = None


class RoundRegistry:
    def __init__(
        self,
        max_active: int = MAX_CONCURRENT_ROUNDS,
        cache: Optional[ResultCache] = None,
    ) -> None:
        self._lock = threading.RLock()
        self._rounds: dict[str, Round] = {}
        # Executions that are still running or whose result is still being
        # published under this lock (looked up by cache key for sharing).
        self._executions: dict[CacheKey, SharedExecution] = {}
        # Slot accounting: an execution occupies a slot from start until its
        # worker has really exited and its outcome has been published.
        self._running_executions: set[str] = set()
        self._execution_objects: dict[str, SharedExecution] = {}
        self._max_active = max_active
        self.cache = cache if cache is not None else ResultCache()
        # Observability: how many real executions were started / completed.
        self._executions_started = 0
        self._executions_completed = 0
        self._shared_rounds = 0
        self._cache_hit_rounds = 0

    # ------------------------------------------------------------------ slots
    def active_executions(self) -> int:
        with self._lock:
            return len(self._running_executions)

    def active_count(self) -> int:
        """Number of execution slots in use (cache hits/waiters count 0)."""
        return self.active_executions()

    # ----------------------------------------------------------------- submit
    def submit(
        self,
        station_id: str,
        sql: str,
        columns: dict[str, list],
        batch_id: int,
        sql_digest: str,
    ) -> Round:
        key = CacheKey(
            batch_id=batch_id, station_id=station_id, sql_digest=sql_digest
        )
        with self._lock:
            round_id = uuid.uuid4().hex
            round_obj = Round(
                round_id=round_id,
                station_id=station_id,
                sql=sql,
                cache_key=key,
                shared_role="initiator",
            )

            # 1) Cache hit: finish immediately, never touching a slot.
            cached = self.cache.get(key)
            if cached is not None:
                round_obj.state = SUCCEEDED
                round_obj.cache_hit = True
                round_obj.shared_role = None
                round_obj.result_csv = cached.csv_bytes
                round_obj.result_bytes = cached.result_bytes
                round_obj.row_count = cached.row_count
                round_obj.finished_at = time.monotonic()
                self._cache_hit_rounds += 1
                self._rounds[round_id] = round_obj
                self._evict_finished_locked()
                return round_obj

            # 2) Attach to an in-flight execution with the same key.
            execution = self._executions.get(key)
            if execution is not None:
                execution.rounds.add(round_id)
                round_obj._execution = execution
                round_obj.shared_role = "waiter"
                round_obj.state = WAITING
                self._shared_rounds += 1
                self._rounds[round_id] = round_obj
                self._evict_finished_locked()
                return round_obj

            # 3) New shared execution requires a free slot.
            if self.active_executions() >= self._max_active:
                raise SlotsFullError(
                    f"already {self._max_active} rounds running; only "
                    f"{self._max_active} executions may run at once; retry after one finishes"
                )

            execution = SharedExecution(
                execution_id=uuid.uuid4().hex,
                key=key,
                station_id=station_id,
                sql=sql,
                columns=columns,
                cache_epoch=self.cache.epoch(),
            )
            execution.rounds.add(round_id)
            round_obj._execution = execution
            round_obj.state = RUNNING
            self._rounds[round_id] = round_obj
            self._executions[key] = execution
            self._running_executions.add(execution.execution_id)
            self._execution_objects[execution.execution_id] = execution
            self._executions_started += 1

            worker = QueryWorker(execution.execution_id, sql, columns)
            watchdog = threading.Timer(
                QUERY_TIMEOUT_SECONDS, self._on_timeout, args=(execution,)
            )
            watchdog.daemon = True
            supervisor = threading.Thread(
                target=self._supervise_execution,
                args=(execution,),
                name=f"supervisor-{execution.execution_id[:8]}",
                daemon=True,
            )
            execution.worker = worker
            execution.watchdog = watchdog

            worker.start()
            watchdog.start()
            supervisor.start()
            self._evict_finished_locked()
            return round_obj

    # -------------------------------------------------------------- supervision
    def _on_timeout(self, execution: SharedExecution) -> None:
        with self._lock:
            if execution.state == "finished":
                return
            execution.timeout_happened = True
            worker = execution.worker
        if worker is not None:
            worker.interrupt("timeout")

    def _supervise_execution(self, execution: SharedExecution) -> None:
        worker = execution.worker
        watchdog = execution.watchdog
        # Wait for the worker to really exit before publishing anything.
        worker.join()
        watchdog.cancel()
        worker_result: Optional[WorkerResult] = worker.result

        with self._lock:
            self._finalize_execution_locked(execution, worker_result)

    def _finalize_execution_locked(
        self,
        execution: SharedExecution,
        worker_result: Optional[WorkerResult],
    ) -> None:
        if execution.state == "finished":
            return
        execution.worker_result = worker_result
        execution.state = "finished"
        execution.finished_at = time.monotonic()
        self._executions_completed += 1

        success = (
            worker_result is not None
            and worker_result.kind == MSG_DONE
            and not execution.timeout_happened
            and not execution.cancelled
        )
        # Cache only complete, successful CSV output. Failures, timeouts,
        # cancellations and oversized results never reach the cache.
        if success:
            self.cache.put(
                execution.key,
                worker_result.csv_bytes,
                worker_result.row_count,
                worker_result.result_bytes,
                epoch=execution.cache_epoch,
            )

        for round_id in list(execution.rounds):
            round_obj = self._rounds.get(round_id)
            if round_obj is None:
                continue
            if round_obj.state in _TERMINAL_STATES:
                continue
            self._apply_execution_result_locked(execution, round_obj)

        execution.rounds.clear()
        execution.worker = None
        execution.watchdog = None
        self._drop_execution_locked(execution)
        self._evict_finished_locked()

    def _drop_execution_locked(self, execution: SharedExecution) -> None:
        if self._executions.get(execution.key) is execution:
            del self._executions[execution.key]
        self._running_executions.discard(execution.execution_id)
        self._execution_objects.pop(execution.execution_id, None)

    def _apply_execution_result_locked(
        self, execution: SharedExecution, round_obj: Round
    ) -> None:
        """Publish the shared execution's outcome to one attached round."""
        # A cancellation requested before finalisation always wins; a late
        # result is never opened for that round.
        if round_obj.cancel_requested:
            round_obj.state = CANCELLED
            round_obj.error = "round cancelled; result discarded"
            round_obj.result_csv = None
            round_obj.finished_at = time.monotonic()
            round_obj._execution = None
            return

        worker_result = execution.worker_result
        if execution.timeout_happened:
            round_obj.state = FAILED
            round_obj.error = (
                f"query exceeded {QUERY_TIMEOUT_SECONDS:.0f} s and was terminated; "
                "no partial results are available"
            )
            round_obj.result_csv = None
        elif execution.cancelled:
            round_obj.state = CANCELLED
            round_obj.error = "shared execution was cancelled; result discarded"
            round_obj.result_csv = None
        elif worker_result is None:
            round_obj.state = FAILED
            round_obj.error = "query worker exited without producing a result"
            round_obj.result_csv = None
        elif worker_result.kind == MSG_DONE:
            round_obj.state = SUCCEEDED
            round_obj.result_csv = worker_result.csv_bytes
            round_obj.result_bytes = worker_result.result_bytes
            round_obj.row_count = worker_result.row_count
            round_obj.error = None
        elif worker_result.kind == MSG_TOO_LARGE:
            round_obj.state = FAILED
            round_obj.error = worker_result.error
            round_obj.result_csv = None
        else:
            round_obj.state = FAILED
            round_obj.error = worker_result.error or "query failed"
            round_obj.result_csv = None

        round_obj.finished_at = time.monotonic()
        round_obj._execution = None

    # ------------------------------------------------------------------ cancel
    def request_cancel(self, round_id: str) -> tuple[bool, str]:
        """Cancel one round without disturbing a shared execution others need.

        * Cache-hit / finished rounds cannot be cancelled once succeeded.
        * A waiter is detached immediately and the query is left alone.
        * The initiator is detached immediately too if other rounds are still
          attached; otherwise the shared query is interrupted and the round is
          marked cancelled only after the worker really exits.
        """
        with self._lock:
            round_obj = self._get_locked(round_id)
            if round_obj.state == SUCCEEDED:
                return False, "round already succeeded and published a result; cannot cancel"
            if round_obj.state in _TERMINAL_STATES:
                return False, f"round is already in terminal state '{round_obj.state}'"
            if round_obj.cancel_requested:
                return True, "cancellation already requested"

            round_obj.cancel_requested = True
            execution = round_obj._execution
            if execution is None:
                # Not attached (defensive): finish it directly.
                round_obj.state = CANCELLED
                round_obj.error = "round cancelled; result discarded"
                round_obj.finished_at = time.monotonic()
                return True, "cancelled"

            execution.rounds.discard(round_id)
            if not execution.rounds:
                # This was the last interested round: stop the shared query.
                # The round stays attached so the supervisor finalises it as
                # cancelled after the worker really exits (slot released then).
                execution.cancelled = True
                execution.rounds.add(round_id)
                # Stop accepting new waiters for this doomed execution; the
                # slot itself stays occupied until the worker exits.
                if self._executions.get(execution.key) is execution:
                    del self._executions[execution.key]
                worker = execution.worker
            else:
                worker = None
                round_obj._execution = None
                round_obj.state = CANCELLED
                round_obj.error = "round cancelled; result discarded"
                round_obj.result_csv = None
                round_obj.finished_at = time.monotonic()

        if worker is not None:
            worker.interrupt("cancel")
            return True, (
                "cancellation accepted; no other round was waiting, the shared "
                "execution is being stopped"
            )
        return True, (
            "cancelled this round only; the shared execution continues for the "
            "remaining round(s)"
        )

    # ------------------------------------------------------------------- reads
    def get(self, round_id: str) -> Round:
        with self._lock:
            return self._get_locked(round_id)

    def _get_locked(self, round_id: str) -> Round:
        try:
            return self._rounds[round_id]
        except KeyError:
            raise RoundNotFound(round_id) from None

    def list_rounds(self) -> list[dict]:
        with self._lock:
            return [r.public_dict() for r in self._rounds.values()]

    def snapshot(self, round_id: str) -> dict:
        return self.get(round_id).public_dict()

    def cache_stats(self) -> dict:
        return self.cache.stats()

    def execution_stats(self) -> dict:
        with self._lock:
            return {
                "executions_started": self._executions_started,
                "executions_completed": self._executions_completed,
                "shared_waiting_rounds": self._shared_rounds,
                "cache_hit_rounds": self._cache_hit_rounds,
                "active_executions": self.active_executions(),
            }

    def reset(self) -> None:
        """Test helper: stop everything and forget all state and counters."""
        self.shutdown()
        with self._lock:
            self._rounds.clear()
            self._executions.clear()
            self._running_executions.clear()
            self._execution_objects.clear()
            self.cache.invalidate_all()
            self._executions_started = 0
            self._executions_completed = 0
            self._shared_rounds = 0
            self._cache_hit_rounds = 0

    # ----------------------------------------------------------------- helpers
    def _evict_finished_locked(self) -> None:
        """Keep the newest finished rounds, bounded by count and result bytes.

        Only metadata/results of *finished* rounds are evicted; active rounds,
        cache hits, waiting rounds and the shared executions are untouched.
        """
        finished = [
            r for r in self._rounds.values() if r.state in _TERMINAL_STATES
        ]
        if len(finished) <= MAX_FINISHED_ROUNDS and sum(
            len(r.result_csv or b"") for r in finished
        ) <= MAX_RETAINED_RESULT_BYTES:
            return
        finished.sort(key=lambda r: r.finished_at or 0.0)  # oldest first
        keep: set[str] = set()
        total = 0
        for round_obj in reversed(finished):  # newest first
            if len(keep) >= MAX_FINISHED_ROUNDS:
                break
            size = len(round_obj.result_csv or b"")
            if keep and total + size > MAX_RETAINED_RESULT_BYTES:
                break
            keep.add(round_obj.round_id)
            total += size
        for round_obj in finished:
            if round_obj.round_id not in keep:
                self._rounds.pop(round_obj.round_id, None)

    def shutdown(self) -> None:
        with self._lock:
            items = [
                (e.worker, e.watchdog)
                for e in list(self._execution_objects.values())
                if e.state != "finished"
            ]
            for e in self._execution_objects.values():
                if e.state != "finished":
                    e.cancelled = True
        for worker, watchdog in items:
            try:
                if watchdog is not None:
                    watchdog.cancel()
            except Exception:
                pass
            try:
                if worker is not None:
                    worker.interrupt("shutdown")
            except Exception:
                pass
        for worker, _ in items:
            try:
                if worker is not None:
                    worker.join(timeout=3.0)
            except Exception:
                pass
