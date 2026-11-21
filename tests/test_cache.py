"""Tests for the result cache, shared executions and batch invalidation."""
from __future__ import annotations

import threading
import time


from app.cache import CacheKey, ResultCache
from app.config import MAX_CACHE_BYTES, MAX_CACHE_ENTRIES
from app.registry import CANCELLED, FAILED, RUNNING, SUCCEEDED, WAITING


# --------------------------------------------------------------------- helper
def _key(n: int, station: str = "ST001", digest: str = "d") -> CacheKey:
    return CacheKey(batch_id=n, station_id=station, sql_digest=digest)


# ----------------------------------------------------------------- unit tests
def test_cache_lru_order_and_entry_limit():
    cache = ResultCache(max_entries=2, max_bytes=1024)
    assert cache.put(_key(1), b"a", 1, 1)
    assert cache.put(_key(2), b"b", 1, 1)
    assert cache.get(_key(1)) is not None  # touches entry 1
    assert cache.put(_key(3), b"c", 1, 1)  # evicts entry 2 (least recent)
    assert cache.get(_key(1)) is not None
    assert cache.get(_key(2)) is None
    assert cache.get(_key(3)) is not None
    stats = cache.stats()
    assert stats["entries"] == 2
    assert stats["evictions"] == 1


def test_cache_byte_limit_evicts_older_entries():
    cache = ResultCache(max_entries=8, max_bytes=10)
    assert cache.put(_key(1), b"x" * 6, 1, 6)
    assert cache.put(_key(2), b"y" * 4, 1, 4)
    assert cache.put(_key(3), b"z" * 7, 1, 7)  # needs room: evict 1 and 2
    assert cache.get(_key(1)) is None
    assert cache.get(_key(2)) is None
    assert cache.get(_key(3)) is not None
    assert cache.stats()["bytes"] == 7


def test_cache_rejects_oversized_single_entry():
    cache = ResultCache(max_entries=8, max_bytes=5)
    assert cache.put(_key(1), b"x" * 6, 1, 6) is False
    assert len(cache) == 0


def test_cache_stale_epoch_insert_is_rejected():
    cache = ResultCache()
    epoch = cache.epoch()
    cache.invalidate_all()
    assert cache.put(_key(1), b"outdated", 1, 8, epoch=epoch) is False
    assert len(cache) == 0


def test_cache_limits_match_requirements():
    assert MAX_CACHE_ENTRIES == 8
    assert MAX_CACHE_BYTES == 16 * 1024 * 1024


# ---------------------------------------------------------------- API helpers
SLOW_SQL = (
    "WITH RECURSIVE seq(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM seq WHERE x < 150000) "
    "SELECT count(*) AS n FROM seq"
)
INFINITE_SQL = (
    "WITH RECURSIVE seq(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM seq) "
    "SELECT count(*) FROM seq"
)


def _upload(client, path):
    with open(path, "rb") as fh:
        resp = client.post(
            "/api/upload", files={"file": (path.name, fh, "text/csv")}
        )
    assert resp.status_code == 200, resp.text
    return resp.json()


def _wait_terminal(client, round_id, timeout=20.0):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = client.get(f"/api/rounds/{round_id}").json()
        if last["state"] in (SUCCEEDED, FAILED, CANCELLED):
            return last
        time.sleep(0.02)
    raise AssertionError(f"round did not finish: {last}")


def _submit(client, sql, station="ST001"):
    resp = client.post("/api/rounds", json={"station_id": station, "sql": sql})
    assert resp.status_code == 201, resp.text
    return resp.json()


# --------------------------------------------------------------- cache hits
def test_second_identical_submission_is_cache_hit_without_slot(client, sample_path):
    _upload(client, sample_path)
    first = _submit(client, "SELECT * FROM observations ORDER BY ts_utc")
    assert first["cache_hit"] is False
    assert first["shared_role"] == "initiator"
    final = _wait_terminal(client, first["round_id"])
    assert final["state"] == SUCCEEDED

    second = _submit(client, "SELECT * FROM observations ORDER BY ts_utc")
    assert second["state"] == SUCCEEDED
    assert second["cache_hit"] is True
    assert second["shared_role"] is None
    # Cache hit consumes no execution slot and no execution is running.
    assert client.get("/api/rounds").json()["active"] == 0
    body_a = client.get(f"/api/rounds/{first['round_id']}/result").content
    body_b = client.get(f"/api/rounds/{second['round_id']}/result").content
    assert body_a == body_b
    assert client.get("/api/health").json()["cache"]["entries"] == 1


def test_formatting_only_changes_share_the_cache_entry(client, sample_path):
    _upload(client, sample_path)
    first = _submit(client, "SELECT ts_utc, temperature_c FROM observations ORDER BY ts_utc")
    _wait_terminal(client, first["round_id"])

    # Same query with a comment and different whitespace/newlines.
    reformatted = (
        "SELECT   ts_utc,  temperature_c   -- same columns\n"
        "FROM observations\n"
        "ORDER BY ts_utc"
    )
    second = _submit(client, reformatted)
    assert second["cache_hit"] is True
    assert second["cache_key"] == first["cache_key"]


def test_cache_key_distinguishes_station_and_sql(client, sample_path):
    _upload(client, sample_path)
    a = _submit(client, "SELECT count(*) AS n FROM observations", station="ST001")
    _wait_terminal(client, a["round_id"])
    b = _submit(client, "SELECT count(*) AS n FROM observations", station="ST002")
    _wait_terminal(client, b["round_id"])
    c = _submit(client, "SELECT count(*) + 1 AS n FROM observations", station="ST001")
    _wait_terminal(client, c["round_id"])

    def identity(r):
        return (r["batch_id"], r["station_id"], r["cache_key"])

    assert len({identity(a), identity(b), identity(c)}) == 3
    # Same SQL on another station shares the SQL digest but not the key.
    assert a["cache_key"] == b["cache_key"]
    assert a["station_id"] != b["station_id"]
    # Distinct stations produce distinct executables and different data.
    n_a = client.get(f"/api/rounds/{a['round_id']}/result").text.splitlines()[1]
    n_b = client.get(f"/api/rounds/{b['round_id']}/result").text.splitlines()[1]
    assert n_a == "5" and n_b == "3"


def test_new_upload_invalidates_cache(client, sample_path, other_sample_path):
    _upload(client, sample_path)
    sql = "SELECT count(*) AS n FROM observations"
    first = _submit(client, sql)
    _wait_terminal(client, first["round_id"])
    hit = _submit(client, sql)
    assert hit["cache_hit"] is True

    upload = _upload(client, other_sample_path)
    assert upload["batch_id"] > first["batch_id"]
    assert upload["cache_invalidated"] == 1

    after = _submit(client, sql)
    assert after["cache_hit"] is False
    assert after["batch_id"] == upload["batch_id"]
    final = _wait_terminal(client, after["round_id"])
    assert final["state"] == SUCCEEDED
    # The new batch has 4 ST001 rows.
    rows = client.get(f"/api/rounds/{after['round_id']}/result").text.splitlines()
    assert rows[1] == "4"


def test_previous_batch_round_stays_downloadable_after_new_upload(
    client, sample_path, other_sample_path
):
    _upload(client, sample_path)
    first = _submit(client, "SELECT * FROM observations ORDER BY ts_utc")
    _wait_terminal(client, first["round_id"])
    before = client.get(f"/api/rounds/{first['round_id']}/result").content

    _upload(client, other_sample_path)
    # The old finished round is still a completed round and remains available.
    again = client.get(f"/api/rounds/{first['round_id']}/result")
    assert again.status_code == 200
    assert again.content == before


# ------------------------------------------------------- concurrent sharing
def test_concurrent_identical_submissions_run_once(client, sample_path):
    _upload(client, sample_path)
    n = 4
    results = []
    lock = threading.Lock()

    def worker():
        r = _submit(client, SLOW_SQL)
        with lock:
            results.append(r)

    threads = [threading.Thread(target=worker) for _ in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)

    assert len(results) == n
    states = {r["state"] for r in results}
    assert states <= {RUNNING, WAITING}
    initiators = [r for r in results if r["shared_role"] == "initiator"]
    waiters = [r for r in results if r["shared_role"] == "waiter"]
    assert len(initiators) == 1, results
    assert len(waiters) == n - 1, results
    # Only the real execution occupies a slot; all four rounds are accounted
    # for and exactly one execution was started.
    listing = client.get("/api/rounds").json()
    assert listing["active"] == 1
    assert listing["executions"]["executions_started"] == 1

    finals = [_wait_terminal(client, r["round_id"]) for r in results]
    assert all(f["state"] == SUCCEEDED for f in finals)
    bodies = {client.get(f"/api/rounds/{r['round_id']}/result").content for r in results}
    assert len(bodies) == 1
    # The shared success is stored once and reusable afterwards.
    assert client.get("/api/rounds").json()["cache"]["entries"] == 1
    later = _submit(client, SLOW_SQL)
    assert later["cache_hit"] is True


def test_waiting_round_can_cancel_without_affecting_shared_execution(
    client, sample_path
):
    _upload(client, sample_path)
    initiator = _submit(client, SLOW_SQL)
    waiter = _submit(client, SLOW_SQL)
    assert waiter["state"] == WAITING
    assert waiter["shared_role"] == "waiter"

    cancel = client.post(f"/api/rounds/{waiter['round_id']}/cancel")
    assert cancel.status_code == 200
    assert cancel.json()["accepted"] is True
    waiter_final = _wait_terminal(client, waiter["round_id"])
    assert waiter_final["state"] == CANCELLED
    assert client.get(f"/api/rounds/{waiter['round_id']}/result").status_code == 409

    # The shared execution still runs and succeeds for the initiator.
    init_final = _wait_terminal(client, initiator["round_id"])
    assert init_final["state"] == SUCCEEDED
    assert client.get(f"/api/rounds/{initiator['round_id']}/result").status_code == 200
    assert client.get("/api/rounds").json()["cache"]["entries"] == 1


def test_cancelling_all_participants_stops_shared_execution_and_no_cache(
    client, sample_path
):
    _upload(client, sample_path)
    initiator = _submit(client, INFINITE_SQL)
    waiter = _submit(client, INFINITE_SQL)
    assert waiter["state"] == WAITING

    assert client.post(f"/api/rounds/{waiter['round_id']}/cancel").status_code == 200
    assert client.post(f"/api/rounds/{initiator['round_id']}/cancel").status_code == 200
    assert _wait_terminal(client, initiator["round_id"])["state"] == CANCELLED
    assert _wait_terminal(client, waiter["round_id"])["state"] == CANCELLED
    # Slot released and the cancelled shared execution was not cached.
    active = 0
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        active = client.get("/api/rounds").json()["active"]
        if active == 0:
            break
        time.sleep(0.02)
    assert active == 0
    assert client.get("/api/rounds").json()["cache"]["entries"] == 0


# ----------------------------------------------------- failures not cached
def test_failed_shared_execution_not_cached_and_waiters_fail(client, sample_path):
    _upload(client, sample_path)
    # Valid to the guard but fails at runtime with a catalog error.
    bad_sql = "SELECT * FROM no_such_table LIMIT 1"
    a = _submit(client, bad_sql)
    b = _submit(client, bad_sql)
    # The second round either shares the execution or starts a new one after
    # the first failed immediately; either way both must fail.
    fa = _wait_terminal(client, a["round_id"])
    fb = _wait_terminal(client, b["round_id"])
    assert fa["state"] == FAILED and fb["state"] == FAILED
    assert client.get("/api/rounds").json()["cache"]["entries"] == 0
    # Retrying still executes instead of serving a cached failure.
    retry = _submit(client, bad_sql)
    assert retry["cache_hit"] is False
    _wait_terminal(client, retry["round_id"])


def test_timeout_not_cached(client, sample_path):
    _upload(client, sample_path)
    first = _submit(client, INFINITE_SQL)
    assert _wait_terminal(client, first["round_id"], timeout=20)["state"] == FAILED
    assert client.get("/api/rounds").json()["cache"]["entries"] == 0
    again = _submit(client, INFINITE_SQL)
    assert again["cache_hit"] is False
    # Clean up so the slot is released for the fixture teardown.
    client.post(f"/api/rounds/{again['round_id']}/cancel")
    _wait_terminal(client, again["round_id"], timeout=20)


def test_oversized_result_not_cached(client, sample_path):
    _upload(client, sample_path)
    huge_sql = (
        "WITH RECURSIVE seq(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM seq "
        "WHERE x < 200000) SELECT x, repeat('0123456789ABCDEF', 40) AS pad FROM seq"
    )
    first = _submit(client, huge_sql)
    assert _wait_terminal(client, first["round_id"], timeout=20)["state"] == FAILED
    assert client.get("/api/rounds").json()["cache"]["entries"] == 0
    again = _submit(client, huge_sql)
    assert again["cache_hit"] is False
    _wait_terminal(client, again["round_id"], timeout=20)


# ------------------------------------------------------------- cache bounds
def test_cache_keeps_at_most_eight_entries(client, sample_path):
    _upload(client, sample_path)
    for i in range(10):
        r = _submit(client, f"SELECT {i} AS v")
        _wait_terminal(client, r["round_id"])
    stats = client.get("/api/health").json()["cache"]
    assert stats["entries"] == 8
    assert stats["evictions"] >= 2
    # The most recent entry is still a hit, the oldest was evicted.
    recent = _submit(client, "SELECT 9 AS v")
    assert recent["cache_hit"] is True
    old = _submit(client, "SELECT 0 AS v")
    assert old["cache_hit"] is False
    _wait_terminal(client, old["round_id"])


def test_cache_respects_total_byte_limit(client, sample_path):
    _upload(client, sample_path)
    # Every cached row is ~3 MiB; 16 MiB total means at most 5 entries even
    # though the entry limit allows 8.
    sql_template = (
        "SELECT '{tag}' AS tag, repeat('x', 3000000) AS pad"
    )
    for i in range(6):
        r = _submit(client, sql_template.format(tag=f"t{i}"))
        _wait_terminal(client, r["round_id"], timeout=20)
    stats = client.get("/api/health").json()["cache"]
    assert stats["entries"] <= 5
    assert stats["bytes"] <= MAX_CACHE_BYTES
    assert stats["evictions"] >= 1


def test_cache_hit_round_can_still_download_after_eviction(client, sample_path):
    _upload(client, sample_path)
    sql = "SELECT 42 AS v"
    first = _submit(client, sql)
    _wait_terminal(client, first["round_id"])
    body = client.get(f"/api/rounds/{first['round_id']}/result").content
    # Evict it by filling the cache with other results.
    for i in range(9):
        r = _submit(client, f"SELECT {100 + i} AS v")
        _wait_terminal(client, r["round_id"])
    assert client.get(f"/api/rounds/{first['round_id']}/result").content == body


def test_cache_hit_is_reported_in_round_status(client, sample_path):
    _upload(client, sample_path)
    sql = "SELECT 7 AS v"
    first = _submit(client, sql)
    _wait_terminal(client, first["round_id"])
    cached = _submit(client, sql)
    status = client.get(f"/api/rounds/{cached['round_id']}").json()
    assert status["cache_hit"] is True
    assert status["state"] == SUCCEEDED
    listing = client.get("/api/rounds").json()
    flags = {r["round_id"]: r["cache_hit"] for r in listing["rounds"]}
    assert flags[cached["round_id"]] is True
    assert flags[first["round_id"]] is False


# ------------------------------------------------- batch change during run
def test_inflight_execution_result_cannot_poison_new_batch(client, sample_path):
    """A query finishing after a new upload must not enter the new batch's cache."""
    _upload(client, sample_path)
    old_sql = "SELECT count(*) AS n FROM observations"
    initiator = _submit(client, SLOW_SQL)
    waiter = _submit(client, SLOW_SQL)
    assert waiter["state"] == WAITING

    # New upload while the shared execution is still running.
    upload = _upload(client, sample_path)  # same file, new batch id
    assert upload["batch_id"] == initiator["batch_id"] + 1

    for r in (initiator, waiter):
        final = _wait_terminal(client, r["round_id"])
        assert final["state"] == SUCCEEDED  # their own results still published
        assert client.get(f"/api/rounds/{r['round_id']}/result").status_code == 200
    # Nothing of the old batch leaked into the new batch cache.
    assert client.get("/api/rounds").json()["cache"]["entries"] == 0
    fresh = _submit(client, old_sql)
    assert fresh["cache_hit"] is False
    assert fresh["batch_id"] == upload["batch_id"]


def test_waiting_round_cancel_message_mentions_shared_execution(client, sample_path):
    _upload(client, sample_path)
    initiator = _submit(client, SLOW_SQL)
    waiter = _submit(client, SLOW_SQL)
    resp = client.post(f"/api/rounds/{waiter['round_id']}/cancel")
    assert resp.status_code == 200
    assert "shared execution continues" in resp.json()["message"]
    assert _wait_terminal(client, initiator["round_id"])["state"] == SUCCEEDED
    # Already-cancelled waiter cannot be cancelled twice (terminal state).
    again = client.post(f"/api/rounds/{waiter['round_id']}/cancel")
    assert again.status_code == 409


def test_ui_exposes_cache_and_shared_flags(client):
    page = client.get("/")
    assert page.status_code == 200
    assert "命中" in page.text
    assert "cache_hit" in page.text
    assert "shared_role" in page.text
    assert "结果缓存" in page.text


def test_finished_round_retention_is_bounded_by_bytes(client, sample_path):
    """Many large successful rounds cannot grow memory without limit."""
    from app.config import MAX_RETAINED_RESULT_BYTES
    from app.main import registry

    _upload(client, sample_path)
    for i in range(6):
        r = _submit(client, f"SELECT '{i}' AS tag, repeat('y', 3000000) AS pad")
        _wait_terminal(client, r["round_id"], timeout=20)
    total = sum(
        len(x.result_csv or b"") for x in registry._rounds.values()
    )
    assert total <= MAX_RETAINED_RESULT_BYTES
    # The most recent round is still downloadable.
    latest = max(registry._rounds.values(), key=lambda x: x.finished_at)
    assert client.get(f"/api/rounds/{latest.round_id}/result").status_code == 200
