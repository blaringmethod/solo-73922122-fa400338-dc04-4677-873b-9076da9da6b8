"""Concurrency-policy tests for admission control with shared executions."""
from __future__ import annotations

import time

from app.registry import CANCELLED, FAILED, SUCCEEDED, WAITING


INFINITE_A = (
    "WITH RECURSIVE seq(x) AS (SELECT 11 UNION ALL SELECT x+1 FROM seq) "
    "SELECT count(*) FROM seq"
)
INFINITE_B = (
    "WITH RECURSIVE seq(x) AS (SELECT 12 UNION ALL SELECT x+1 FROM seq) "
    "SELECT count(*) FROM seq"
)


def _submit(client, sql):
    return client.post("/api/rounds", json={"station_id": "ST001", "sql": sql})


def _wait(client, rid, timeout=20.0):
    end = time.monotonic() + timeout
    last = None
    while time.monotonic() < end:
        last = client.get(f"/api/rounds/{rid}").json()
        if last["state"] in (SUCCEEDED, FAILED, CANCELLED):
            return last
        time.sleep(0.02)
    raise AssertionError(last)


def _upload(client, sample_path):
    with open(sample_path, "rb") as fh:
        resp = client.post(
            "/api/upload", files={"file": ("obs.csv", fh, "text/csv")}
        )
    assert resp.status_code == 200


def test_waiter_is_accepted_even_when_all_slots_are_full(client, sample_path):
    _upload(client, sample_path)
    first = _submit(client, INFINITE_A)
    second = _submit(client, INFINITE_B)
    assert first.status_code == 201 and second.status_code == 201
    assert client.get("/api/rounds").json()["active"] == 2

    # A new distinct key needs a third slot -> rejected.
    third = _submit(client, "SELECT 1 AS v")
    assert third.status_code == 503
    assert "2 rounds running" in third.json()["detail"]

    # The exact same key as a running execution only waits -> accepted.
    waiter = _submit(client, INFINITE_A)
    assert waiter.status_code == 201
    assert waiter.json()["state"] == WAITING
    assert waiter.json()["shared_role"] == "waiter"
    # Still only two real executions occupy slots.
    assert client.get("/api/rounds").json()["active"] == 2

    for rid in (first.json()["round_id"], second.json()["round_id"]):
        client.post(f"/api/rounds/{rid}/cancel")
    _wait(client, first.json()["round_id"])
    _wait(client, second.json()["round_id"])
    assert _wait(client, waiter.json()["round_id"])["state"] == FAILED  # timed out


def test_cancelling_initiator_keeps_execution_for_waiting_round(client, sample_path):
    _upload(client, sample_path)
    sql = (
        "WITH RECURSIVE seq(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM seq "
        "WHERE x < 150000) SELECT count(*) AS n FROM seq"
    )
    initiator = _submit(client, sql).json()
    waiter = _submit(client, sql).json()
    assert waiter["state"] == WAITING

    resp = client.post(f"/api/rounds/{initiator['round_id']}/cancel")
    assert resp.status_code == 200
    assert "shared execution continues" in resp.json()["message"]

    assert _wait(client, initiator["round_id"])["state"] == CANCELLED
    final = _wait(client, waiter["round_id"])
    assert final["state"] == SUCCEEDED
    assert client.get(f"/api/rounds/{waiter['round_id']}/result").status_code == 200
    # The shared success is cached once.
    assert client.get("/api/rounds").json()["cache"]["entries"] == 1
