"""HTTP 入口：提交/查询/取消/死信/重放，错误响应归一化、原因可区分。"""

from __future__ import annotations

import importlib
import time

import pytest

import main as main_module


@pytest.fixture
def client(tmp_path, monkeypatch):
    db = str(tmp_path / "api.db")
    monkeypatch.setenv("JOBQUEUE_DB", db)
    monkeypatch.setenv("JOBQUEUE_CAPACITY", "2")
    monkeypatch.setenv("JOBQUEUE_CONCURRENCY", "2")

    async def echo(payload: str) -> dict:
        return {"echoed": payload}

    async def boom(payload: str) -> None:
        raise RuntimeError("http-boom")

    importlib.reload(main_module)  # 先 reload（重建模块级注册表），再注册 handler

    async def _echo(payload: str) -> dict:
        return {"echoed": payload}

    async def _boom(payload: str) -> None:
        raise RuntimeError("http-boom")

    main_module.TASK_HANDLERS.update({"echo": _echo, "boom": _boom})

    from fastapi.testclient import TestClient

    with TestClient(main_module.app) as c:
        yield c


def _wait_status(c, tid, status, tries=200):
    for _ in range(tries):
        r = c.get(f"/tasks/{tid}")
        if r.json()["status"] == status:
            return r.json()
        time.sleep(0.01)
    raise AssertionError(f"task did not reach {status}")


def test_submit_get_and_execute(client):
    r = client.post("/tasks", json={"task_type": "echo", "payload": "hi"})
    assert r.status_code == 201
    assert r.json()["status"] == "PENDING"

    r2 = client.post("/tasks", json={"task_type": "echo", "payload": "hi",
                                     "idempotency_key": "abc"})
    assert r2.status_code == 201
    r3 = client.post("/tasks", json={"task_type": "echo", "payload": "hi",
                                     "idempotency_key": "abc"})
    assert r3.json()["id"] == r2.json()["id"]

    final = _wait_status(client, r2.json()["id"], "SUCCEEDED")
    assert final["result"] == '{"echoed": "hi"}'


def test_idempotency_conflict_409_distinct(client):
    client.post("/tasks", json={"task_type": "echo", "payload": "a",
                                "idempotency_key": "k"})
    r = client.post("/tasks", json={"task_type": "echo", "payload": "b",
                                    "idempotency_key": "k"})
    assert r.status_code == 409
    err = r.json()["error"]
    assert err["reason"] == "IDEMPOTENCY_PAYLOAD_MISMATCH"
    assert err["task_id"]


def test_not_found_404(client):
    r = client.get("/tasks/nope")
    assert r.status_code == 404
    assert r.json()["error"]["reason"] == "TASK_NOT_FOUND"
    r = client.post("/tasks/nope/cancel")
    assert r.status_code == 404


def test_unknown_type_400(client):
    r = client.post("/tasks", json={"task_type": "ghost", "payload": ""})
    assert r.status_code == 400
    assert r.json()["error"]["reason"] == "UNKNOWN_TASK_TYPE"


def test_backpressure_503_distinct_reasons(tmp_path, monkeypatch):
    import asyncio as _aio

    monkeypatch.setenv("JOBQUEUE_DB", str(tmp_path / "bp.db"))
    monkeypatch.setenv("JOBQUEUE_CAPACITY", "1")
    monkeypatch.setenv("JOBQUEUE_CONCURRENCY", "1")
    importlib.reload(main_module)
    gate = _aio.Event()

    async def blocking_echo(payload: str) -> None:
        # 占据唯一 worker，让后续任务停留在 PENDING 以填满积压名额
        gate.set()
        await _aio.sleep(0.5)

    main_module.TASK_HANDLERS.update({"echo": blocking_echo})
    from fastapi.testclient import TestClient

    with TestClient(main_module.app) as c:
        # 第一条被 worker 领取（占住 worker），第二条占满 capacity=1 的 PENDING
        assert c.post("/tasks", json={"task_type": "echo",
                                      "payload": "occupying"}).status_code == 201
        for _ in range(50):
            if gate.is_set():
                break
            time.sleep(0.01)
        assert gate.is_set()
        assert c.post("/tasks", json={"task_type": "echo",
                                      "payload": "queued"}).status_code == 201
        r = c.post("/tasks", json={"task_type": "echo", "payload": "overflow"})
        assert r.status_code == 503
        assert r.json()["error"]["reason"] == "QUEUE_FULL_REJECT"
        assert r.json()["error"]["backlog"] == 1


def test_cancel_semantics_endpoint(client):
    # 排队取消（任务可能很快被 worker 领取，多提交制造窗口，主要断言响应结构合法）
    r = client.post("/tasks", json={"task_type": "echo", "payload": "z"})
    tid = r.json()["id"]
    cr = client.post(f"/tasks/{tid}/cancel")
    assert cr.status_code == 200
    body = cr.json()
    assert body["task_id"] == tid
    assert body["semantics"] in {
        "cancelled_while_queued", "cancel_requested", "already_settled"
    }
    assert body["status"] in {"CANCELLED", "CANCELLING", "SUCCEEDED", "DEAD"}
    # 重复取消不报错
    cr2 = client.post(f"/tasks/{tid}/cancel")
    assert cr2.status_code == 200


def test_dead_letter_list_and_replay_preserves_reason(client):
    r = client.post("/tasks", json={"task_type": "boom", "payload": "x",
                                    "max_attempts": 1})
    tid = r.json()["id"]
    final = _wait_status(client, tid, "DEAD")
    assert final["failures"][0]["kind"] == "HANDLER_ERROR"
    assert "http-boom" in final["failures"][0]["reason"]

    dl = client.get("/dead-letter")
    assert dl.status_code == 200
    assert any(t["id"] == tid for t in dl.json()["tasks"])

    # 重放：handler 仍失败 -> 再进 DEAD；但历史保留
    rp = client.post(f"/tasks/{tid}/replay")
    assert rp.status_code == 200
    assert rp.json()["preserved_failures"] == 1
    final2 = _wait_status(client, tid, "DEAD")
    assert len(final2["failures"]) == 2

    # 再次重放非死信期间，刚启动任务可能 PENDING；重放仅允许 DEAD
    # 等它再次 DEAD 后重放，验证幂等接口路径稳定
    assert client.post(f"/tasks/{tid}/replay").status_code == 200
