"""真·跨进程恢复：子进程领取任务后被 kill -9，新进程启动时恢复且不重复执行。"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
import time

WORKER_SCRIPT = textwrap.dedent('''
    import asyncio, json, sys
    from jobqueue.queue import JobQueue, QueueOptions
    from jobqueue.storage import SQLiteStorage

    DB = sys.argv[1]
    MODE = sys.argv[2]

    async def main():
        q = JobQueue(SQLiteStorage(DB),
                     options=QueueOptions(max_attempts=3))
        await q.initialize()
        if MODE == "submit":
            rec = await q.submit("work", "subprocess-payload",
                                 idempotency_key="sp-key")
            print(json.dumps({"id": rec.id}), flush=True)
        elif MODE == "claim":
            rec = await q.claim("doomed-runner")
            print(json.dumps({"id": rec.id, "status": rec.status}), flush=True)
            # 模拟执行中：保持进程存活，直到被外部 kill
            await asyncio.sleep(30)
        elif MODE == "resume":
            executions = []

            async def handler(payload):
                executions.append(payload)
                # 留痕：通过外部文件证明只执行一次
                marker = sys.argv[3]
                with open(marker, "a") as f:
                    f.write("executed\\n")
                return "resumed-ok"

            from jobqueue.worker import WorkerPool
            pool = WorkerPool(q, {"work": handler}, concurrency=1,
                              poll_interval=0.02)
            await pool.start()
            deadline = asyncio.get_running_loop().time() + 5
            while asyncio.get_running_loop().time() < deadline:
                rec = await q.get_task(sys.argv[3] and json.loads(
                    open(sys.argv[3] + ".tid").read())["id"])
                if rec.status.value in ("SUCCEEDED", "DEAD", "CANCELLED"):
                    print(json.dumps({"final": rec.status.value,
                                      "attempts": rec.attempts,
                                      "result": rec.result}), flush=True)
                    await pool.stop()
                    await q.aclose()
                    return
                await asyncio.sleep(0.02)
            raise SystemExit("task did not settle in time")
        await q.aclose()

    asyncio.run(main())
''')


def _run(db_path: str, mode: str, *extra: str, wait_ready: bool = False):
    cmd = [sys.executable, "-c", WORKER_SCRIPT, db_path, mode, *extra]
    env = dict(os.environ, PYTHONPATH=os.getcwd())
    return subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )


def _readline(proc, timeout=5.0):
    import select

    end = time.time() + timeout
    while time.time() < end:
        r, _, _ = select.select([proc.stdout], [], [], end - time.time())
        if r:
            line = proc.stdout.readline()
            if line:
                return json.loads(line)
    raise AssertionError("subprocess did not emit expected line")


def test_killed_process_running_task_is_recovered_once(tmp_path):
    db = str(tmp_path / "sp.db")

    # 进程 A：提交任务
    p_submit = _run(db, "submit")
    out, err = p_submit.communicate(timeout=10)
    assert p_submit.returncode == 0, err
    submitted = json.loads(out.strip().splitlines()[-1])

    # 进程 B：领取任务后在"执行中"被强杀（SIGKILL，无清理机会）
    p_claim = _run(db, "claim")
    claimed = _readline(p_claim)
    assert claimed["id"] == submitted["id"] and claimed["status"] == "RUNNING"
    time.sleep(0.3)
    p_claim.kill()
    p_claim.wait(timeout=5)
    assert p_claim.returncode != 0  # 确认非正常退出

    # 进程 C：全新进程恢复并执行；用 marker 文件计数执行次数
    marker = str(tmp_path / "marker")
    tid_file = tmp_path / "marker.tid"
    tid_file.write_text(json.dumps({"id": submitted["id"]}))

    p_resume = _run(db, "resume", marker)
    try:
        final = _readline(p_resume, timeout=10)
    finally:
        p_resume.wait(timeout=10)
    assert p_resume.returncode == 0, p_resume.stderr.read()

    assert final["final"] == "SUCCEEDED", final
    assert final["result"] == "resumed-ok"
    # attempts 计数（每次领取+1）：崩溃前领取 1 + 恢复重排 1 + 恢复后领取 1 = 3
    assert final["attempts"] == 3, final

    # 关键不变量：真实处理器执行留痕恰有一行 —— 恢复没有导致重复执行
    with open(marker) as f:
        lines = [l for l in f.read().splitlines() if l.strip()]
    assert lines == ["executed"], lines
