#!/usr/bin/env python3
"""端到端冒烟测试：只通过控制台 HTTP API 驱动（与网页操作等价），覆盖全部故障场景。

用法：先启动控制台（python3 control.py），再运行 python3 smoke_test.py
"""
import json
import os
import sqlite3
import subprocess
import sys
import time
import urllib.request

BASE = "http://127.0.0.1:8620"
DATA = "/workspace/pipeline_data"
PY = sys.executable

results = []


def api(action, **params):
    body = json.dumps({"action": action, **params}).encode()
    req = urllib.request.Request(BASE + "/api/action", data=body,
                                 headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read())


def state():
    with urllib.request.urlopen(BASE + "/api/state", timeout=10) as r:
        return json.loads(r.read())


def wait_for(pred, desc, timeout=20):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            s = state()
            if pred(s):
                return s
        except Exception:
            pass
        time.sleep(0.4)
    raise TimeoutError(desc)


def ensure_up():
    s = state()
    if not s["procs"]["catalog"]["alive"]:
        api("start_catalog")
    if not s["procs"]["consumer"]["alive"]:
        api("start_consumer")
    wait_for(lambda s: s["procs"]["catalog"]["alive"] and s["procs"]["consumer"]["alive"],
             "目录与消费者存活")


def db_checkpoint():
    """消费者进程死掉时，直接读它的 SQLite 看回执（checkpoint）。"""
    con = sqlite3.connect(os.path.join(DATA, "consumer.db"))
    try:
        r = con.execute("SELECT v FROM meta WHERE k='checkpoint'").fetchone()
        return int(r[0]) if r else 0
    finally:
        con.close()


def verify():
    return subprocess.run(
        [PY, "verify.py", "--data", DATA,
         "--catalog", "http://127.0.0.1:8621", "--consumer", "http://127.0.0.1:8622"],
        cwd="/workspace/pipeline", capture_output=True, text=True, timeout=30)


def check(name, fn):
    try:
        ensure_up()
        fn()
        results.append(("PASS", name))
        print(f"[PASS] {name}", flush=True)
    except Exception as e:
        results.append(("FAIL", name))
        print(f"[FAIL] {name}: {e}", flush=True)


def consumer_ckpt(s):
    return s["consumer"]["status"]["checkpoint"]


def head(s):
    return s["catalog"]["head"]["version"]


def t01_reset():
    r = api("reset")
    assert r["ok"], r
    wait_for(lambda s: s["procs"]["catalog"]["alive"] and s["procs"]["consumer"]["alive"],
             "重启后进程存活")


def t02_commit_and_consume():
    for _ in range(3):
        r = api("commit")
        assert r["ok"], r
    wait_for(lambda s: consumer_ckpt(s) == head(s) == 3, "消费者追平 v3")
    v = verify()
    assert v.returncode == 0, v.stdout + v.stderr


def t03_kill_consumer_no_dup():
    api("stop_consumer")
    wait_for(lambda s: not s["procs"]["consumer"]["alive"], "消费者已停止")
    r = api("commit")
    assert r["ok"], r
    time.sleep(1.5)  # 消费者不在，回执（checkpoint）必须原地不动
    assert db_checkpoint() == 3, f"消费者已停，checkpoint 不应前进: {db_checkpoint()}"
    api("start_consumer")
    wait_for(lambda s: consumer_ckpt(s) == head(s) == 4, "重启后追平 v4")
    v = verify()
    assert v.returncode == 0, v.stdout + v.stderr


def t04_crash_before_commit():
    r = api("consumer_fault", mode="crash_before_commit")
    assert r["ok"], r
    r = api("commit")
    assert r["ok"], r
    wait_for(lambda s: not s["procs"]["consumer"]["alive"], "消费者应已崩溃")
    assert db_checkpoint() == 4, "崩溃在提交前：回执必须还是旧值"
    api("start_consumer")
    wait_for(lambda s: consumer_ckpt(s) == head(s) == 5, "崩溃恢复后追平 v5")
    time.sleep(2)  # 多等几轮，确认重放没有重复计入
    v = verify()
    assert v.returncode == 0, v.stdout + v.stderr


def t05_crash_after_commit_receipt_lost():
    r = api("consumer_fault", mode="crash_after_commit")
    assert r["ok"], r
    r = api("commit")
    assert r["ok"], r
    wait_for(lambda s: not s["procs"]["consumer"]["alive"], "消费者应已崩溃")
    api("start_consumer")
    wait_for(lambda s: consumer_ckpt(s) == head(s) == 6, "恢复后 checkpoint 已是 v6")
    s = state()  # 回执丢失：目录侧 receipt 仍停在 v5，但汇总不重复
    rc = {x["name"]: x["version"] for x in s["catalog"]["receipts"]["receipts"]}
    assert rc.get("aggregator") == 5, f"目录侧回执应滞后（丢失 v6 上报）: {rc}"
    v = verify()
    assert v.returncode == 0, v.stdout + v.stderr


def t06_writer_crash_orphans_and_gc():
    r = api("writer_crash")
    assert r["ok"], r
    wait_for(lambda s: len(s["orphans"].get("files") or []) >= 2, "出现≥2个孤儿文件")
    r = api("gc", grace=0)
    assert r["ok"], r
    s = state()
    assert not s["orphans"]["files"], f"GC 后仍有孤儿: {s['orphans']}"
    v = verify()  # 被目录引用的文件一个都不能少
    assert v.returncode == 0, v.stdout + v.stderr


def t07_commit_response_lost_dedup():
    s0 = state()
    v0 = head(s0)
    r = api("commit", drop_response=True)
    assert r["ok"], r
    assert "去重" in r["detail"], f"应发生目录侧幂等去重: {r['detail']}"
    s1 = wait_for(lambda s: head(s) == v0 + 1, "只应新增一个版本")
    assert len(s1["catalog"]["versions"]["versions"]) == \
        len(s0["catalog"]["versions"]["versions"]) + 1, "响应丢失不得产生重复版本"
    wait_for(lambda s: consumer_ckpt(s) == head(s), "消费者追平")
    v = verify()
    assert v.returncode == 0, v.stdout + v.stderr


def t08_rollback_reconsume():
    s = state()
    target = head(s) - 2
    r = api("rollback", version=target)
    assert r["ok"], r
    wait_for(lambda s: consumer_ckpt(s) == target, f"消费者回退到 v{target}")
    v = verify()
    assert v.returncode == 0, "回退后: " + v.stdout + v.stderr
    r = api("commit")  # 回退后继续提交新版本
    assert r["ok"], r
    wait_for(lambda s: consumer_ckpt(s) == head(s), "回退后新提交被消费")
    v = verify()
    assert v.returncode == 0, "回退后新提交: " + v.stdout + v.stderr


def t09_delete_partition():
    s = state()
    pkey = s["catalog"]["manifest"]["files"][0]["pkey"]
    r = api("delete_partition", pkey=pkey)
    assert r["ok"], r
    wait_for(lambda s: consumer_ckpt(s) == head(s), "下架被消费")
    s = state()
    keys = [d["pkey"] for d in s["consumer"]["details"]]
    assert pkey not in keys, f"{pkey} 应从明细中消失"
    v = verify()
    assert v.returncode == 0, v.stdout + v.stderr


def t10_catalog_down_retry():
    api("stop_catalog")
    wait_for(lambda s: not s["procs"]["catalog"]["alive"], "目录已停止")
    r = api("commit")
    assert not r["ok"], "目录停止时提交应失败"
    api("start_catalog")
    wait_for(lambda s: s["procs"]["catalog"]["alive"], "目录已重启")
    r = api("commit")  # 同一批次号重试，应成功且不产生重复版本
    assert r["ok"], r
    wait_for(lambda s: consumer_ckpt(s) == head(s), "消费者追平")
    v = verify()
    assert v.returncode == 0, v.stdout + v.stderr


def main():
    steps = [
        ("01 重置环境", t01_reset),
        ("02 连续提交并消费", t02_commit_and_consume),
        ("03 断开消费者→提交→重启：不重复计入", t03_kill_consumer_no_dup),
        ("04 消费者提交前崩溃（回执丢失）→重放幂等", t04_crash_before_commit),
        ("05 消费者提交后崩溃（回执未上报）→不重复", t05_crash_after_commit_receipt_lost),
        ("06 写入器崩溃产生孤儿→GC 只删孤儿", t06_writer_crash_orphans_and_gc),
        ("07 提交响应丢失→写入器重试被目录幂等去重", t07_commit_response_lost_dedup),
        ("08 回退版本→重新消费→继续提交", t08_rollback_reconsume),
        ("09 下架分区→明细与汇总同步移除", t09_delete_partition),
        ("10 目录停机→提交失败→恢复后同批次重试成功", t10_catalog_down_retry),
    ]
    for name, fn in steps:
        check(name, fn)
    failed = [r for r in results if r[0] == "FAIL"]
    print(f"\n===== {len(results)-len(failed)}/{len(results)} 通过 =====")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
