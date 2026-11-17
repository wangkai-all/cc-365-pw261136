#!/usr/bin/env python3
"""清洗写入器：每次运行 = 一个批次（独立进程，可崩溃、可安全重试）。

流程：生成原始记录 -> 清洗（过滤非法/去重）-> 分区文件原子落盘 -> 提交目录。
- 文件先落盘、目录后提交：未提交的文件不在任何清单里，消费者不可见；
- batch_id 由批次号决定：崩溃后重跑同一批次，目录幂等去重；
- 每第 3 批会"重洗"前一天的分区（同键不同内容），模拟订正数据覆盖。
"""
import argparse
import json
import os
import random
import sys
import time

import common

TYPES = ["view", "click", "purchase"]


def gen_partition(run, day, part, seed):
    rng = random.Random(seed)
    pkey = f"dt=2026-11-{day:02d}/part-{part}"
    raw = []
    for j in range(rng.randint(15, 40)):
        t = rng.choices(TYPES, weights=[6, 3, 1])[0]
        rec = {
            "id": f"{pkey}-{j}",
            "user": f"u{rng.randint(1, 500)}",
            "type": t,
            "amount_cents": rng.randint(100, 50000) if t == "purchase" else 0,
            "ts": int(time.time()) - rng.randint(0, 86400),
        }
        r = rng.random()  # 注入脏数据：缺用户 / 负金额 / 非法类型 / 重复 id
        if r < 0.03:
            rec = dict(rec, user=None)
        elif r < 0.05:
            rec = dict(rec, amount_cents=-5)
        elif r < 0.07:
            rec = dict(rec, type="???")
        elif r < 0.09 and raw:
            rec = dict(rec, id=raw[-1]["id"])
        raw.append(rec)
    seen, clean, dropped = set(), [], 0
    for rec in raw:  # 清洗：过滤非法记录 + 按 id 去重
        ok = (
            rec.get("user")
            and rec.get("type") in TYPES
            and rec.get("amount_cents", 0) >= 0
            and rec["id"] not in seen
        )
        if ok:
            seen.add(rec["id"])
            clean.append(rec)
        else:
            dropped += 1
    payload = {"pkey": pkey, "run": run, "created_at": time.time(), "records": clean}
    return pkey, payload, dropped


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", required=True)
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--run", type=int, required=True)
    ap.add_argument("--partitions", type=int, default=2)
    ap.add_argument("--crash", choices=["after_write"], default=None)
    args = ap.parse_args()

    batch_id = f"run-{args.run:04d}"
    days = [args.run]
    if args.run % 3 == 0 and args.run > 1:
        days.append(args.run - 1)  # 订正重洗前一天（同分区键、新内容）

    adds, staged = [], []
    for day in days:
        for part in range(args.partitions):
            seed = 20261117 + day * 1000 + part + (7 if day != args.run else 0)
            pkey, payload, dropped = gen_partition(args.run, day, part, seed)
            body = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode()
            sha = common.sha256_bytes(body)
            rel = f"objects/{sha}.json"
            common.atomic_write(common.safe_join(args.data, rel), body)
            staged.append(rel)
            adds.append({
                "pkey": pkey, "path": rel, "sha256": sha,
                "rows": len(payload["records"]), "bytes": len(body),
                "meta": {"run": args.run, "dropped": dropped},
            })

    if args.crash == "after_write":
        print(json.dumps({"status": "crashed", "stage": "after_write", "staged": staged},
                         ensure_ascii=False), flush=True)
        os._exit(3)  # 模拟猝死：文件已落盘，目录未提交

    payload = {"batch_id": batch_id, "message": f"清洗批次 {batch_id}",
               "adds": adds, "deletes": []}
    url = args.catalog.rstrip("/") + "/api/commit"
    last_err = None
    for attempt in range(1, 9):
        try:
            resp = common.http_json("POST", url, payload, timeout=3.0)
            print(json.dumps({"status": "committed", "batch_id": batch_id, **resp},
                             ensure_ascii=False), flush=True)
            return 0
        except common.HttpError as e:
            print(json.dumps({"status": "rejected", "error": str(e)}, ensure_ascii=False),
                  flush=True)
            return 1
        except OSError as e:  # 连接断开/超时等瞬时错误：重试，目录按 batch_id 幂等
            last_err = str(e)
            time.sleep(min(0.4 * attempt, 2.0))
    print(json.dumps({"status": "commit_failed", "batch_id": batch_id, "error": last_err},
                     ensure_ascii=False), flush=True)
    return 1


if __name__ == "__main__":
    sys.exit(main())
