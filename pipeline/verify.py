#!/usr/bin/env python3
"""独立校验器：按目录 head 清单 + 磁盘文件重算期望汇总，与消费者实际汇总对比。

校验点：checkpoint == head；明细集合 == head 清单；汇总 == 明细之和。
退出码 0 = 一致，1 = 不一致。
"""
import argparse
import json
import sys
from collections import Counter

import common


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", required=True)
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--consumer", required=True)
    args = ap.parse_args()
    cat = args.catalog.rstrip("/")
    cons = args.consumer.rstrip("/")

    head = common.http_json("GET", cat + "/api/head", timeout=3)["version"]
    manifest = common.http_json("GET", f"{cat}/api/manifest?version={head}", timeout=3)["files"]

    exp_details = {}
    types = Counter()
    for f in manifest:
        full = common.safe_join(args.data, f["path"])
        with open(full, "rb") as fh:
            body = fh.read()
        assert common.sha256_bytes(body) == f["sha256"], f"sha256 不匹配: {f['path']}"
        payload = json.loads(body)
        t = Counter(r["type"] for r in payload["records"])
        types.update(t)
        exp_details[f["pkey"]] = {
            "rows": len(payload["records"]),
            "amount_cents": sum(r["amount_cents"] for r in payload["records"]),
            "types": dict(t),
        }

    summary = common.http_json("GET", cons + "/api/summary", timeout=3)
    details = {d["pkey"]: d for d in
               common.http_json("GET", cons + "/api/details", timeout=3)["details"]}

    problems = []
    if summary["checkpoint"] != head:
        problems.append(f"checkpoint={summary['checkpoint']} != head={head}")
    if set(details) != set(exp_details):
        problems.append(f"明细键集不一致: 多{sorted(set(details)-set(exp_details))} "
                        f"少{sorted(set(exp_details)-set(details))}")
    for k in set(details) & set(exp_details):
        e, a = exp_details[k], details[k]
        if e["rows"] != a["rows"] or e["amount_cents"] != a["amount_cents"] or e["types"] != a["types"]:
            problems.append(f"明细不一致 {k}: 期望{e} 实际{{'rows':{a['rows']},...}}")
    exp = {"partitions": len(exp_details),
           "rows": sum(d["rows"] for d in exp_details.values()),
           "amount_cents": sum(d["amount_cents"] for d in exp_details.values()),
           "types": dict(types)}
    for k in ("partitions", "rows", "amount_cents", "types"):
        if summary[k] != exp[k]:
            problems.append(f"汇总不一致 {k}: 期望{exp[k]} 实际{summary[k]}")

    if problems:
        print("FAIL")
        for p in problems:
            print("  -", p)
        return 1
    print(f"PASS: head=v{head} 分区={exp['partitions']} 行={exp['rows']} "
          f"金额={exp['amount_cents']/100:.2f}元 分类={exp['types']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
