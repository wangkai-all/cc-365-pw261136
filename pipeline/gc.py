#!/usr/bin/env python3
"""孤立文件回收（独立进程，一次性运行）。

安全准则：
- 只删"所有目录版本都不引用"的文件（回退可能复活旧版本，必须看全集，不能只看 head）；
- 年龄小于宽限期的文件跳过（写入器可能刚落盘、还没来得及提交）；
- 绝不碰仍被任一版本引用的文件。
"""
import argparse
import json
import os
import time

import common


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", required=True)
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--grace", type=float, default=30.0, help="宽限期（秒）")
    args = ap.parse_args()

    referenced = set(common.http_json(
        "GET", args.catalog.rstrip("/") + "/api/referenced", timeout=5)["paths"])
    objects = os.path.join(args.data, "objects")
    now = time.time()
    report = {"grace": args.grace, "deleted": [], "skipped_young": [], "kept_referenced": 0}
    if os.path.isdir(objects):
        for name in sorted(os.listdir(objects)):
            full = os.path.join(objects, name)
            if not os.path.isfile(full):
                continue
            rel = f"objects/{name}"
            if rel in referenced:
                report["kept_referenced"] += 1
                continue
            age = now - os.path.getmtime(full)
            if age < args.grace:
                report["skipped_young"].append({"path": rel, "age": round(age, 1)})
                continue
            size = os.path.getsize(full)
            os.remove(full)
            report["deleted"].append({"path": rel, "age": round(age, 1), "bytes": size})
    print(json.dumps(report, ensure_ascii=False), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
