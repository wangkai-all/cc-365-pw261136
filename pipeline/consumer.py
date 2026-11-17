#!/usr/bin/env python3
"""消费汇总器（独立进程）：按目录版本增量消费，本地 SQLite 原子落账。

去重放在消费端（设计选择）：
1) 明细按分区键幂等 upsert —— 同一分区重复应用，结果完全相同；
2) 汇总更新与"回执"(checkpoint)在同一个 SQLite 事务里提交 ——
   崩溃只会两者一起回滚，恢复后重放依然幂等，绝不会把同一分区重复计入汇总；
3) 上报给目录服务的 receipt 仅用于展示消费滞后，丢失不影响正确性；
4) 回退版本后：diff 是集合运算（from>to 也成立），消失的分区从明细中删除，
   重新出现的分区重新计入 —— 汇总始终等于"当前 head 清单"的物化视图。
"""
import argparse
import json
import os
import sqlite3
import threading
import time
import traceback
from collections import Counter
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

import common

SCHEMA = """
CREATE TABLE IF NOT EXISTS details(
  pkey TEXT PRIMARY KEY,
  sha256 TEXT NOT NULL,
  rows INTEGER NOT NULL,
  amount_cents INTEGER NOT NULL,
  types TEXT NOT NULL,
  version INTEGER NOT NULL,
  updated_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS summary(
  id INTEGER PRIMARY KEY CHECK(id=1),
  partitions INTEGER NOT NULL,
  rows INTEGER NOT NULL,
  amount_cents INTEGER NOT NULL,
  types TEXT NOT NULL,
  updated_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT);
"""


class Consumer:
    def __init__(self, data_dir, catalog, interval):
        self.data_dir = data_dir
        self.catalog = catalog.rstrip("/")
        self.interval = interval
        self.db_path = os.path.join(data_dir, "consumer.db")
        self.pause_path = os.path.join(data_dir, "consumer.pause")
        self.fault_path = os.path.join(data_dir, "consumer_fault.json")
        con = self._conn()
        con.executescript(SCHEMA)
        con.commit()
        con.close()

    def _conn(self):
        con = sqlite3.connect(self.db_path, timeout=10)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA journal_mode=WAL")
        return con

    @staticmethod
    def _meta(con, k, default=None):
        r = con.execute("SELECT v FROM meta WHERE k=?", (k,)).fetchone()
        return r["v"] if r else default

    @staticmethod
    def _set_meta(con, k, v):
        con.execute(
            "INSERT INTO meta(k,v) VALUES(?,?)"
            " ON CONFLICT(k) DO UPDATE SET v=excluded.v",
            (k, str(v)),
        )

    def paused(self):
        return os.path.exists(self.pause_path)

    # ---------------- 主循环 ----------------
    def run(self):
        while True:
            try:
                if not self.paused():
                    self.tick()
            except Exception:
                self._record_error(traceback.format_exc(limit=3))
            time.sleep(self.interval)

    def _record_error(self, msg):
        try:
            con = self._conn()
            con.execute("BEGIN IMMEDIATE")
            self._set_meta(con, "last_error", msg[-800:])
            con.commit()
            con.close()
        except Exception:
            pass

    def tick(self):
        head = common.http_json("GET", self.catalog + "/api/head", timeout=3)["version"]
        con = self._conn()
        checkpoint = int(self._meta(con, "checkpoint", "0"))
        con.close()
        if head == checkpoint:
            return
        diff = common.http_json(
            "GET", f"{self.catalog}/api/diff?from={checkpoint}&to={head}", timeout=5)

        # 1) 事务外先读文件并校验（坏文件 => 本轮放弃，下轮重试，不前进 checkpoint）
        prepared = []
        for u in diff["upserts"]:
            full = common.safe_join(self.data_dir, u["path"])
            with open(full, "rb") as f:
                body = f.read()
            if common.sha256_bytes(body) != u["sha256"]:
                raise RuntimeError(f"sha256 校验失败: {u['path']}")
            payload = json.loads(body)
            types = Counter(r["type"] for r in payload["records"])
            prepared.append({
                "pkey": u["pkey"], "sha256": u["sha256"],
                "rows": len(payload["records"]),
                "amount_cents": sum(r["amount_cents"] for r in payload["records"]),
                "types": dict(types),
            })

        fault = (common.read_json(self.fault_path, {}) or {}).get("mode")
        now = time.time()
        con = self._conn()
        try:
            con.execute("BEGIN IMMEDIATE")
            for pk in diff["deletes"]:
                con.execute("DELETE FROM details WHERE pkey=?", (pk,))
            for p in prepared:
                con.execute(
                    """INSERT INTO details(pkey,sha256,rows,amount_cents,types,version,updated_at)
                       VALUES(?,?,?,?,?,?,?)
                       ON CONFLICT(pkey) DO UPDATE SET
                         sha256=excluded.sha256, rows=excluded.rows,
                         amount_cents=excluded.amount_cents, types=excluded.types,
                         version=excluded.version, updated_at=excluded.updated_at""",
                    (p["pkey"], p["sha256"], p["rows"], p["amount_cents"],
                     json.dumps(p["types"], ensure_ascii=False), head, now),
                )
            # 汇总与回执（checkpoint）同事务提交：要么都生效，要么都回滚
            agg = con.execute(
                "SELECT COUNT(*) c, COALESCE(SUM(rows),0) r,"
                " COALESCE(SUM(amount_cents),0) a FROM details"
            ).fetchone()
            types = Counter()
            for row in con.execute("SELECT types FROM details"):
                types.update(json.loads(row["types"]))
            con.execute(
                """INSERT INTO summary(id,partitions,rows,amount_cents,types,updated_at)
                   VALUES(1,?,?,?,?,?)
                   ON CONFLICT(id) DO UPDATE SET
                     partitions=excluded.partitions, rows=excluded.rows,
                     amount_cents=excluded.amount_cents, types=excluded.types,
                     updated_at=excluded.updated_at""",
                (agg["c"], agg["r"], agg["a"],
                 json.dumps(dict(types), ensure_ascii=False), now),
            )
            self._set_meta(con, "checkpoint", head)
            self._set_meta(con, "applied", int(self._meta(con, "applied", "0")) + 1)
            self._set_meta(con, "last_apply_ts", now)
            if fault == "crash_before_commit":
                self._consume_fault()
                con.rollback()
                con.close()
                os._exit(3)  # 猝死：事务未提交，汇总与回执一起丢失 -> 恢复后重放
            con.commit()
        except Exception:
            con.rollback()
            raise
        finally:
            con.close()

        if fault == "crash_after_commit":
            self._consume_fault()
            os._exit(4)  # 已提交但进程猝死，receipt 尚未上报 -> 回执丢失场景

        # 2) 上报回执给目录服务（仅展示用；丢失不影响正确性）
        try:
            common.http_json("POST", self.catalog + "/api/receipt",
                             {"name": "aggregator", "version": head}, timeout=2)
        except OSError:
            pass

    def _consume_fault(self):
        try:
            os.unlink(self.fault_path)
        except OSError:
            pass

    # ---------------- 状态查询 ----------------
    def status(self):
        con = self._conn()
        try:
            out = {
                "checkpoint": int(self._meta(con, "checkpoint", "0")),
                "applied": int(self._meta(con, "applied", "0")),
                "last_error": self._meta(con, "last_error"),
                "last_apply_ts": float(self._meta(con, "last_apply_ts", "0") or 0),
                "paused": self.paused(),
            }
        finally:
            con.close()
        try:
            out["head"] = common.http_json("GET", self.catalog + "/api/head", timeout=1.5)["version"]
        except Exception:
            out["head"] = None
        return out

    def summary(self):
        con = self._conn()
        try:
            row = con.execute("SELECT * FROM summary WHERE id=1").fetchone()
            base = {"checkpoint": int(self._meta(con, "checkpoint", "0"))}
            if not row:
                return {**base, "partitions": 0, "rows": 0, "amount_cents": 0,
                        "types": {}, "updated_at": None}
            return {**base, "partitions": row["partitions"], "rows": row["rows"],
                    "amount_cents": row["amount_cents"], "types": json.loads(row["types"]),
                    "updated_at": row["updated_at"]}
        finally:
            con.close()

    def details(self):
        con = self._conn()
        try:
            rows = con.execute("SELECT * FROM details ORDER BY pkey").fetchall()
            return [{
                "pkey": r["pkey"], "sha256": r["sha256"], "rows": r["rows"],
                "amount_cents": r["amount_cents"], "types": json.loads(r["types"]),
                "version": r["version"], "updated_at": r["updated_at"],
            } for r in rows]
        finally:
            con.close()


def make_handler(consumer):
    class Handler(BaseHTTPRequestHandler):
        server_version = "consumer/1.0"

        def log_message(self, *a):
            pass

        def _send(self, status, obj):
            body = json.dumps(obj, ensure_ascii=False).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            try:
                p = urlparse(self.path).path
                if p == "/api/health":
                    r = {"ok": True, "role": "consumer"}
                elif p == "/api/status":
                    r = consumer.status()
                elif p == "/api/summary":
                    r = consumer.summary()
                elif p == "/api/details":
                    r = {"details": consumer.details()}
                else:
                    return self._send(404, {"error": "not found"})
                self._send(200, r)
            except Exception as e:
                self._send(500, {"error": f"{type(e).__name__}: {e}"})

    return Handler


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", required=True)
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--interval", type=float, default=1.0)
    args = ap.parse_args()
    c = Consumer(args.data, args.catalog, args.interval)
    srv = ThreadingHTTPServer(("127.0.0.1", args.port), make_handler(c))
    srv.daemon_threads = True
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    print(f"consumer 已启动 port={args.port} catalog={args.catalog}", flush=True)
    c.run()


if __name__ == "__main__":
    main()
