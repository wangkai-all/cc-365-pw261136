#!/usr/bin/env python3
"""目录服务（独立进程）：分区清单的版本化、原子提交、回退与消费回执登记。

不变式：
- 版本号只增不减，历史版本永不改写；head 指针可回退到任意历史版本；
- 每个版本物化保存完整清单（files 表），diff 由两个版本的集合比较得出；
- 提交按 batch_id 幂等：同一批次重复提交返回原版本号，不会产生重复版本；
- 提交前逐文件校验存在性与 sha256 —— 缺失/损坏的半成品进不了目录；
- 消费者只能凭目录清单拿到文件路径，未提交的文件对下游不可见。
"""
import argparse
import json
import os
import sqlite3
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import common

SCHEMA = """
CREATE TABLE IF NOT EXISTS commits(
  version INTEGER PRIMARY KEY,
  batch_id TEXT UNIQUE NOT NULL,
  message TEXT NOT NULL DEFAULT '',
  created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS files(
  version INTEGER NOT NULL,
  pkey TEXT NOT NULL,
  path TEXT NOT NULL,
  sha256 TEXT NOT NULL,
  rows INTEGER NOT NULL,
  bytes INTEGER NOT NULL,
  meta TEXT NOT NULL DEFAULT '{}',
  PRIMARY KEY(version, pkey)
);
CREATE TABLE IF NOT EXISTS head(id INTEGER PRIMARY KEY CHECK(id=1), version INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS events(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts REAL NOT NULL,
  kind TEXT NOT NULL,
  detail TEXT NOT NULL DEFAULT '{}'
);
CREATE TABLE IF NOT EXISTS receipts(
  name TEXT PRIMARY KEY,
  version INTEGER NOT NULL,
  ts REAL NOT NULL
);
"""


class Catalog:
    def __init__(self, data_dir):
        self.data_dir = data_dir
        os.makedirs(os.path.join(data_dir, "objects"), exist_ok=True)
        self.db_path = os.path.join(data_dir, "catalog.db")
        self.fault_path = os.path.join(data_dir, "catalog_fault.json")
        self.lock = threading.Lock()
        con = self._conn()
        con.executescript(SCHEMA)
        con.execute("INSERT OR IGNORE INTO head(id, version) VALUES(1, 0)")
        con.commit()
        con.close()

    def _conn(self):
        con = sqlite3.connect(self.db_path, timeout=10)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA journal_mode=WAL")
        return con

    def _q(self, fn):
        con = self._conn()
        try:
            return fn(con)
        finally:
            con.close()

    @staticmethod
    def _manifest(con, version):
        rows = con.execute(
            "SELECT pkey,path,sha256,rows,bytes,meta FROM files WHERE version=? ORDER BY pkey",
            (version,),
        ).fetchall()
        return [dict(r) for r in rows]

    # ---------------- 只读 API ----------------
    def head(self):
        def fn(con):
            v = con.execute("SELECT version FROM head WHERE id=1").fetchone()["version"]
            c = con.execute(
                "SELECT batch_id,message,created_at FROM commits WHERE version=?", (v,)
            ).fetchone()
            n = con.execute("SELECT COUNT(*) n FROM files WHERE version=?", (v,)).fetchone()["n"]
            return {
                "version": v,
                "partitions": n,
                "batch_id": c["batch_id"] if c else None,
                "message": c["message"] if c else "(初始空版本)",
                "created_at": c["created_at"] if c else None,
            }
        return self._q(fn)

    def versions(self):
        def fn(con):
            head_v = con.execute("SELECT version FROM head WHERE id=1").fetchone()["version"]
            rows = con.execute(
                """SELECT c.version, c.batch_id, c.message, c.created_at,
                          (SELECT COUNT(*) FROM files f WHERE f.version=c.version) n
                   FROM commits c ORDER BY c.version"""
            ).fetchall()
            out = [
                {"version": 0, "batch_id": None, "message": "(初始空版本)",
                 "created_at": None, "n": 0, "head": head_v == 0}
            ]
            for r in rows:
                out.append({**dict(r), "head": r["version"] == head_v})
            return {"head": head_v, "versions": out}
        return self._q(fn)

    def manifest(self, version=None):
        def fn(con):
            v = version
            if v is None:
                v = con.execute("SELECT version FROM head WHERE id=1").fetchone()["version"]
            files = [{**m, "meta": json.loads(m["meta"])} for m in self._manifest(con, v)]
            return {"version": v, "files": files}
        return self._q(fn)

    def diff(self, frm, to):
        """集合式 diff：upserts = 新增或内容变化（sha256 不同），deletes = 消失的分区键。
        对任意两个版本成立（包括回退造成的 from > to）。"""
        def fn(con):
            a = {m["pkey"]: m for m in self._manifest(con, frm)}
            b = {m["pkey"]: m for m in self._manifest(con, to)}
            upserts = [
                {**b[k], "meta": json.loads(b[k]["meta"])}
                for k in sorted(b)
                if k not in a or a[k]["sha256"] != b[k]["sha256"]
            ]
            deletes = sorted(k for k in a if k not in b)
            return {"from": frm, "to": to, "upserts": upserts, "deletes": deletes}
        return self._q(fn)

    def events(self, limit=60):
        def fn(con):
            rows = con.execute(
                "SELECT id,ts,kind,detail FROM events ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
            return {"events": [{**dict(r), "detail": json.loads(r["detail"])} for r in rows]}
        return self._q(fn)

    def receipts(self):
        def fn(con):
            rows = con.execute("SELECT name,version,ts FROM receipts ORDER BY name").fetchall()
            return {"receipts": [dict(r) for r in rows]}
        return self._q(fn)

    def referenced(self):
        """所有历史版本引用过的文件全集 —— 回退可能复活旧版本，GC 必须以此为基准。"""
        def fn(con):
            rows = con.execute("SELECT DISTINCT path FROM files ORDER BY path").fetchall()
            return {"paths": [r["path"] for r in rows]}
        return self._q(fn)

    # ---------------- 写 API ----------------
    def commit(self, batch_id, message, adds, deletes):
        # 1) 提交前校验：文件必须真实存在且内容哈希匹配，半成品进不了目录
        for a in adds:
            full = common.safe_join(self.data_dir, a["path"])
            if not os.path.isfile(full):
                raise common.HttpError(400, f"文件不存在: {a['path']}")
            if common.sha256_file(full) != a["sha256"]:
                raise common.HttpError(400, f"sha256 不匹配: {a['path']}")
            a["bytes"] = os.path.getsize(full)
        with self.lock:
            con = self._conn()
            try:
                con.execute("BEGIN IMMEDIATE")
                dup = con.execute(
                    "SELECT version FROM commits WHERE batch_id=?", (batch_id,)
                ).fetchone()
                if dup:  # 幂等：同一批次重复提交，返回原版本号
                    con.rollback()
                    return {"version": dup["version"], "deduped": True}
                newv = con.execute("SELECT COALESCE(MAX(version),0)+1 FROM commits").fetchone()[0]
                head_v = con.execute("SELECT version FROM head WHERE id=1").fetchone()["version"]
                manifest = {m["pkey"]: m for m in self._manifest(con, head_v)}
                for pk in deletes:
                    manifest.pop(pk, None)
                for a in adds:
                    manifest[a["pkey"]] = {
                        "pkey": a["pkey"], "path": a["path"], "sha256": a["sha256"],
                        "rows": int(a["rows"]), "bytes": int(a["bytes"]),
                        "meta": json.dumps(a.get("meta", {}), ensure_ascii=False),
                    }
                con.execute(
                    "INSERT INTO commits(version,batch_id,message,created_at) VALUES(?,?,?,?)",
                    (newv, batch_id, message, time.time()),
                )
                for m in manifest.values():
                    con.execute(
                        "INSERT INTO files(version,pkey,path,sha256,rows,bytes,meta)"
                        " VALUES(?,?,?,?,?,?,?)",
                        (newv, m["pkey"], m["path"], m["sha256"], m["rows"], m["bytes"], m["meta"]),
                    )
                con.execute("UPDATE head SET version=? WHERE id=1", (newv,))
                con.execute(
                    "INSERT INTO events(ts,kind,detail) VALUES(?,?,?)",
                    (time.time(), "commit", json.dumps(
                        {"version": newv, "batch_id": batch_id,
                         "adds": len(adds), "deletes": len(deletes)}, ensure_ascii=False)),
                )
                con.commit()
                return {"version": newv, "deduped": False}
            except Exception:
                con.rollback()
                raise
            finally:
                con.close()

    def rollback(self, version):
        with self.lock:
            con = self._conn()
            try:
                con.execute("BEGIN IMMEDIATE")
                if version != 0 and not con.execute(
                    "SELECT 1 FROM commits WHERE version=?", (version,)
                ).fetchone():
                    raise common.HttpError(400, f"版本不存在: {version}")
                old = con.execute("SELECT version FROM head WHERE id=1").fetchone()["version"]
                con.execute("UPDATE head SET version=? WHERE id=1", (version,))
                con.execute(
                    "INSERT INTO events(ts,kind,detail) VALUES(?,?,?)",
                    (time.time(), "rollback",
                     json.dumps({"from": old, "to": version}, ensure_ascii=False)),
                )
                con.commit()
                return {"head": version, "was": old}
            except Exception:
                con.rollback()
                raise
            finally:
                con.close()

    def receipt(self, name, version):
        """消费回执登记（仅用于展示消费滞后；丢失不影响正确性，权威状态在消费者本地）。"""
        def fn(con):
            con.execute(
                "INSERT INTO receipts(name,version,ts) VALUES(?,?,?)"
                " ON CONFLICT(name) DO UPDATE SET version=excluded.version, ts=excluded.ts",
                (name, version, time.time()),
            )
            con.commit()
            return {"ok": True}
        return self._q(fn)


def make_handler(cat):
    class Handler(BaseHTTPRequestHandler):
        server_version = "catalog/1.0"

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
                u = urlparse(self.path)
                q = parse_qs(u.query)
                p = u.path
                if p == "/api/health":
                    r = {"ok": True, "role": "catalog"}
                elif p == "/api/head":
                    r = cat.head()
                elif p == "/api/versions":
                    r = cat.versions()
                elif p == "/api/manifest":
                    r = cat.manifest(int(q["version"][0]) if "version" in q else None)
                elif p == "/api/diff":
                    r = cat.diff(int(q["from"][0]), int(q["to"][0]))
                elif p == "/api/events":
                    r = cat.events()
                elif p == "/api/receipts":
                    r = cat.receipts()
                elif p == "/api/referenced":
                    r = cat.referenced()
                else:
                    return self._send(404, {"error": "not found"})
                self._send(200, r)
            except common.HttpError as e:
                self._send(e.status, {"error": e.body})
            except Exception as e:
                self._send(500, {"error": f"{type(e).__name__}: {e}"})

        def do_POST(self):
            try:
                body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))) or b"{}")
                p = urlparse(self.path).path
                if p == "/api/commit":
                    r = cat.commit(body["batch_id"], body.get("message", ""),
                                   body.get("adds", []), body.get("deletes", []))
                    # 故障注入：提交已落库，但假装网络断开不回响应（写入器将重试并被幂等去重）
                    fault = common.read_json(cat.fault_path, {}) or {}
                    if fault.get("commit_close"):
                        if fault.get("once", True):
                            try:
                                os.unlink(cat.fault_path)
                            except OSError:
                                pass
                        self.close_connection = True
                        self.connection.close()
                        return
                elif p == "/api/rollback":
                    r = cat.rollback(int(body["version"]))
                elif p == "/api/receipt":
                    r = cat.receipt(body["name"], int(body["version"]))
                else:
                    return self._send(404, {"error": "not found"})
                self._send(200, r)
            except common.HttpError as e:
                self._send(e.status, {"error": e.body})
            except Exception as e:
                self._send(500, {"error": f"{type(e).__name__}: {e}"})

    return Handler


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", required=True)
    ap.add_argument("--port", type=int, required=True)
    args = ap.parse_args()
    cat = Catalog(args.data)
    srv = ThreadingHTTPServer(("127.0.0.1", args.port), make_handler(cat))
    srv.daemon_threads = True
    print(f"catalog 已启动 port={args.port} data={args.data}", flush=True)
    srv.serve_forever()


if __name__ == "__main__":
    main()
