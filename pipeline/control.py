#!/usr/bin/env python3
"""控制台（独立进程）：监管目录服务/消费者/写入器，提供 Web 控制台页面。

页面能力：逐步推进提交、杀死/重启任意一方、消费者暂停/恢复、故障注入、
版本回退、下架分区、回收孤儿文件，并实时查看目录版本、明细与汇总。
"""
import argparse
import json
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

import common

CODE_DIR = os.path.dirname(os.path.abspath(__file__))


class Control:
    def __init__(self, data_dir, ports):
        self.data_dir = data_dir
        self.ports = ports
        self.state_path = os.path.join(data_dir, "control_state.json")
        self.logs_dir = os.path.join(data_dir, "logs")
        os.makedirs(self.logs_dir, exist_ok=True)
        os.makedirs(os.path.join(data_dir, "objects"), exist_ok=True)
        self.state = common.read_json(self.state_path, None) or {
            "next_run": 1, "pids": {}, "log": [], "last_gc": None}
        self.procs = {}  # name -> Popen（仅本次控制台启动的；收养的只有 pid）
        self.lock = threading.Lock()

    # ---------------- 状态持久化 ----------------
    def save(self):
        common.write_json(self.state_path, self.state)

    def log(self, action, ok, detail):
        self.state["log"].append({"ts": time.time(), "action": action,
                                  "ok": bool(ok), "detail": str(detail)[:300]})
        self.state["log"] = self.state["log"][-12:]
        self.save()

    # ---------------- 进程管理 ----------------
    @staticmethod
    def pid_alive(pid):
        if not pid:
            return False
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False

    def proc_status(self, name):
        p = self.procs.get(name)
        if p is not None and p.poll() is None:
            return {"alive": True, "pid": p.pid}
        pid = self.state["pids"].get(name)
        alive = self.pid_alive(pid)
        return {"alive": alive, "pid": pid if alive else None}

    def start(self, name):
        if self.proc_status(name)["alive"]:
            return {"ok": True, "detail": "已在运行"}
        port = self.ports[name]
        logf = open(os.path.join(self.logs_dir, f"{name}.log"), "ab", buffering=0)
        if name == "catalog":
            cmd = [sys.executable, "catalog_service.py",
                   "--data", self.data_dir, "--port", str(port)]
        else:
            cmd = [sys.executable, "consumer.py",
                   "--data", self.data_dir, "--port", str(port),
                   "--catalog", self._cat(""), "--interval", "0.5"]
        p = subprocess.Popen(cmd, cwd=CODE_DIR, stdout=logf,
                             stderr=subprocess.STDOUT, start_new_session=True)
        self.procs[name] = p
        self.state["pids"][name] = p.pid
        self.save()
        return {"ok": True, "detail": f"已启动 pid={p.pid}"}

    def stop(self, name):
        pid = self.proc_status(name)["pid"]
        if not pid:
            return {"ok": True, "detail": "本来就没在运行"}
        try:
            os.kill(pid, signal.SIGKILL)
        except OSError as e:
            return {"ok": False, "detail": str(e)}
        p = self.procs.get(name)
        if p is not None:
            try:
                p.wait(timeout=3)
            except Exception:
                pass
        else:
            for _ in range(30):
                if not self.pid_alive(pid):
                    break
                time.sleep(0.1)
        return {"ok": True, "detail": f"已杀死 pid={pid}"}

    # ---------------- 动作 ----------------
    def _cat(self, path):
        return f"http://127.0.0.1:{self.ports['catalog']}{path}"

    def _cons(self, path):
        return f"http://127.0.0.1:{self.ports['consumer']}{path}"

    def action(self, name, params):
        with self.lock:
            try:
                result = self._action(name, params)
            except Exception as e:
                result = {"ok": False, "detail": f"{type(e).__name__}: {e}"}
            self.log(name, result.get("ok", False), result.get("detail", ""))
            return result

    def _action(self, name, p):
        if name == "commit":
            if p.get("drop_response"):  # 目录提交成功但响应丢失 -> 写入器重试被幂等去重
                common.write_json(os.path.join(self.data_dir, "catalog_fault.json"),
                                  {"commit_close": True, "once": True})
            return self._run_writer()
        if name == "writer_crash":
            return self._run_writer(crash="after_write")
        if name == "start_catalog":
            return self.start("catalog")
        if name == "stop_catalog":
            return self.stop("catalog")
        if name == "start_consumer":
            return self.start("consumer")
        if name == "stop_consumer":
            return self.stop("consumer")
        if name == "consumer_pause":
            open(os.path.join(self.data_dir, "consumer.pause"), "w").write("1")
            return {"ok": True, "detail": "消费者已暂停轮询（进程仍存活）"}
        if name == "consumer_resume":
            try:
                os.unlink(os.path.join(self.data_dir, "consumer.pause"))
            except OSError:
                pass
            return {"ok": True, "detail": "消费者已恢复轮询"}
        if name == "consumer_fault":
            common.write_json(os.path.join(self.data_dir, "consumer_fault.json"),
                              {"mode": p["mode"], "once": True})
            return {"ok": True, "detail": f"故障已注入：{p['mode']}，下一次应用时触发"}
        if name == "rollback":
            r = common.http_json("POST", self._cat("/api/rollback"),
                                 {"version": int(p["version"])}, timeout=5)
            return {"ok": True, "detail": f"head 已回退：v{r['was']} -> v{r['head']}"}
        if name == "delete_partition":
            r = common.http_json("POST", self._cat("/api/commit"), {
                "batch_id": f"del-{int(time.time() * 1000)}",
                "message": f"下架分区 {p['pkey']}",
                "adds": [], "deletes": [p["pkey"]],
            }, timeout=5)
            return {"ok": True, "detail": f"已提交下架版本 v{r['version']}"}
        if name == "gc":
            return self._run_gc(float(p.get("grace", 30)))
        if name == "reset":
            return self._reset()
        return {"ok": False, "detail": f"未知操作 {name}"}

    def _run_writer(self, crash=None):
        run = self.state["next_run"]
        logf = os.path.join(self.logs_dir, f"writer-{run:04d}-{int(time.time())}.log")
        cmd = [sys.executable, "writer.py", "--data", self.data_dir,
               "--catalog", self._cat(""), "--run", str(run)]
        if crash:
            cmd += ["--crash", crash]
        with open(logf, "wb") as lf:
            rc = subprocess.run(cmd, cwd=CODE_DIR, stdout=lf,
                                stderr=subprocess.STDOUT, timeout=30).returncode
        out = ""
        try:
            with open(logf, "r", encoding="utf-8") as f:
                lines = [l for l in f.read().strip().splitlines() if l.strip()]
                out = lines[-1] if lines else ""
        except OSError:
            pass
        try:
            info = json.loads(out) if out else {}
        except ValueError:
            info = {"raw": out}
        if rc == 0 and info.get("status") == "committed":
            self.state["next_run"] = run + 1
            self.save()
            tag = "（重复提交被目录幂等去重）" if info.get("deduped") else ""
            return {"ok": True,
                    "detail": f"批次 run-{run:04d} 已提交为 v{info.get('version')}{tag}"}
        if crash:
            return {"ok": True,
                    "detail": f"写入器已崩溃(rc={rc})：文件落盘但未提交，已产生孤儿文件"}
        return {"ok": False, "detail": f"写入器失败 rc={rc}: {out[-200:]}"}

    def _run_gc(self, grace):
        logf = os.path.join(self.logs_dir, f"gc-{int(time.time())}.log")
        cmd = [sys.executable, "gc.py", "--data", self.data_dir,
               "--catalog", self._cat(""), "--grace", str(grace)]
        with open(logf, "wb") as lf:
            rc = subprocess.run(cmd, cwd=CODE_DIR, stdout=lf,
                                stderr=subprocess.STDOUT, timeout=30).returncode
        with open(logf, "r", encoding="utf-8") as f:
            out = f.read().strip().splitlines()
        try:
            report = json.loads(out[-1]) if out else {}
        except ValueError:
            report = {}
        self.state["last_gc"] = report
        self.save()
        if rc != 0:
            return {"ok": False, "detail": f"GC 失败 rc={rc}"}
        return {"ok": True,
                "detail": (f"GC 完成：删除 {len(report.get('deleted', []))} 个孤儿，"
                           f"保留被引用 {report.get('kept_referenced', 0)} 个，"
                           f"跳过过新 {len(report.get('skipped_young', []))} 个")}

    def _reset(self):
        self.stop("catalog")
        self.stop("consumer")
        shutil.rmtree(self.data_dir, ignore_errors=True)
        os.makedirs(os.path.join(self.data_dir, "objects"), exist_ok=True)
        os.makedirs(self.logs_dir, exist_ok=True)
        self.state = {"next_run": 1, "pids": {}, "log": [], "last_gc": None}
        self.save()
        self.start("catalog")
        self.start("consumer")
        return {"ok": True, "detail": "已清空全部数据并重启目录服务与消费者"}

    # ---------------- 状态聚合 ----------------
    def snapshot(self):
        snap = {"time": time.time(), "next_run": self.state["next_run"],
                "log": self.state["log"], "last_gc": self.state.get("last_gc"),
                "procs": {}}
        for n in ("catalog", "consumer"):
            st = self.proc_status(n)
            if n == "consumer":
                st["paused"] = os.path.exists(os.path.join(self.data_dir, "consumer.pause"))
            snap["procs"][n] = st
        try:
            snap["catalog"] = {
                "head": common.http_json("GET", self._cat("/api/head"), timeout=1.5),
                "versions": common.http_json("GET", self._cat("/api/versions"), timeout=1.5),
                "events": common.http_json("GET", self._cat("/api/events"), timeout=1.5),
                "receipts": common.http_json("GET", self._cat("/api/receipts"), timeout=1.5),
                "manifest": common.http_json("GET", self._cat("/api/manifest"), timeout=1.5),
            }
        except Exception as e:
            snap["catalog"] = {"error": str(e)}
        try:
            snap["consumer"] = {
                "status": common.http_json("GET", self._cons("/api/status"), timeout=1.5),
                "summary": common.http_json("GET", self._cons("/api/summary"), timeout=1.5),
                "details": common.http_json("GET", self._cons("/api/details"), timeout=1.5)["details"],
            }
        except Exception as e:
            snap["consumer"] = {"error": str(e)}
        snap["orphans"] = self._orphans()
        return snap

    def _orphans(self):
        try:
            referenced = set(common.http_json(
                "GET", self._cat("/api/referenced"), timeout=1.5)["paths"])
        except Exception as e:
            return {"error": f"目录服务不可用: {e}"}
        objs = os.path.join(self.data_dir, "objects")
        now = time.time()
        out = []
        try:
            for name in sorted(os.listdir(objs)):
                full = os.path.join(objs, name)
                if not os.path.isfile(full):
                    continue
                rel = f"objects/{name}"
                if rel not in referenced:
                    out.append({"path": rel,
                                "age": round(now - os.path.getmtime(full), 1),
                                "bytes": os.path.getsize(full)})
        except OSError as e:
            return {"error": str(e)}
        return {"files": out}


INDEX_HTML = r"""<!doctype html>
<html lang="zh">
<head>
<meta charset="utf-8">
<title>本地数据管道控制台</title>
<style>
  :root { --bg:#0f1420; --card:#1a2233; --line:#2a3550; --fg:#dbe4f5; --dim:#8fa0c0;
          --ok:#4ade80; --bad:#f87171; --warn:#fbbf24; --acc:#60a5fa; }
  * { box-sizing: border-box; }
  body { margin:0; padding:18px; background:var(--bg); color:var(--fg);
         font:14px/1.5 -apple-system,"Segoe UI","PingFang SC","Microsoft YaHei",sans-serif; }
  h1 { font-size:19px; margin:0 0 12px; }
  h2 { font-size:15px; margin:0 0 10px; color:var(--acc); }
  .grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(430px,1fr)); gap:12px; }
  .card { background:var(--card); border:1px solid var(--line); border-radius:10px; padding:14px; }
  .wide { grid-column:1/-1; }
  button { background:#243050; color:var(--fg); border:1px solid var(--line);
           border-radius:6px; padding:5px 10px; margin:2px 3px 2px 0; cursor:pointer; font-size:13px; }
  button:hover { border-color:var(--acc); }
  button.danger { border-color:#7f2d2d; }
  button:disabled { opacity:.4; cursor:default; }
  input,select { background:#0d1322; color:var(--fg); border:1px solid var(--line);
                 border-radius:6px; padding:4px 7px; font-size:13px; width:90px; }
  table { border-collapse:collapse; width:100%; font-size:12.5px; }
  th,td { border-bottom:1px solid var(--line); padding:4px 7px; text-align:left; vertical-align:top; }
  th { color:var(--dim); font-weight:600; }
  .mono { font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace; }
  .dim { color:var(--dim); }
  .ok { color:var(--ok); } .bad { color:var(--bad); } .warn { color:var(--warn); }
  .dot { display:inline-block; width:9px; height:9px; border-radius:50%; margin-right:6px; }
  .tiles { display:flex; gap:10px; flex-wrap:wrap; margin:6px 0; }
  .tile { background:#0d1322; border:1px solid var(--line); border-radius:8px;
          padding:8px 14px; min-width:110px; }
  .tile .v { font-size:20px; font-weight:700; }
  .tile .k { font-size:12px; color:var(--dim); }
  .badge { display:inline-block; background:#0d1322; border:1px solid var(--line);
           border-radius:10px; padding:1px 8px; margin:1px 2px; font-size:12px; }
  .err { background:#3a1520; border:1px solid #7f2d2d; border-radius:6px;
         padding:6px 9px; margin-top:6px; font-size:12px; white-space:pre-wrap; }
  .row { display:flex; align-items:center; gap:8px; flex-wrap:wrap; margin:4px 0; }
  .note { font-size:12.5px; color:var(--dim); }
  ul.notes { margin:6px 0 0; padding-left:18px; font-size:12.5px; color:var(--dim); }
</style>
</head>
<body>
<h1>本地数据管道控制台 <span class="dim" id="clock"></span></h1>
<div class="grid">
  <div class="card" id="procs"></div>
  <div class="card" id="actions"></div>
  <div class="card" id="catalog"></div>
  <div class="card" id="manifest"></div>
  <div class="card wide" id="consumer"></div>
  <div class="card" id="orphans"></div>
  <div class="card" id="log"></div>
  <div class="card wide">
    <h2>设计说明</h2>
    <ul class="notes">
      <li><b>去重放在消费端</b>：明细按分区键幂等 upsert；汇总与回执（checkpoint）在同一个 SQLite 事务提交 —— 崩溃只会一起回滚，重放不会重复计入。</li>
      <li>目录提交按 <span class="mono">batch_id</span> 幂等：写入器重试同一批次不会产生重复版本（响应丢失时会看到"幂等去重"）。</li>
      <li>半成品不可见：文件原子落盘（tmp+rename），消费者只能凭目录清单读取；提交前目录逐文件校验 sha256。</li>
      <li>回退后重消费：diff 是集合运算，回退导致消失的分区从明细删除、重现的分区重新计入，汇总始终等于 head 清单的物化视图。</li>
      <li>孤儿回收：只删"所有历史版本都不引用"且超过宽限期的文件，仍被引用的文件绝不删除。</li>
    </ul>
  </div>
</div>
<script>
const $ = id => document.getElementById(id);
const esc = s => String(s ?? '').replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
const fmtT = t => t ? new Date(t*1000).toLocaleTimeString() : '-';
const fmtAmt = c => '¥' + (c/100).toFixed(2);
const dot = ok => `<span class="dot" style="background:${ok?'var(--ok)':'var(--bad)'}"></span>`;

async function act(action, params={}) {
  try {
    const r = await fetch('/api/action', {method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify({action, ...params})});
    const j = await r.json();
    if (!j.ok) alert('操作失败: ' + (j.detail || j.error || ''));
  } catch(e) { alert('请求失败: ' + e); }
  refresh();
}

function typesBadges(t) {
  return Object.entries(t||{}).map(([k,v]) =>
    `<span class="badge">${esc(k)} × ${v}</span>`).join('');
}

function renderProcs(s) {
  const c = s.procs.catalog, m = s.procs.consumer;
  $('procs').innerHTML = `<h2>进程</h2>
    <div class="row">${dot(c.alive)}<b>目录服务</b> <span class="dim mono">pid=${c.pid??'-'}</span>
      <button onclick="act('stop_catalog')" ${c.alive?'':'disabled'}>断开</button>
      <button onclick="act('start_catalog')" ${c.alive?'disabled':''}>启动</button></div>
    <div class="row">${dot(m.alive)}<b>消费汇总器</b> <span class="dim mono">pid=${m.pid??'-'}</span>
      ${m.paused?'<span class="warn">已暂停轮询</span>':''}
      <button onclick="act('stop_consumer')" ${m.alive?'':'disabled'}>断开</button>
      <button onclick="act('start_consumer')" ${m.alive?'disabled':''}>启动</button>
      <button onclick="act('consumer_pause')" ${m.alive&&!m.paused?'':'disabled'}>暂停</button>
      <button onclick="act('consumer_resume')" ${m.paused?'':'disabled'}>恢复</button></div>
    <div class="row"><b>写入器</b> <span class="dim">一次性进程，下一批次 run-${String(s.next_run).padStart(4,'0')}</span></div>
    <div class="row"><span class="note">故障注入（消费者下次应用时崩溃）：</span>
      <select id="faultMode">
        <option value="crash_before_commit">提交前崩溃（回执+汇总一起丢失 → 重放）</option>
        <option value="crash_after_commit">提交后崩溃（回执未上报 → 目录侧滞后）</option>
      </select>
      <button onclick="act('consumer_fault',{mode:document.getElementById('faultMode').value})" ${m.alive?'':'disabled'}>注入</button></div>`;
}

function renderActions(s) {
  $('actions').innerHTML = `<h2>操作</h2>
    <div class="row">
      <button onclick="act('commit')">提交下一批</button>
      <button onclick="act('commit',{drop_response:true})" title="目录提交成功但响应丢失，写入器重试">提交·模拟响应丢失</button>
      <button onclick="act('writer_crash')" title="文件落盘后崩溃，不提交目录">写入器崩溃·只落盘</button>
    </div>
    <div class="row">
      回退到版本 <input id="rbVer" type="number" min="0" style="width:70px">
      <button onclick="act('rollback',{version:+document.getElementById('rbVer').value})">回退</button>
      宽限 <input id="gcGrace" type="number" value="30" min="0" style="width:60px"> 秒
      <button onclick="act('gc',{grace:+document.getElementById('gcGrace').value})">回收孤儿文件</button>
      <button class="danger" onclick="if(confirm('清空全部数据并重启？'))act('reset')">重置全部</button>
    </div>
    <div class="note">提示：先"暂停"消费者，连提几批再"恢复"，可观察增量追赶；提交后立刻"断开"消费者再"启动"，验证不重复计入。</div>`;
}

function renderCatalog(s) {
  if (s.catalog.error) { $('catalog').innerHTML = `<h2>目录服务</h2><div class="err">${esc(s.catalog.error)}</div>`; return; }
  const h = s.catalog.head, vs = s.catalog.versions, rc = s.catalog.receipts.receipts;
  const rows = vs.versions.slice().reverse().map(v => `<tr>
    <td class="mono">v${v.version}${v.head?' <span class="ok">●head</span>':''}</td>
    <td class="mono">${esc(v.batch_id??'-')}</td><td>${esc(v.message)}</td>
    <td>${v.n}</td><td class="dim">${fmtT(v.created_at)}</td>
    <td>${v.head?'':`<button onclick="act('rollback',{version:${v.version}})">回退</button>`}</td></tr>`).join('');
  const receipts = rc.map(r => `<span class="badge">${esc(r.name)} → v${r.version}</span>`).join('') || '<span class="dim">无</span>';
  const evs = s.catalog.events.events.slice(0,8).map(e =>
    `<div class="note mono">${fmtT(e.ts)} ${esc(e.kind)} ${esc(JSON.stringify(e.detail))}</div>`).join('');
  $('catalog').innerHTML = `<h2>目录服务</h2>
    <div class="tiles">
      <div class="tile"><div class="v">v${h.version}</div><div class="k">head 版本</div></div>
      <div class="tile"><div class="v">${h.partitions}</div><div class="k">当前分区数</div></div>
      <div class="tile"><div class="v">${vs.versions.length-1}</div><div class="k">累计提交</div></div>
    </div>
    <div class="row"><span class="dim">消费回执：</span>${receipts}</div>
    <table><tr><th>版本</th><th>批次</th><th>说明</th><th>分区</th><th>时间</th><th></th></tr>${rows}</table>
    <h2 style="margin-top:10px">事件</h2>${evs || '<div class="note">暂无</div>'}`;
}

function renderManifest(s) {
  if (s.catalog.error) { $('manifest').innerHTML = `<h2>当前清单</h2><div class="err">目录不可用</div>`; return; }
  const m = s.catalog.manifest;
  const rows = m.files.map(f => `<tr>
    <td class="mono">${esc(f.pkey)}</td><td>${f.rows}</td><td>${f.bytes}</td>
    <td class="mono dim" title="${esc(f.sha256)}">${esc(f.sha256.slice(0,10))}</td>
    <td class="dim">run ${esc(f.meta.run??'-')} / 脏行 ${esc(f.meta.dropped??'-')}</td>
    <td><button onclick="if(confirm('下架分区 ${esc(f.pkey)}？'))act('delete_partition',{pkey:'${esc(f.pkey)}'})">下架</button></td></tr>`).join('');
  $('manifest').innerHTML = `<h2>当前清单（head = v${m.version}）</h2>
    ${m.files.length ? `<table><tr><th>分区键</th><th>行数</th><th>字节</th><th>sha256</th><th>来源</th><th></th></tr>${rows}</table>`
                     : '<div class="note">空清单</div>'}`;
}

function renderConsumer(s) {
  if (s.consumer.error) { $('consumer').innerHTML = `<h2>消费汇总器</h2><div class="err">${esc(s.consumer.error)}</div>`; return; }
  const st = s.consumer.status, sm = s.consumer.summary, ds = s.consumer.details;
  const lag = (st.head!=null) ? st.head - st.checkpoint : '?';
  const rows = ds.map(d => `<tr>
    <td class="mono">${esc(d.pkey)}</td><td>${d.rows}</td><td>${fmtAmt(d.amount_cents)}</td>
    <td>${typesBadges(d.types)}</td><td class="mono dim" title="${esc(d.sha256)}">${esc(d.sha256.slice(0,10))}</td>
    <td class="mono">v${d.version}</td><td class="dim">${fmtT(d.updated_at)}</td></tr>`).join('');
  $('consumer').innerHTML = `<h2>消费汇总器（明细与汇总）</h2>
    <div class="tiles">
      <div class="tile"><div class="v">v${st.checkpoint}</div><div class="k">checkpoint（回执）</div></div>
      <div class="tile"><div class="v ${lag>0?'warn':''}">${lag}</div><div class="k">滞后（head-checkpoint）</div></div>
      <div class="tile"><div class="v">${st.applied}</div><div class="k">已应用批次</div></div>
      <div class="tile"><div class="v">${sm.partitions}</div><div class="k">汇总·分区数</div></div>
      <div class="tile"><div class="v">${sm.rows}</div><div class="k">汇总·总行数</div></div>
      <div class="tile"><div class="v">${fmtAmt(sm.amount_cents)}</div><div class="k">汇总·总金额</div></div>
    </div>
    <div class="row"><span class="dim">分类汇总：</span>${typesBadges(sm.types)||'<span class="dim">无</span>'}
      <span class="dim" style="margin-left:12px">汇总更新时间 ${fmtT(sm.updated_at)}</span></div>
    ${st.last_error?`<div class="err">${esc(st.last_error)}</div>`:''}
    <table><tr><th>分区键</th><th>行数</th><th>金额</th><th>分类</th><th>sha256</th><th>版本</th><th>更新时间</th></tr>
    ${rows || '<tr><td colspan="7" class="dim">暂无明细</td></tr>'}</table>`;
}

function renderOrphans(s) {
  const o = s.orphans;
  let body;
  if (o.error) body = `<div class="err">${esc(o.error)}</div>`;
  else if (!o.files.length) body = '<div class="note">无孤儿文件（磁盘上的文件均被目录引用）</div>';
  else body = `<table><tr><th>路径</th><th>年龄(s)</th><th>字节</th></tr>` + o.files.map(f =>
    `<tr><td class="mono">${esc(f.path)}</td><td>${f.age}</td><td>${f.bytes}</td></tr>`).join('') + `</table>`;
  const gc = s.last_gc ? `<div class="note" style="margin-top:8px">上次 GC：删除 ${s.last_gc.deleted?.length??0}，
    保留被引用 ${s.last_gc.kept_referenced??0}，跳过过新 ${s.last_gc.skipped_young?.length??0}（宽限 ${s.last_gc.grace}s）</div>` : '';
  $('orphans').innerHTML = `<h2>孤儿文件（落盘但未被目录引用）</h2>${body}${gc}`;
}

function renderLog(s) {
  const items = s.log.slice().reverse().map(l =>
    `<div class="note">${fmtT(l.ts)} <span class="${l.ok?'ok':'bad'}">${l.ok?'✓':'✗'}</span>
     <b>${esc(l.action)}</b> ${esc(l.detail)}</div>`).join('');
  $('log').innerHTML = `<h2>操作日志</h2>${items || '<div class="note">暂无</div>'}`;
}

async function refresh() {
  try {
    const s = await (await fetch('/api/state')).json();
    $('clock').textContent = new Date(s.time*1000).toLocaleTimeString();
    renderProcs(s); renderActions(s); renderCatalog(s);
    renderManifest(s); renderConsumer(s); renderOrphans(s); renderLog(s);
  } catch(e) { /* 控制台重启中 */ }
}
setInterval(refresh, 1000);
refresh();
</script>
</body>
</html>
"""


def make_handler(ctl):
    class Handler(BaseHTTPRequestHandler):
        server_version = "control/1.0"

        def log_message(self, *a):
            pass

        def _send(self, status, obj, ctype="application/json; charset=utf-8"):
            body = obj.encode() if isinstance(obj, str) else json.dumps(obj, ensure_ascii=False).encode()
            self.send_response(status)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            p = urlparse(self.path).path
            if p == "/":
                self._send(200, INDEX_HTML, "text/html; charset=utf-8")
            elif p == "/api/state":
                self._send(200, ctl.snapshot())
            else:
                self._send(404, {"error": "not found"})

        def do_POST(self):
            try:
                body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))) or b"{}")
                if urlparse(self.path).path != "/api/action":
                    return self._send(404, {"error": "not found"})
                action = body.pop("action", None)
                self._send(200, ctl.action(action, body))
            except Exception as e:
                self._send(500, {"ok": False, "detail": f"{type(e).__name__}: {e}"})

    return Handler


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default="/workspace/pipeline_data")
    ap.add_argument("--port", type=int, default=8620)
    ap.add_argument("--catalog-port", type=int, default=8621)
    ap.add_argument("--consumer-port", type=int, default=8622)
    args = ap.parse_args()
    ctl = Control(args.data, {"catalog": args.catalog_port, "consumer": args.consumer_port})
    print(ctl.start("catalog"), flush=True)
    print(ctl.start("consumer"), flush=True)
    srv = ThreadingHTTPServer(("127.0.0.1", args.port), make_handler(ctl))
    srv.daemon_threads = True
    print(f"控制台已启动: http://127.0.0.1:{args.port}", flush=True)
    srv.serve_forever()


if __name__ == "__main__":
    main()
