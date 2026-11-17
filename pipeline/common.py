"""共享工具：原子写文件、哈希、HTTP JSON 调用、JSON 文件读写、路径防穿越。"""
import hashlib
import json
import os
import tempfile
import urllib.error
import urllib.request


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def atomic_write(path: str, data: bytes) -> None:
    """先写临时文件并 fsync，再 os.replace 原子改名，最后 fsync 目录。

    任何时刻读者只能看到完整旧文件或完整新文件，绝不会看到半成品。
    """
    d = os.path.dirname(path)
    os.makedirs(d, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=d, prefix=".tmp-")
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
        dirfd = os.open(d, os.O_DIRECTORY)
        try:
            os.fsync(dirfd)
        finally:
            os.close(dirfd)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


class HttpError(Exception):
    """对端返回了非 2xx 状态码（属于确定性拒绝，不应盲目重试）。"""

    def __init__(self, status, body):
        super().__init__(f"HTTP {status}: {body}")
        self.status = status
        self.body = body


def http_json(method, url, payload=None, timeout=3.0, headers=None):
    """发起 HTTP JSON 调用。网络层错误抛 OSError（可重试），HTTP 状态错误抛 HttpError。"""
    data = None
    hdrs = {"Accept": "application/json"}
    if headers:
        hdrs.update(headers)
    if payload is not None:
        data = json.dumps(payload).encode()
        hdrs["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=hdrs, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "replace")
        raise HttpError(e.code, body)


def read_json(path, default=None):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return default


def write_json(path, obj):
    atomic_write(path, json.dumps(obj, ensure_ascii=False, indent=1).encode())


def safe_join(root, rel):
    """防目录穿越：rel 必须解析到 root 之内。"""
    p = os.path.realpath(os.path.join(root, rel))
    root_r = os.path.realpath(root)
    if p != root_r and not p.startswith(root_r + os.sep):
        raise ValueError(f"非法路径: {rel}")
    return p
