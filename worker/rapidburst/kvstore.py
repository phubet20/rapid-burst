"""Tiny durable key/value client for the learner memory (no extra dependency).

Render's free web service has no persistent disk, so `learning.json` would be lost on every redeploy or
spin-down. When RB_LEARN_REDIS_URL is set (a Render Key Value instance, internal URL, reachable only from the
owner's Render services) the learner also stores its memory there. The URL is a secret: it is read from the
environment only and never logged or sent to the UI.
"""
from __future__ import annotations

import os
import socket
import ssl
from typing import Optional
from urllib.parse import unquote, urlparse


class RedisKV:
    def __init__(self, url: str, timeout: float = 3.0):
        u = urlparse(url)
        if u.scheme not in ("redis", "rediss"):
            raise ValueError("unsupported key/value URL scheme")
        self.host, self.port = u.hostname or "localhost", u.port or 6379
        self.user = unquote(u.username) if u.username else None
        self.password = unquote(u.password) if u.password else None
        self.db = int((u.path or "/0").strip("/") or 0)
        self.tls = u.scheme == "rediss"
        self.timeout = timeout

    # ------------------------------------------------------------- RESP
    @staticmethod
    def _enc(*args) -> bytes:
        out = [b"*%d\r\n" % len(args)]
        for a in args:
            b = a if isinstance(a, bytes) else str(a).encode()
            out.append(b"$%d\r\n%s\r\n" % (len(b), b))
        return b"".join(out)

    @staticmethod
    def _read(f):
        line = f.readline()
        if not line:
            raise ConnectionError("key/value store closed the connection")
        t, rest = line[:1], line[1:].rstrip(b"\r\n")
        if t == b"+":
            return rest
        if t == b"-":
            raise RuntimeError("key/value store error: " + rest.decode(errors="replace")[:80])
        if t == b":":
            return int(rest)
        if t == b"$":
            n = int(rest)
            if n < 0:
                return None
            data = f.read(n + 2)
            return data[:-2]
        if t == b"*":
            return [RedisKV._read(f) for _ in range(int(rest))]
        raise RuntimeError("bad key/value reply")

    def _call(self, *cmds):
        sock = socket.create_connection((self.host, self.port), timeout=self.timeout)
        try:
            if self.tls:
                sock = ssl.create_default_context().wrap_socket(sock, server_hostname=self.host)
            f = sock.makefile("rb")
            pre = []
            if self.password:
                pre.append(("AUTH", self.user, self.password) if self.user else ("AUTH", self.password))
            if self.db:
                pre.append(("SELECT", self.db))
            out = None
            for c in pre + list(cmds):
                sock.sendall(self._enc(*c))
                out = self._read(f)
            return out
        finally:
            sock.close()

    def get(self, key: str) -> Optional[bytes]:
        return self._call(("GET", key))

    def set(self, key: str, value: bytes) -> None:
        self._call(("SET", key, value))


def from_env() -> Optional[RedisKV]:
    url = os.environ.get("RB_LEARN_REDIS_URL", "").strip()
    if not url:
        return None
    try:
        return RedisKV(url)
    except ValueError:
        return None
