"""Bounded same-host Rust scope control, never a market-data proxy.

Socket paths and budgets are operator configuration, not HTTP request fields.
A lost response is an unknown outcome: reconcile actual status, never retry a
publication automatically. No shell, process restart, account or order access.
"""
import hashlib
import json
import os
from pathlib import Path
import socket
import stat
import struct
import time


class RuntimeOutcomeUnknown(RuntimeError):
    pass


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate runtime response field")
        result[key] = value
    return result


class RustScopeClient:
    def __init__(self, *, socket_path: Path, maximum_bytes: int, timeout_seconds: float):
        if (not socket_path.is_absolute() or type(maximum_bytes) is not int
                or not 1024 <= maximum_bytes <= 16*1024*1024
                or type(timeout_seconds) not in (int, float) or not 0 < timeout_seconds <= 300):
            raise ValueError("explicit runtime socket/byte/deadline configuration required")
        self.path, self.maximum, self.timeout = socket_path, maximum_bytes, timeout_seconds

    def _check_socket(self):
        parent = self.path.parent
        directory, endpoint = parent.lstat(), self.path.lstat()
        if (parent.resolve(strict=True) != parent or not stat.S_ISDIR(directory.st_mode)
                or directory.st_uid != os.getuid() or directory.st_mode & 0o077
                or not stat.S_ISSOCK(endpoint.st_mode) or endpoint.st_uid != os.getuid()
                or endpoint.st_mode & 0o077):
            raise ValueError("runtime socket must have a private same-user owner")

    def request(self, command: dict) -> dict:
        body = canonical(command)
        if not body or len(body) > self.maximum:
            raise ValueError("runtime request byte capacity")
        self._check_socket()
        deadline = time.monotonic()+self.timeout
        sent = False
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
            def remaining():
                value = deadline-time.monotonic()
                if value <= 0:
                    raise TimeoutError("runtime command deadline")
                connection.settimeout(value)

            def exact(count):
                data = bytearray()
                while len(data) < count:
                    remaining()
                    chunk = connection.recv(count-len(data))
                    if not chunk:
                        raise EOFError("runtime response incomplete")
                    data.extend(chunk)
                return bytes(data)

            try:
                remaining(); connection.connect(str(self.path))
                remaining(); sent = True
                connection.sendall(struct.pack("!I", len(body))+body)
                count = struct.unpack("!I", exact(4))[0]
                if not 0 < count <= self.maximum:
                    raise ValueError("runtime response byte capacity")
                raw = exact(count)
                result = json.loads(raw, object_pairs_hook=_unique,
                                    parse_constant=lambda _: (_ for _ in ()).throw(ValueError("nonfinite JSON")))
                if canonical(result) != raw or type(result) is not dict or type(result.get("ok")) is not bool:
                    raise ValueError("invalid runtime response")
                if result["ok"]:
                    if set(result) != {"ok", "result"} or type(result["result"]) is not dict:
                        raise ValueError("invalid runtime result")
                    return result["result"]
                if set(result) != {"ok", "error"} or not isinstance(result["error"], str):
                    raise ValueError("invalid runtime error")
            except (OSError, EOFError, ValueError) as error:
                if sent:
                    raise RuntimeOutcomeUnknown("runtime outcome unknown; request_sha256="+hashlib.sha256(body).hexdigest()) from error
                raise
        raise ValueError("runtime rejected operation: "+result["error"])

    def status(self):
        return self.request({"operation": "status"})
