import os
import socket
import struct
import threading
import tempfile
from pathlib import Path

import pytest

from marketcow.universe_rust_control import RustScopeClient, RuntimeOutcomeUnknown, canonical


@pytest.fixture
def socket_root():
    # macOS sockaddr_un cannot hold pytest's long default temporary paths.
    with tempfile.TemporaryDirectory(prefix="mc-scope-", dir="/tmp") as directory:
        yield Path(directory).resolve()


def exchange(tmp_path, response, *, claimed_size=None):
    tmp_path.chmod(0o700)
    path = tmp_path/"runtime.sock"
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(str(path)); path.chmod(0o600); server.listen(1); server.settimeout(2)
    seen = []

    def run():
        with server, server.accept()[0] as connection:
            connection.settimeout(2)
            header = connection.recv(4)
            count = struct.unpack("!I", header)[0]
            body = bytearray()
            while len(body) < count:
                body.extend(connection.recv(count-len(body)))
            seen.append(bytes(body))
            if response is not None:
                connection.sendall(struct.pack("!I", len(response) if claimed_size is None else claimed_size))
                connection.sendall(response)
    thread = threading.Thread(target=run)
    thread.start()
    return RustScopeClient(socket_path=path, maximum_bytes=4096, timeout_seconds=1), thread, seen


def test_actual_unix_status_and_canonical_request(socket_root):
    client, thread, seen = exchange(socket_root, canonical({"ok": True, "result": {"revision": 7}}))
    assert client.status() == {"revision": 7}
    thread.join(2); assert not thread.is_alive()
    assert seen == [b'{"operation":"status"}']


@pytest.mark.parametrize("raw,size", [(None, None), (b"{}", 4097), (b'{"ok":true,"ok":true}', None)])
def test_lost_or_invalid_response_is_not_automatic_retry(socket_root, raw, size):
    client, thread, seen = exchange(socket_root, raw, claimed_size=size)
    with pytest.raises(RuntimeOutcomeUnknown):
        client.request({"operation": "publish_scope"})
    thread.join(2); assert not thread.is_alive(); assert len(seen) == 1


def test_explicit_rejection_is_not_success(socket_root):
    client, thread, _ = exchange(socket_root, canonical({"ok": False, "error": "scope revision conflict"}))
    with pytest.raises(ValueError, match="scope revision conflict"):
        client.status()
    thread.join(2); assert not thread.is_alive()


def test_no_public_socket_or_implicit_budgets(tmp_path):
    with pytest.raises(ValueError):
        RustScopeClient(socket_path=tmp_path/"socket", maximum_bytes=0, timeout_seconds=1)
    path = tmp_path/"not-socket"; path.write_bytes(b""); os.chmod(path, 0o600)
    client = RustScopeClient(socket_path=path, maximum_bytes=4096, timeout_seconds=1)
    with pytest.raises(ValueError, match="private same-user"):
        client.status()
