"""One bounded existing Live full-sync; no subscription or scope mutation."""
import hashlib
import json
from pathlib import Path
import sys
import time
import urllib.request

root = Path(sys.argv[1])
assert root.is_absolute()
root.mkdir()
base = "http://192.168.124.3:8793/v1/prediction-markets/polymarket/live"
with urllib.request.urlopen(base + "/health", timeout=10) as response:
    health_raw = response.read(65537)
assert len(health_raw) <= 65536
health = json.loads(health_raw)
assert {"2587796", "2587798"} <= set(health["scope_market_ids"])
url = base + "/full-sync?scope_id=" + health["scope_id"]
started = time.time_ns()
deadline = time.monotonic() + 30
raw = bytearray()
with urllib.request.urlopen(url, timeout=10) as response:
    while True:
        if time.monotonic() >= deadline:
            raise TimeoutError("total body deadline")
        chunk = response.read(min(65536, 67108865 - len(raw)))
        if not chunk:
            break
        raw.extend(chunk)
        if len(raw) > 67108864:
            raise ValueError("64MiB cap exceeded (one detection byte)")
received = time.time_ns()
v = json.loads(raw)
(root / "fullsync.json").write_bytes(raw)
(root / "health.json").write_bytes(health_raw)
(root / "receipt.json").write_text(json.dumps(dict(
    url=url, started_at_ns=started, received_at_ns=received,
    bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest(),
    scope_id=health["scope_id"], cursor=v.get("cursor"),
    stream_instance_id=v.get("stream_instance_id"),
    ready_observed=False, note="Full-sync observation only, no WS/ready or execution eligibility"
), indent=2))
print((root / "receipt.json").read_text())
