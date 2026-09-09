"""Offline, lossless selected subtrees; original full-sync SHA is retained."""
import hashlib
import json
from pathlib import Path
import sys

root = Path(sys.argv[1])
raw = (root / "fullsync.json").read_bytes()
v = json.loads(raw)
receipt = json.loads((root / "receipt.json").read_bytes())
assert hashlib.sha256(raw).hexdigest() == receipt["sha256"]
markets = [m for m in v["bootstrap"]["markets"] if m["identity"]["market_id"] in {"2587796", "2587798"}]
assert len(markets) == 2
tokens = {o["token_id"] for m in markets for o in m["identity"]["outcomes"] if o["outcome"] == "No"}
out = dict(source_receipt=receipt, derivation="exact subtrees, not original raw response",
           markets=markets, books={t: v["snapshot"]["books"][t] for t in sorted(tokens)},
           snapshot_metadata={k: x for k, x in v["snapshot"].items() if k not in {"books", "items"}},
           items=[m for m in v["snapshot"]["items"] if m.get("market_id") in {"2587796", "2587798"}],
           ready_observed=False)
body = json.dumps(out, ensure_ascii=False, indent=2).encode()
with (root / "ecb-selected-r2.json").open("xb") as f:
    f.write(body)
print("selected_bytes", len(body), "sha256", hashlib.sha256(body).hexdigest())
for m in markets:
    print(m["identity"]["market_id"], json.dumps(m["rules"]))
for t,b in out["books"].items():
    print(t,json.dumps({k:x for k,x in b.items() if k not in {'bids','asks','raw_payload'}}))
    print("best",b['bids'][:1], b['asks'][:1],"levels",len(b['bids']),len(b['asks']))
