"""Research client: MarketCow only; official transport belongs to Rust."""
import argparse
import base64
import hashlib
import json
from pathlib import Path
import urllib.parse
import urllib.request

PATH = "/v1/prediction-markets/polymarket/history/prices"
WIRE_CAP = 2 * 1024 * 1024


def verify_evidence(body, request):
    evidence = json.loads(body)
    if (evidence.get("schema_version") != "marketcow.polymarket.price-history-evidence.v1"
            or evidence.get("request") != request or evidence.get("raw_complete") is not True):
        raise ValueError("evidence identity or completeness mismatch")
    raw = base64.b64decode(evidence["raw_base64"], validate=True)
    if (len(raw) > 1024 * 1024 or len(raw) != evidence["raw_bytes"]
            or hashlib.sha256(raw).hexdigest() != evidence["raw_sha256"]):
        raise ValueError("raw evidence integrity mismatch")
    return evidence, raw


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--token-id", required=True)
    parser.add_argument("--start-ts", required=True, type=int)
    parser.add_argument("--end-ts", required=True, type=int)
    parser.add_argument("--fidelity-minutes", required=True, type=int)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    base = urllib.parse.urlsplit(args.base_url)
    if (base.scheme not in ("http", "https") or not base.hostname or base.username
            or base.password or base.query or base.fragment or base.path not in ("", "/")):
        raise ValueError("explicit MarketCow origin required")
    request = {"token_id": args.token_id, "start_ts": args.start_ts,
               "end_ts": args.end_ts, "fidelity_minutes": args.fidelity_minutes}
    args.output.mkdir(parents=False, exist_ok=False)
    # This connection is to the explicitly configured MarketCow origin, not upstream.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
    url = args.base_url.rstrip("/") + PATH + "?" + urllib.parse.urlencode(request)
    with opener.open(url, timeout=20) as response:
        body = response.read(WIRE_CAP + 1)
    if len(body) > WIRE_CAP:
        raise ValueError("MarketCow response exceeds cap (one detection byte read)")
    (args.output / "marketcow-response.json").write_bytes(body)
    evidence, raw = verify_evidence(body, request)
    (args.output / "upstream.raw").write_bytes(raw)
    print(json.dumps({"upstream_status": evidence["upstream_status"],
                      "raw_sha256": evidence["raw_sha256"], "bytes": len(raw)}))
    if evidence["upstream_status"] != 200:
        raise SystemExit("upstream rejected request; evidence retained; no retry")


if __name__ == "__main__":
    main()
