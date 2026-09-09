"""Four-request, read-only official history probe; retains bounded raw evidence."""
import argparse
import hashlib
import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path


def utc():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def history_window(start_text, end_text, now):
    """Explicit past UTC seconds only; never derive a window from endDate."""
    values = []
    for value in (start_text, end_text):
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", value):
            raise ValueError("history_window_requires_UTC_seconds")
        values.append(datetime.fromisoformat(value.replace("Z", "+00:00")))
    start, end = values
    if not start < end <= now or end - start > timedelta(days=7):
        raise ValueError("history_window_must_be_past_positive_at_most_7_days")
    return start, end


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def frozen_identity(path, expected_sha):
    with path.open("rb") as source:
        raw = source.read(65537)
    if len(raw) > 65536 or hashlib.sha256(raw).hexdigest() != expected_sha:
        raise ValueError("frozen_identity_hash_or_size")
    envelope = json.loads(raw)
    evidence = envelope["evidence"]
    payload = evidence["raw_response"].encode()
    if hashlib.sha256(payload).hexdigest() != evidence["raw_response_sha256"]:
        raise ValueError("frozen_raw_hash")
    market = json.loads(payload, parse_float=str)
    if market["id"] != "1088482" or evidence["market_id"] != market["id"]:
        raise ValueError("frozen_market_identity")
    tokens = json.loads(market["clobTokenIds"])
    outcomes = json.loads(market["outcomes"])
    pairs = [(p["token_id"], p["outcome"]) for p in evidence["settlement"]["payouts"]]
    if len(tokens) != 2 or len(outcomes) != 2 or sorted(zip(tokens, outcomes)) != sorted(pairs):
        raise ValueError("frozen_token_binding")
    return market, {"path": str(path.resolve()), "sha256": expected_sha,
                    "raw_response_sha256": evidence["raw_response_sha256"],
                    "capture_observed_at": evidence["observed_at"],
                    "source_url": evidence["source_url"],
                    "historical_rules_verified": False, "label_available_at": None}


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--start-utc", required=True)
    parser.add_argument("--end-utc", required=True)
    parser.add_argument("--research-type", required=True,
                        choices=("availability_probe", "single_match"))
    parser.add_argument("--frozen-identity", type=Path)
    parser.add_argument("--frozen-identity-sha256")
    args = parser.parse_args(argv)
    if args.research_type != "availability_probe":
        raise ValueError("research_type_mismatch: championship candidate is not single_match")
    captured_clock = datetime.now(timezone.utc)
    start, end = history_window(args.start_utc, args.end_utc, captured_clock)
    if bool(args.frozen_identity) != bool(args.frozen_identity_sha256):
        raise ValueError("frozen_identity_requires_path_and_hash")
    frozen = frozen_identity(args.frozen_identity, args.frozen_identity_sha256) if args.frozen_identity else None
    code_sha = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    args.output.mkdir(parents=True, exist_ok=False)
    report = {"started_at": utc(), "budget": {"requests": 4, "bytes": 1048576,
              "request_seconds": 15, "total_seconds": 70, "history_days": 7,
              "retries": 0}, "requests": []}
    if frozen:
        report["budget"]["requests"] = 2
        report["frozen_identity"] = frozen[1]
    report.update(code_sha256=code_sha, configuration={
                  "research_type": args.research_type, "fidelity_minutes": 60,
                  "environment_proxy_enabled": True, "proxy_policy": "system_environment",
                  "direct_fallback_on_proxy_failure": False, "redirects": False,
                  "user_agent": "urllib_default", "output": str(args.output.resolve())},
                  window={"start_utc": args.start_utc,
                  "end_utc": args.end_utc, "validated_at": captured_clock.isoformat(),
                  "derived_from_market_end": False},
                  sample={"market_id": "1088482", "category": "championship",
                          "category_evidence": "peer prior evidence; not independently verified",
                          "single_match": False, "paired_label_verified": False})
    started = time.monotonic()
    remaining = 1048576
    opener = urllib.request.build_opener(urllib.request.ProxyHandler(), NoRedirect())

    def get(label, url):
        nonlocal remaining
        if datetime.now(timezone.utc) < captured_clock:
            raise RuntimeError("wall_clock_regression")
        if len(report["requests"]) >= report["budget"]["requests"] or remaining <= 0:
            raise RuntimeError("request_or_byte_budget")
        now = time.monotonic()
        if started + 70 - now <= 0:
            raise TimeoutError("total_deadline_before_request")
        deadline = min(started + 70, now + 15)
        item = {"label": label, "url": url, "started_at": utc()}
        report["requests"].append(item)
        chunks = []
        complete = False
        try:
            request = urllib.request.Request(url, headers={"Accept-Encoding": "identity"})
            try:
                timeout = deadline - time.monotonic()
                if timeout <= 0:
                    raise TimeoutError("deadline_before_open")
                response = opener.open(request, timeout=timeout)
            except urllib.error.HTTPError as error:
                response = error
            with response:
                item["status"] = response.status
                item["headers"] = {key: response.headers.get(key) for key in
                                   ("Date", "Content-Type", "Content-Length", "CF-RAY")}
                while remaining:
                    if time.monotonic() >= deadline:
                        raise TimeoutError("deadline")
                    chunk = response.read1(min(16384, remaining))
                    if not chunk:
                        complete = True
                        break
                    chunks.append(chunk)
                    remaining -= len(chunk)
                if remaining == 0:
                    item["stop_reason"] = "byte_budget_eof_unverified"
        except Exception as error:
            # Transport errors can include proxy URLs or credentials; retain type only.
            item["error"] = type(error).__name__
        raw = b"".join(chunks)
        path = args.output / (label + ".raw.json")
        path.write_bytes(raw)
        item.update(received_at=utc(), raw_complete=complete, bytes=len(raw),
                    raw_path=str(path.resolve()), raw_sha256=hashlib.sha256(raw).hexdigest())
        if not complete or item.get("status") != 200:
            raise RuntimeError("incomplete_or_http_error")
        value = json.loads(raw, parse_float=str)
        canonical = json.dumps(value, sort_keys=True, separators=(",", ":"),
                               ensure_ascii=False).encode()
        item["canonical_sha256"] = hashlib.sha256(canonical).hexdigest()
        return value

    try:
        market = frozen[0] if frozen else get("gamma-1088482", "https://gamma-api.polymarket.com/markets/1088482")
        if market.get("id") != "1088482":
            raise RuntimeError("market_identity_mismatch")
        condition = market["conditionId"]
        if not isinstance(condition, str) or not re.fullmatch(r"0x[0-9a-fA-F]{64}", condition):
            raise RuntimeError("condition_identity_invalid")
        clob = None if frozen else get("clob-market", "https://clob.polymarket.com/markets/" + condition)
        report["pairing"] = {"market_id": market["id"], "condition_id": condition,
                             "question": market.get("question"), "scheduled_end": market["endDate"],
                             "gamma_resolution_status": market.get("umaResolutionStatus"),
                             "clob_condition_id": clob.get("condition_id") if clob else None,
                             "tokens": clob.get("tokens") if clob else None, "label_available_at": None}
        tokens = json.loads(market["clobTokenIds"])
        if (not isinstance(tokens, list) or len(tokens) != 2
                or any(not isinstance(t, str) or not t.isascii() or not t.isdecimal() for t in tokens)
                or len(set(tokens)) != 2 or (clob is not None and (
                    len(clob["tokens"]) != 2 or clob.get("condition_id") != condition
                    or set(tokens) != {t["token_id"] for t in clob["tokens"]}))):
            raise RuntimeError("identity_mismatch")
        for index, token in enumerate(tokens):
            history_window(args.start_utc, args.end_utc, datetime.now(timezone.utc))
            query = urllib.parse.urlencode({"market": token, "startTs": int(start.timestamp()),
                                            "endTs": int(end.timestamp()), "fidelity": 60})
            history = get("history-" + str(index), "https://clob.polymarket.com/prices-history?" + query)
            report["requests"][-1]["history_count"] = len(history.get("history", []))
    except Exception as error:
        report["error"] = type(error).__name__ + ": " + str(error)
    report.update(ended_at=utc(), elapsed_seconds=time.monotonic()-started,
                  total_bytes=1048576-remaining,
                  canonical_profile="json parse_float=str; sorted keys; compact UTF-8; not JCS")
    output = args.output / "report.json"
    output.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")
    print(json.dumps(report, ensure_ascii=False))
    print("report_sha256=" + hashlib.sha256(output.read_bytes()).hexdigest())


if __name__ == "__main__":
    main()
