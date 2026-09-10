"""Finite synthetic blocked-disk publication measurement, no network clients."""
import argparse
import hashlib
import json
import resource
import statistics
import sys
import threading
import time
import tracemalloc
from pathlib import Path

from .btc_fact_log import FactLog, read_page
from .btc_hourly_dataset import canonical


def measure(root, count):
    if not 2 <= count <= 10000:
        raise ValueError("count_budget")
    root.mkdir(parents=True, exist_ok=False)
    gate, entered = threading.Event(), threading.Event()
    def block():
        entered.set()
        if not gate.wait(30):
            raise TimeoutError("benchmark_disk_gate")
    log = FactLog(root / "facts", maximum_pending=count, maximum_pending_bytes=count*2048,
                  maximum_disk_bytes=count*2048, maximum_subscribers=2, before_write=block)
    slow = log.subscribe(count=1, size=2048)
    fast = log.subscribe(count=2, size=4096)
    latencies = []
    report = {"synthetic": True, "count": count, "network": False, "error": None}
    tracemalloc.start()
    baseline_memory = tracemalloc.get_traced_memory()[0]
    try:
        for index in range(count):
            started = time.perf_counter_ns()
            sequence = log.publish({"index": index, "payload": "x"*1024})
            item = json.loads(fast.receive())
            latencies.append(time.perf_counter_ns()-started)
            if sequence != index+1 or item["sequence"] != sequence:
                raise ValueError("delivery_order")
            if index == 0 and not entered.wait(2):
                raise TimeoutError("writer_not_entered")
        state = log.status()
        if state["durable"] != 0 or state["pending_count"] != count or slow.error != "slow_consumer":
            raise ValueError("blocked_disk_invariant")
        try:
            log.publish({"overflow": True})
        except RuntimeError as exc:
            if str(exc) != "persistence_capacity":
                raise
        else:
            raise ValueError("capacity_not_enforced")
        current, peak = tracemalloc.get_traced_memory()
        ordered = sorted(latencies)
        report.update(blocked_state=state, fast_received=count, slow_error=slow.error,
                      latency_ns={"median": statistics.median(ordered),
                                  "p95": ordered[int((count-1)*.95)], "max": max(ordered)},
                      traced_memory_growth_bytes=current-baseline_memory, traced_peak_bytes=peak,
                      process_peak_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*(1 if sys.platform == "darwin" else 1024),
                      measurement="publish through same-thread subscriber receive and JSON parse; wall time, not CPU or network")
    except Exception as exc:
        report["error"] = type(exc).__name__+":"+str(exc)
    finally:
        tracemalloc.stop()
        gate.set()
        started = time.monotonic()
        try:
            log.close(30)
        except Exception as exc:
            report["error"] = report["error"] or type(exc).__name__+":"+str(exc)
        report["drain_wall_seconds"] = time.monotonic()-started
        report["final_state"] = log.status()
    if report["error"] is None:
        after = 0
        while after < count:
            page = read_page(root / "facts", after=after, through=count, limit=100, maximum_bytes=204800)
            if not page:
                raise ValueError("empty_committed_page")
            after = page[-1]["sequence"]
        report["verified_committed_count"] = after
    report["code_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    (root / "report.json").write_bytes(canonical(report))
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--count", type=int, required=True)
    args = parser.parse_args()
    report = measure(args.output, args.count)
    print(canonical(report).decode())
    if report["error"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
