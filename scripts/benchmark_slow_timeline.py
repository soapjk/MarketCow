"""Synthetic instrumentation-only costs; not end-to-end decode overhead."""
import json
import time
from pathlib import Path
from marketcow.polymarket_stream_metrics import StreamMetrics, timeline

report = []
for repeat in range(5):
    row = {}
    stages = ('socket_send', 'json_encode') if repeat % 2 == 0 else ('json_encode', 'socket_send')
    for stage in stages:
        metrics = StreamMetrics('benchmark')
        started = time.perf_counter()
        for _ in range(50000):
            metrics.record(stage, .00001)
        row[stage + '_us_per_record'] = (time.perf_counter()-started)/50000*1e6
    started = time.perf_counter()
    for _ in range(8):
        now = time.perf_counter()
        timeline().overlap(now-.002, now)
    row['eight_slow_scans_ms'] = (time.perf_counter()-started)*1000
    report.append(row)
path = Path('/mnt/p44pro/marketcow-shadow-v3-runtime/linux/logs/slow-timeline-overhead-r1.json')
with path.open('x') as output:
    json.dump({'synthetic': True, 'runs': report, 'does_not_measure_full_decode_callback_overhead': True}, output)
print(json.dumps(report))
