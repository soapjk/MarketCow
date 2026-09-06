"""Two explicit REST cycles over the existing 1000-market plan, no catalog fetch."""
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import time

from marketcow.polymarket_discovery import PolymarketDiscoveryStore
from marketcow.polymarket_live import PolymarketLiveReadStore

RUNTIME = Path('/mnt/p44pro/marketcow-shadow-v3-runtime/linux')
ROOT = RUNTIME / 'bounded-discovery-candidate-r1'


def main(report):
    store = PolymarketDiscoveryStore(PolymarketLiveReadStore(ROOT), depth_notionals=('10','50','100','500'), maximum_book_age_ms=5000)
    baseline = store.capture()
    report['before_cursor'] = baseline.boundary_cursor
    plan = ROOT / 'rust-source-plan.json'
    assert hashlib.sha256(plan.read_bytes()).hexdigest() == '655f5536841baa52a7c5680432aa621d2a14f786145a9ae0f1aae70a18f9a807'
    args = [str(RUNTIME / 'target/release/marketcow-discovery-collector'),
        '--root',str(ROOT),'--plan',str(plan),'--plan-sha256',hashlib.sha256(plan.read_bytes()).hexdigest(),
        '--input-mode','rest-poll','--expected-market-count','1000','--concurrency','16',
        '--request-market-batch-size','20','--response-byte-limit','2097152','--batch-byte-limit','16777216',
        '--persistence-queue-batches','256','--persistence-queue-bytes','67108864',
        '--bounded-history-bytes','67108864','--poll-seconds','1','--request-timeout-seconds','10','--cycles','2']
    env = dict(os.environ, HTTPS_PROXY='http://127.0.0.1:17890', HTTP_PROXY='http://127.0.0.1:17890', NO_PROXY='localhost,127.0.0.1')
    with (RUNTIME / 'logs/bounded-discovery-incremental-r1-collector.log').open('xb') as log:
        child = subprocess.Popen(args, env=env, stdout=log, stderr=subprocess.STDOUT)
        start = time.monotonic()
        try:
            while child.poll() is None:
                assert time.monotonic() - start < 240, 'collector timeout'
                current = store.capture()
                frame = store.events_page(baseline.projection_id, baseline.boundary_cursor, 1)
                if frame.items and not frame.resync_required:
                    assert frame.next_cursor == frame.after_cursor + 1
                    for item in frame.items:
                        if item.type == 'market_update':
                            assert item.payload.cursor == frame.next_cursor
                    if len(report['frames']) < 8:
                        report['frames'].append(frame.model_dump(mode='json'))
                    baseline = current
                time.sleep(1)
            assert child.returncode == 0, f'collector exit {child.returncode}'
            store.capture()
            full = store.full_sync()
            assert full.boundary_cursor > report['before_cursor']
            assert len(full.markets) == 1000
            assert report['frames'], 'no real typed delta verified'
            assert not (ROOT / 'events.jsonl').exists()
            with sqlite3.connect(f'file:{ROOT}/indexes/latest-state.sqlite3?mode=ro', uri=True) as db:
                state = dict(db.execute('SELECT * FROM metadata'))
                assert int(state['latest_cursor']) == full.boundary_cursor
                assert int(state['recent_event_bytes']) <= int(state['bounded_history_bytes'])
                assert db.execute('PRAGMA integrity_check').fetchone()[0] == 'ok'
            report.update(passed=True, after_cursor=full.boundary_cursor, market_count=1000,
                          elapsed_seconds=time.monotonic()-start, durable_state=state)
        finally:
            if child.poll() is None:
                child.terminate()
                try:
                    child.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait()
            store.stop_background_materialization()


if __name__ == '__main__':
    report = {'passed':False,'frames':[], 'transport':'explicit rest-poll, not websocket fallback',
              'evidence':'direct materializer contract, not HTTP/WS transport audit'}
    try:
        main(report)
    except BaseException as exc:
        report['error'] = repr(exc)
        raise
    finally:
        (RUNTIME / 'logs/bounded-discovery-incremental-r1-report.json').write_text(json.dumps(report,indent=2))
