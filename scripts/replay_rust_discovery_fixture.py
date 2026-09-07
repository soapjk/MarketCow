"""Replay byte-preserved Rust Discovery fixtures through unmodified Tradude.

Run with Tradude's interpreter and PYTHONPATH. No network or Paper engine.
"""
import argparse
import hashlib
import json
from pathlib import Path

from domains.prediction_markets.marketcow.discovery_v2 import MarketCowDiscoveryClient


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--full-sync', required=True, type=Path)
    parser.add_argument('--full-sync-sha256', required=True)
    parser.add_argument('--wire', required=True, type=Path)
    parser.add_argument('--wire-sha256', required=True)
    args = parser.parse_args()
    for path, expected in [(args.full_sync, args.full_sync_sha256), (args.wire, args.wire_sha256)]:
        with path.open('rb') as stream:
            assert hashlib.file_digest(stream, 'sha256').hexdigest() == expected
    baseline_raw = json.loads(args.full_sync.read_bytes())
    with MarketCowDiscoveryClient(base_url='http://127.0.0.1:1', timeout_seconds=1,
            maximum_full_sync_bytes=67108864, maximum_stream_frame_bytes=16777216) as client:
        # Replace only the transport; strict consumer decoding remains unchanged.
        client._get_json = lambda *a, **kw: baseline_raw
        baseline = client.fetch_full_sync()
        cursor = baseline.boundary_cursor
        count = 0
        with args.wire.open() as stream:
            for line in stream:
                frame = client._parse_stream_frame(line, after_cursor=cursor)
                assert frame.projection_id == baseline.projection_id
                assert frame.catalog_revision == baseline.catalog_revision
                assert frame.universe_revision == baseline.universe_revision
                assert not frame.resync_required
                assert frame.after_cursor == cursor and frame.next_cursor == cursor + 1
                cursor = frame.next_cursor
                count += 1
        print(json.dumps({'markets': len(baseline.markets), 'relations': len(baseline.relations),
            'ready': baseline.ready, 'gaps': baseline.unresolved_gap_count, 'frames': count,
            'initial_cursor': baseline.boundary_cursor, 'final_cursor': cursor}))


if __name__ == '__main__':
    main()
