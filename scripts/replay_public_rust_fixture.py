"""Read-only consumer replay; caller supplies the existing Tradude workspace.

Run with Tradude's Python and PYTHONPATH pointing to that workspace. No Paper
engine, account, network request or service operation is performed.
"""
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path

from domains.prediction_markets.marketcow.configured_live import ConfiguredLiveState


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--full-sync', type=Path, required=True)
    parser.add_argument('--full-sync-sha256', required=True)
    parser.add_argument('--wire', type=Path, required=True)
    parser.add_argument('--wire-sha256', required=True)
    args = parser.parse_args()
    for path, digest in [(args.full_sync, args.full_sync_sha256), (args.wire, args.wire_sha256)]:
        with path.open('rb') as stream:
            assert hashlib.file_digest(stream, 'sha256').hexdigest() == digest
    full = json.loads(args.full_sync.read_bytes())
    state = ConfiguredLiveState(full, scope_id=full['scope_id'], market_ids=tuple(full['scope_market_ids']))
    counts, rejected = Counter(), Counter()
    with args.wire.open('rb') as stream:
        for raw in stream:
            frame = json.loads(raw)
            result = state.apply(frame)
            counts[frame['type']] += 1
            rejected.update(reason for _, reason in result.rejected_confirmations)
    print(json.dumps({'full_sync_sha256': args.full_sync_sha256, 'wire_sha256': args.wire_sha256,
        'scope_count': len(full['scope_market_ids']), 'counts': counts,
        'local_confirmation_rejections': rejected, 'cursor': state.cursor,
        'global_protocol_errors': 0, 'paper_executed': False}))


if __name__ == '__main__':
    main()
