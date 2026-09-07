"""Read bounded gap-maintenance fixture JSON from stdin; no service mutation.

Input: {initial_gaps: [...], operations: [{token_id, gaps: [...]}]}.
Compares the previous token-recovery gap algorithm to the indexed algorithm.
This is not full pipeline replay, a current baseline, or production CPU timing.
"""
import json
import statistics
import sys
import time

from marketcow.polymarket_contracts import GapEntry, content_sha256
from marketcow.polymarket_live_stream import _GapIndex


def main():
    payload = sys.stdin.buffer.read(4 * 1024 * 1024 + 1)
    if len(payload) > 4 * 1024 * 1024:
        raise ValueError('fixture exceeds 4 MiB')
    fixture = json.loads(payload)
    initial = [GapEntry.model_validate(g) for g in fixture['initial_gaps']]
    operations = [(o['token_id'], [GapEntry.model_validate(g) for g in o['gaps']])
                  for o in fixture['operations']]
    if len(initial) > 2048 or len(operations) > 100:
        raise ValueError('benchmark work budget exceeded')
    timings = {'old': [], 'new': []}
    for _ in range(3):
        old = [g.model_copy(deep=True) for g in initial]
        indexed = _GapIndex(initial)
        started = time.perf_counter()
        for token, gaps in operations:
            old = [g for g in old if g.token_id != token]
            for gap in gaps:
                identity = content_sha256(gap.model_dump(mode='json'))
                if all(content_sha256(g.model_dump(mode='json')) != identity for g in old):
                    old.append(gap.model_copy(deep=True))
        timings['old'].append(time.perf_counter() - started)
        started = time.perf_counter()
        for token, gaps in operations:
            indexed.remove_tokens((token,))
            for gap in gaps:
                indexed.add(gap)
        timings['new'].append(time.perf_counter() - started)
        assert old == list(indexed.by_id.values()), 'ordered gap facts changed'
    print(json.dumps({
        'fixture_sha256': content_sha256(fixture),
        'initial_gaps': len(initial), 'operations': len(operations),
        'ordered_final_equal': True, 'seconds': timings,
        'median_seconds': {k: statistics.median(v) for k, v in timings.items()},
        'measurement': 'isolated gap maintenance wall time; excludes initialization, full apply, locks and live scheduling',
    }))


if __name__ == '__main__':
    main()
