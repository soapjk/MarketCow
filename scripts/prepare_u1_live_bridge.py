"""Offline indexed hydration of frozen scope plus required relation dependencies."""
import hashlib
import json
import os
from pathlib import Path
import sqlite3

from marketcow.polymarket_configured_scope import PolymarketConfiguredScope
from marketcow.polymarket_live import LiveMarket

os.umask(0o077)
root = Path('/mnt/p44pro/marketcow-shadow-v3-runtime/linux/scoped-live-source-r1')
manifest_bytes = (root / 'catalog.json').read_bytes()
manifest = json.loads(manifest_bytes)
scope = PolymarketConfiguredScope.model_validate_json((root / 'configured-scope.json').read_bytes())
assert scope.catalog_revision == manifest['catalog_revision']
paths = []
for field in ['normalized_catalog', 'catalog_index']:
    entry = manifest[field]
    path = Path(entry['path']).resolve(strict=True)
    assert path.is_relative_to(root)
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(chunk)
    assert h.hexdigest() == entry['sha256']
    paths.append(path)
with sqlite3.connect(f'file:{root}/indexes/latest-state.sqlite3?mode=ro', uri=True) as state:
    book_market_ids = {row[0] for row in state.execute('select distinct market_id from books')}
    book_tokens = {row[0] for row in state.execute('select token_id from books')}
pending = book_market_ids | {m.market_id for m in scope.configured_markets}
markets = {}
with sqlite3.connect(f'file:{paths[1]}?mode=ro', uri=True) as index, paths[0].open('rb') as stream:
    while pending:
        market_id = min(pending)
        pending.remove(market_id)
        if market_id in markets:
            continue
        if len(markets) >= 512:
            raise ValueError('dependency hydration exceeds explicit 512-market safety budget')
        row = index.execute('select byte_offset,byte_length,row_sha256 from markets where market_id=?', (market_id,)).fetchone()
        if row is None:
            raise ValueError('missing required dependency market: ' + market_id)
        offset, size, checksum = row
        assert 0 < size <= 1024 * 1024
        stream.seek(offset)
        raw = stream.read(size)
        assert hashlib.sha256(raw).hexdigest() == checksum and stream.read(1) == b'\n'
        market = LiveMarket.model_validate_json(raw)
        assert market.identity.market_id == market_id
        markets[market_id] = market
        for relation in market.relations:
            for pair in relation.outcome_pairs:
                if pair.market_id not in markets:
                    pending.add(pair.market_id)
plan = {'schema_version': 'marketcow.polymarket.live-bridge-plan.v1',
        'catalog_revision': scope.catalog_revision, 'scope_id': scope.active_scope_id,
        'catalog_manifest_sha256': hashlib.sha256(manifest_bytes).hexdigest(),
        'catalog_source': manifest['catalog_source'],
        'markets': [markets[key].model_dump(mode='json') for key in sorted(markets)]}
raw = json.dumps(plan, sort_keys=True, separators=(',', ':')).encode()
assert len(raw) <= 16 * 1024 * 1024
with (root / 'live-bridge-plan-r1.json').open('xb') as stream:
    stream.write(raw)
    stream.flush()
    os.fsync(stream.fileno())
report = {'complete': True, 'configured_market_count': len(scope.configured_markets),
          'hydrated_market_count': len(markets), 'book_token_count': len(book_tokens),
          'plan_sha256': hashlib.sha256(raw).hexdigest(),
          'missing_book_tokens': sorted({o.token_id for m in markets.values() for o in m.identity.outcomes} - book_tokens)}
with (root / 'live-bridge-preparation-r1.json').open('x') as stream:
    json.dump(report, stream, indent=2)
    stream.flush()
    os.fsync(stream.fileno())
print(json.dumps(report))
