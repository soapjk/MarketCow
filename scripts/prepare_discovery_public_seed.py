"""Export validated local Discovery metadata; no catalog traversal or network.

Reads one transaction in an existing verified materialization. The target is
an already isolated bounded source root; publication refuses overwrite.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import sqlite3

from marketcow.polymarket_contracts import content_sha256
from marketcow.polymarket_discovery import DiscoveryMetadataFact, DiscoveryRelation, PolymarketDiscoveryStore
from marketcow.polymarket_live import LiveMarket


def prepare(source: Path, target: Path, quantities: list[str], age_ms: int, *, allow_same_root=False):
    source, target = source.resolve(strict=True), target.resolve(strict=True)
    assert source != target or allow_same_root, 'same-root preparation requires explicit opt-in'
    output = target / 'discovery-public-seed.json'
    stage = output.with_suffix('.preparing')
    assert not output.exists() and not stage.exists()
    manifest = json.loads((source/'discovery-materialized-v3/current.json').read_bytes())
    db_path = Path(manifest['database_path']).resolve(strict=True)
    assert db_path.is_relative_to(source)
    catalog_raw = (target/'catalog.json').read_bytes()
    catalog = json.loads(catalog_raw)
    universe = dict(catalog['realtime_universe'])
    revision = universe.pop('universe_id')
    assert content_sha256(universe) == revision == manifest['realtime_universe_id']
    assert catalog['catalog_revision'] == manifest['catalog_revision']
    ids = set(universe['market_ids'])
    assert len(ids) == universe['market_count']
    markets, relations, settlements = [], [], {}
    with sqlite3.connect(f'file:{db_path}?mode=ro', uri=True) as db:
        db.execute('BEGIN')
        for mid, static, metadata in db.execute('SELECT market_id,static_payload,metadata_payload FROM markets ORDER BY market_id'):
            assert mid in ids
            market = LiveMarket.model_validate_json(static)
            assert market.identity.market_id == mid
            fact = DiscoveryMetadataFact.model_validate_json(metadata)
            assert fact.market_id == mid
            markets.append(market.model_dump(mode='json'))
            settlement = PolymarketDiscoveryStore._settlement_from_metadata(fact)
            settlements[mid] = settlement.model_dump(mode='json') if settlement else None
        for body, in db.execute('SELECT payload_json FROM relations ORDER BY relation_id'):
            relation = DiscoveryRelation.model_validate_json(body)
            assert relation.catalog_revision == catalog['catalog_revision']
            relations.append(relation.model_dump(mode='json'))
    assert len(markets) == len(ids) and set(settlements) == ids
    # Validate the explicit policy before publishing it.
    from marketcow.polymarket_discovery import _decimal_config
    normalized = list(_decimal_config(quantities))
    assert age_ms > 0
    seed = {'schema_version': 'marketcow.polymarket.discovery-public-seed.v1',
        'catalog_revision': catalog['catalog_revision'], 'catalog_manifest_sha256': hashlib.sha256(catalog_raw).hexdigest(),
        'universe_revision': revision, 'markets': markets, 'relations': relations,
        'settlements': settlements, 'catalog_source': catalog['catalog_source'],
        'depth_quantities': normalized, 'maximum_book_age_ms': age_ms}
    raw = json.dumps(seed, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()
    assert len(raw) <= 134217728
    with stage.open('xb') as stream:
        stream.write(raw); stream.flush(); os.fsync(stream.fileno())
    stage.rename(output)
    fd = os.open(target, os.O_RDONLY)
    try: os.fsync(fd)
    finally: os.close(fd)
    print(json.dumps({'complete': True, 'path': str(output), 'sha256': hashlib.sha256(raw).hexdigest(),
        'markets': len(markets), 'relations': len(relations), 'settlements_present': sum(v is not None for v in settlements.values())}))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--source-root', required=True, type=Path)
    parser.add_argument('--target-root', required=True, type=Path)
    parser.add_argument('--depth-quantities', required=True, nargs='+')
    parser.add_argument('--maximum-book-age-ms', required=True, type=int)
    parser.add_argument('--allow-same-root', action='store_true', help='Only create a new seed; never overwrite catalog/state')
    args = parser.parse_args()
    prepare(args.source_root, args.target_root, args.depth_quantities, args.maximum_book_age_ms, allow_same_root=args.allow_same_root)
