"""Offline empty-state preparation from already audited scoped startup records.

This does not fetch data or synthesize books. Old supplier raw files are not
recreated: the receipt explicitly records their absence after an operator reset.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path

from marketcow.polymarket_configured_scope import PolymarketConfiguredScope
from marketcow.polymarket_contracts import canonical_json
from marketcow.polymarket_live import LiveMarket, LiveStateIndex


def read_verified(path, expected):
    raw = path.read_bytes()
    if len(raw) > 32 * 1024 * 1024 or hashlib.sha256(raw).hexdigest() != expected:
        raise ValueError(f"startup input hash or size differs: {path.name}")
    return json.loads(raw)


def create(path, body):
    with path.open('xb') as stream:
        stream.write(canonical_json(body))
        stream.flush()
        os.fsync(stream.fileno())


def prepare(root, plan_sha256, scope_sha256, dependency_sha256):
    root = root.resolve(strict=True)
    if root.stat().st_mode & 0o077:
        raise ValueError('preparation root must be private (mode 0700)')
    plan_path = root / 'rust-scoped-plan-r1.json'
    scope_path = root / 'configured-scope.json'
    dependency_path = root / 'live-bridge-plan-r1.json'
    if any((root / name).exists() for name in
           ('catalog.json', 'scope-runtime.json', 'events.jsonl',
            'state-index.json', 'indexes', 'runtime-scope-state.json')):
        raise ValueError('empty-state preparation requires an uninitialized root')
    plan = read_verified(plan_path, plan_sha256)
    scope_body = read_verified(scope_path, scope_sha256)
    dependency = read_verified(dependency_path, dependency_sha256)
    scope = PolymarketConfiguredScope.model_validate(scope_body)
    if (plan['schema_version'] != 'marketcow.polymarket.rust-scoped-source-plan.v1'
            or dependency['schema_version'] != 'marketcow.polymarket.live-bridge-plan.v1'
            or not plan['catalog_revision'] == scope.catalog_revision == dependency['catalog_revision']
            or scope.active_scope_id != dependency['scope_id']):
        raise ValueError('startup catalog/scope identity differs')
    records = {m.identity.market_id: m for m in
               (LiveMarket.model_validate(value) for value in dependency['markets'])}
    if len(records) != len(dependency['markets']) or not 1 <= len(records) <= 1024:
        raise ValueError('dependency identity or preparation budget')
    selected = {m.market_id: m for m in scope.configured_markets}
    if len(selected) != len(plan['markets']):
        raise ValueError('startup plan count differs')
    for item in plan['markets']:
        mid = item['market_id']
        record, chosen = records[mid], selected[mid]
        tokens = {o.token_id for o in record.identity.outcomes}
        if (item['condition_id'] != chosen.condition_id
                or chosen.condition_id != record.identity.condition_id
                or set(item['token_ids']) != set(chosen.token_ids)
                or set(chosen.token_ids) != tokens):
            raise ValueError('startup market identity differs')
    # A new local preparation manifest binds preserved normalized records. It
    # makes no assertion that deleted historical Gamma raw bytes still exist.
    manifest = {
        'schema_version': 'marketcow.polymarket.empty-scoped-preparation.v1',
        'catalog_revision': plan['catalog_revision'],
        'catalog_source': dependency['catalog_source'],
        'preserved_dependency_sha256': dependency_sha256,
        'historical_supplier_raw_available': False,
        'realtime_universe': {'market_ids': sorted(selected)},
    }
    create(root / 'catalog.json', manifest)
    create(root / 'scope-runtime.json', {
        'schema_version': 'marketcow.polymarket.scope-runtime.v1',
        'scope_id': scope.active_scope_id, 'manifest_sha256': scope_sha256,
    })
    with (root / 'events.jsonl').open('xb') as stream:
        stream.flush()
        os.fsync(stream.fileno())
    index = LiveStateIndex(root)
    index.rebuild(event_path=root / 'events.jsonl', books={}, gaps=[],
                  catalog_revision=plan['catalog_revision'],
                  token_to_market={}, verified_event_offsets=[])
    index.close()
    dependency['catalog_manifest_sha256'] = hashlib.sha256((root / 'catalog.json').read_bytes()).hexdigest()
    replacement = root / 'live-bridge-plan-r1.prepared.json'
    create(replacement, dependency)
    replacement.replace(dependency_path)
    receipt = {
        'schema_version': 'marketcow.polymarket.empty-scoped-preparation-receipt.v1',
        'catalog_revision': plan['catalog_revision'], 'scope_id': scope.active_scope_id,
        'market_count': len(selected), 'dependency_market_count': len(records),
        'published_cursor': 0, 'durable_cursor': 0, 'book_count': 0,
        'historical_supplier_raw_available': False,
        'original_dependency_sha256': dependency_sha256,
        'dependency_sha256': hashlib.sha256(dependency_path.read_bytes()).hexdigest(),
        'catalog_manifest_sha256': dependency['catalog_manifest_sha256'],
    }
    create(root / 'empty-preparation-receipt.json', receipt)
    descriptor = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return receipt


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    for name in ('plan-sha256', 'scope-sha256', 'dependency-sha256'):
        parser.add_argument('--' + name, required=True)
    os.umask(0o077)
    print(json.dumps(prepare(**vars(parser.parse_args())), sort_keys=True))
