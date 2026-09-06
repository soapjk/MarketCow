#!/usr/bin/env python3
"""Offline prepared-root registration for an exact, content-addressed selection.

Uses the existing verified relocation tool. No Gamma traversal or Python collector.
The candidate stays unpublished until the activation endpoint verifies live readiness.
"""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys
from datetime import datetime

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from relocate_polymarket_live_source import relocate, digest
from marketcow.polymarket_configured_scope import PolymarketConfiguredScope
from marketcow.polymarket_live import LiveMarket
from marketcow.polymarket_scopes import _atomic_create, _atomic_replace
from marketcow.polymarket_contracts import canonical_json


MAX_DEPENDENCY_MARKETS = 1024


def prepare(*, source, target, scope_path, storage, registry, bridge_port, resume=False,
            in_place_existing=False):
    source=source.resolve(strict=True); storage=storage.resolve(strict=True)
    target=target.resolve(); registry=registry.resolve(strict=True)
    if not source.is_relative_to(storage) or not target.is_relative_to(storage) or not registry.is_relative_to(storage):
        raise ValueError('all prepared files must remain inside isolated storage')
    scope=PolymarketConfiguredScope.model_validate_json(scope_path.read_bytes())
    if not 1 <= scope.configured_market_count <= 250: raise ValueError('configured scope must contain 1..250 exact markets')
    if not 1024<=bridge_port<=65535 or bridge_port in (8790,8793,17890): raise ValueError('invalid bridge port')
    if in_place_existing != (source == target):
        raise ValueError('in-place preparation requires identical source and target')
    with (source/'.collector.lock').open('a') as lease:
        fcntl.flock(lease,fcntl.LOCK_EX|fcntl.LOCK_NB)
        if in_place_existing:
            manifest = json.loads((target/'catalog.json').read_bytes())
            paths = [manifest['normalized_catalog']['path'],
                     manifest['catalog_index']['path'],
                     manifest['candidate_snapshot']['path'],
                     manifest['catalog_source']['raw_path']]
            if any(not Path(value).resolve(strict=True).is_relative_to(target)
                   for value in paths):
                raise ValueError('existing candidate catalog escapes its root')
        else:
            relocate(source,target,source,source/'configured-scope.json',resume=resume)
    manifest=json.loads((target/'catalog.json').read_bytes())
    if manifest['catalog_revision'] != scope.catalog_revision: raise ValueError('selection catalog differs')
    pending={m.market_id for m in scope.configured_markets}
    with sqlite3.connect(f'file:{target}/indexes/latest-state.sqlite3?mode=ro',uri=True) as state:
        pending.update(row[0] for row in state.execute('select distinct market_id from books'))
        metadata=dict(state.execute('select key,value from metadata'))
        if int(metadata['event_log_size']) != (target/'events.jsonl').stat().st_size:
            raise ValueError('candidate needs authoritative tail recovery before registration')
    markets={}
    catalog=Path(manifest['normalized_catalog']['path']); index=Path(manifest['catalog_index']['path'])
    with sqlite3.connect(f'file:{index}?mode=ro',uri=True) as db, catalog.open('rb') as stream:
        while pending:
            mid=min(pending); pending.remove(mid)
            if mid in markets: continue
            if len(markets)>=MAX_DEPENDENCY_MARKETS:
                raise ValueError('dependency hydration exceeds 1024 market budget')
            row=db.execute('select byte_offset,byte_length,row_sha256 from markets where market_id=?',(mid,)).fetchone()
            if row is None: raise ValueError('missing catalog dependency:'+mid)
            offset,size,sha=row
            if not 0<size<=1048576: raise ValueError('catalog row byte cap')
            stream.seek(offset); raw=stream.read(size)
            if hashlib.sha256(raw).hexdigest()!=sha: raise ValueError('catalog row hash differs')
            market=LiveMarket.model_validate_json(raw)
            if market.identity.market_id != mid: raise ValueError('catalog row identity differs')
            markets[mid]=market
            for relation in market.relations:
                pending.update(pair.market_id for pair in relation.outcome_pairs if pair.market_id not in markets)
    for chosen in scope.configured_markets:
        market=markets[chosen.market_id]
        if chosen.condition_id != market.identity.condition_id or set(chosen.token_ids)!={o.token_id for o in market.identity.outcomes} or datetime.fromisoformat(chosen.end_at.replace('Z','+00:00')) != market.end_at:
            raise ValueError('selection identity differs:'+chosen.market_id)
    scope_body=scope.model_dump(mode='json')
    _atomic_replace(target/'configured-scope.json',scope_body)
    _atomic_replace(target/'scope-runtime.json',{'schema_version':'marketcow.polymarket.scope-runtime.v1',
        'scope_id':scope.active_scope_id,'manifest_sha256':digest(target/'configured-scope.json')})
    _atomic_replace(target/'rust-scoped-plan-r1.json',{
        'schema_version':'marketcow.polymarket.rust-scoped-source-plan.v1','catalog_revision':scope.catalog_revision,
        'markets':[{'market_id':m.market_id,'condition_id':m.condition_id,'token_ids':list(m.token_ids)} for m in scope.configured_markets]})
    _atomic_replace(target/'live-bridge-plan-r1.json',{'schema_version':'marketcow.polymarket.live-bridge-plan.v1',
        'catalog_revision':scope.catalog_revision,'scope_id':scope.active_scope_id,
        'catalog_manifest_sha256':digest(target/'catalog.json'),'catalog_source':manifest['catalog_source'],
        'markets':[markets[mid].model_dump(mode='json') for mid in sorted(markets)]})
    artifact={'schema_version':'marketcow.polymarket.managed-live-generation.v1','scope_id':scope.active_scope_id,
        'root':str(target.relative_to(storage)),'configured_scope_sha256':digest(target/'configured-scope.json'),
        'collector_plan_sha256':digest(target/'rust-scoped-plan-r1.json'),
        'dependency_plan_sha256':digest(target/'live-bridge-plan-r1.json'),'bridge_port':bridge_port}
    report={'complete':True,'ready':False,'configured_market_count':scope.configured_market_count,
        'in_place_existing':in_place_existing,
        'hydrated_market_count':len(markets),'dependency_market_limit':MAX_DEPENDENCY_MARKETS,
        'artifact':artifact,'scope_file_sha256':hashlib.sha256(canonical_json(artifact)).hexdigest()}
    _atomic_replace(target/'dynamic-preparation-report.json',report)
    _atomic_create(registry/(scope.active_scope_id+'.json'),canonical_json(artifact))
    print(json.dumps(report))


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    for name in ['source','target','scope-path','storage','registry']: parser.add_argument('--'+name,type=Path,required=True)
    parser.add_argument('--bridge-port',type=int,required=True)
    parser.add_argument('--resume',action='store_true')
    parser.add_argument('--in-place-existing',action='store_true')
    os.umask(0o077)
    prepare(**vars(parser.parse_args()))
