#!/usr/bin/env python3
"""Explicit one-market identity migration; no supplier request or service restart.

Reads one retained manager original, preserves its opaque legacy ID, and stores
the same unverified RulesRecord projection as source_btc5m_manager. No hashes.
Requires generated bindings for the already frozen v7 contract.
"""
import argparse
import base64
import json
from pathlib import Path
import shlex
import subprocess
import sys
from urllib import parse, request

ROOT = '/mnt/p44pro/marketcow-shadow-v3-runtime/linux/bounded-scoped-candidate-r2-20261002'
NAMESPACE = 'polymarket.btc5m.rules'


def checked_original(raw, market_id, condition_id, slug, old_id):
    if len(raw) > 1024 * 1024:
        raise ValueError('manager_original_capacity')
    original = json.loads(raw)
    if original['schema_version'] != 'marketcow.polymarket.btc-5m-manager-evidence.v1':
        raise ValueError('manager_schema')
    gamma_raw = base64.b64decode(original['gamma']['raw_base64'], validate=True)
    execution_raw = base64.b64decode(original['execution']['raw_base64'], validate=True)
    if len(gamma_raw) != original['gamma']['bytes'] or len(execution_raw) != original['execution']['bytes']:
        raise ValueError('manager_original_length')
    g, e, r = json.loads(gamma_raw), json.loads(execution_raw), original['record']
    outcomes, tokens = json.loads(g['outcomes']), json.loads(g['clobTokenIds'])
    expected = [{'outcome': name, 'token_id': token} for name, token in zip(outcomes, tokens)]
    if (g['id'] != market_id or g['conditionId'] != condition_id or g['slug'] != slug
            or original['slug'] != slug or original['rule_sha256'] != old_id
            or r['rules']['rule_version'] != old_id or outcomes != ['Up', 'Down']
            or len(tokens) != 2 or tokens[0] == tokens[1]
            or r['identity']['market_id'] != market_id or r['identity']['condition_id'] != condition_id
            or r['identity']['outcomes'] != expected or e['c'] != condition_id
            or e['t'] != [{'o': name, 't': token} for name, token in zip(outcomes, tokens)]):
        raise ValueError('manager_identity_conflict')
    from datetime import datetime, timezone
    start = datetime.fromtimestamp(int(slug.rsplit('-', 1)[1]), timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    end = datetime.fromtimestamp(int(slug.rsplit('-', 1)[1]) + 300, timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    if (r['start_at'] != start or r['end_at'] != end or g['endDate'] != end
            or g['eventStartTime'] != start or not g['description']
            or g['resolutionSource'] != 'https://data.chain.link/streams/btc-usd-twap-60s-streams'):
        raise ValueError('manager_rule_binding_conflict')
    return dict(market_id=market_id, condition_id=condition_id, window_id=slug,
                start_at=start, end_at=end, up_token_id=tokens[0], down_token_id=tokens[1],
                description=g['description'], resolution_source=g['resolutionSource'])


def remote(host, code, payload):
    result = subprocess.run(['ssh', host, 'python3 -c ' + shlex.quote(code)],
                            input=json.dumps(payload).encode(), stdout=subprocess.PIPE, check=True)
    return json.loads(result.stdout)


READ = '''import sys,json,base64
from pathlib import Path
p=json.load(sys.stdin); root=Path(p['root'])/'btc-5m-manager-r1'; f=Path(p['original'])
assert f.parent==root and f.is_file() and not f.is_symlink() and f.stat().st_size<=1048576
print(json.dumps({'raw':base64.b64encode(f.read_bytes()).decode()}))
'''

APPLY = '''import sys,json,sqlite3,base64,uuid
from pathlib import Path
p=json.load(sys.stdin); root=Path(p['root']); original=Path(p['original'])
assert original.parent==root/'btc-5m-manager-r1' and original.is_file() and not original.is_symlink()
assert original.read_bytes()==base64.b64decode(p['original_bytes'],validate=True), 'original_changed'
path=root/'typed-identities.sqlite3'; assert path.is_file() and not path.is_symlink()
db=sqlite3.connect('file:'+str(path)+'?mode=rw',uri=True,timeout=3)
db.execute('PRAGMA synchronous=FULL'); db.execute('BEGIN IMMEDIATE')
cols=['market_id','condition_id','window_id','start_at','end_at','up_token_id','down_token_id','description','resolution_source']
values=tuple(p['fields'][c] for c in cols)+(base64.b64decode(p['resolution']),base64.b64decode(p['evidence']))
rows=db.execute('SELECT * FROM typed_rules WHERE market_id=? ORDER BY revision',(p['fields']['market_id'],)).fetchall()
if rows:
 assert len(rows)==1 and tuple(rows[0][2:])==values, 'existing_rules_conflict'
 identity=(rows[0][0],rows[0][1])
else:
 identity=(str(uuid.uuid4()),1)
 db.execute('INSERT INTO typed_rules(rules_id,revision,'+','.join(cols)+',resolution_proto,evidence_proto) VALUES ('+','.join('?' for _ in range(13))+')',identity+values)
prior=db.execute('SELECT record_id,revision FROM typed_legacy_identity WHERE namespace=? AND legacy_id=?',(p['namespace'],p['old_id'])).fetchone()
assert prior is None or prior==identity, 'legacy_mapping_conflict'
if prior is None:
 db.execute('INSERT INTO typed_legacy_identity VALUES (?,?,?,?)',(p['namespace'],p['old_id'],*identity))
db.commit()
print(json.dumps({'rules_id':identity[0],'revision':identity[1],'namespace':p['namespace'],'legacy_id':p['old_id'],'original':str(original),'database':str(path),'committed':True}))
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--host', default='czx@u1')
    parser.add_argument('--proto-python', required=True)
    parser.add_argument('--original', required=True)
    parser.add_argument('--market-id', required=True)
    parser.add_argument('--condition-id', required=True)
    parser.add_argument('--slug', required=True)
    parser.add_argument('--legacy-id', required=True)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    sys.path.insert(0, args.proto_python)
    from marketcow.polymarket import btc5m_market_local_v7_pb2 as proto
    original = remote(args.host, READ, {'root': ROOT, 'original': args.original})
    raw = base64.b64decode(original['raw'], validate=True)
    fields = checked_original(raw, args.market_id, args.condition_id, args.slug, args.legacy_id)
    record = proto.RulesRecord(**fields, resolution=proto.ResolutionTerms(
        verification='unverified', source_instrument='BTCUSD.CHAINLINK.TWAP60', quote_currency='USD',
        price_method='twap', averaging_seconds=60, provenance='official_rule'))
    if not args.apply:
        print(json.dumps({'validated': True, 'apply': False, 'fields': fields}))
        return
    receipt = remote(args.host, APPLY, dict(root=ROOT, original=args.original,
        original_bytes=original['raw'], namespace=NAMESPACE, old_id=args.legacy_id, fields=fields,
        resolution=base64.b64encode(proto.RulesRecord(resolution=record.resolution).SerializeToString()).decode(),
        evidence=base64.b64encode(proto.RulesRecord().SerializeToString()).decode()))
    record.identity.id = receipt['rules_id']
    record.identity.revision = receipt['revision']
    url = 'http://192.168.124.3:8793/v7/polymarket/btc-5m/rules?' + parse.urlencode(
        {'rules_id': receipt['rules_id'], 'revision': receipt['revision']})
    with request.urlopen(url, timeout=10) as response:
        assert response.headers.get_content_type() == 'application/x-protobuf'
        restored = proto.RulesRecord.FromString(response.read(1024 * 1024))
    if restored != record:
        raise ValueError('post_commit_http_rules_mismatch')
    receipt['http_rules_verified'] = True
    receipt['key'] = {k: v for k, v in fields.items() if k not in ('description', 'resolution_source')}
    receipt['key']['rules'] = {'id': receipt['rules_id'], 'revision': receipt['revision']}
    print(json.dumps(receipt))


if __name__ == '__main__':
    main()
