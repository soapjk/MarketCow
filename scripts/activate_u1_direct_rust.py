"""Coordinated U1-only switch. Require an actual Paper pause receipt.

Retains all data roots; restores prior units on activation failure. No orders,
account access, enable operation, or proxy restart.
"""
import argparse
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import time
import urllib.request

from prepare_u1_direct_rust_release import R, RELEASE, CONFIG, LIVE, DISCOVERY, API, sha


def status(name):
    output = subprocess.check_output(['systemctl', '--user', 'show', name,
        '-p', 'MainPID', '-p', 'Result', '-p', 'ExecMainStatus', '-p', 'ActiveState'], text=True)
    return dict(line.split('=', 1) for line in output.splitlines())


def switch(name, target):
    pending = CONFIG/(name + '.direct-rust-pending')
    assert not pending.exists() and not pending.is_symlink()
    pending.symlink_to(target)
    os.replace(pending, CONFIG/name)


def stop(name):
    subprocess.run(['systemctl', '--user', 'stop', name], check=True, timeout=100)
    stopped = status(name)
    assert stopped['MainPID'] == '0' and stopped['Result'] == 'success' and stopped['ExecMainStatus'] == '0', stopped


def baseline(url, count, live):
    deadline = time.monotonic()+60
    while True:
        try:
            with urllib.request.urlopen(url, timeout=10) as response:
                raw = response.read(134217729)
            assert len(raw) <= 134217728
            value = json.loads(raw)
            assert len(value['bootstrap']['markets'] if live else value['markets']) == count
            return {'cursor': value['cursor'] if live else value['boundary_cursor'],
                'instance': value.get('stream_instance_id'), 'projection_id': value.get('projection_id'),
                'bytes': len(raw), 'catalog_revision': value['catalog_revision'],
                'scope_id': value.get('active_scope_id'), 'ready': value.get('ready')}
        except Exception:
            if time.monotonic() >= deadline:
                raise
            time.sleep(1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--manifest-sha256', required=True)
    parser.add_argument('--paper-pause-receipt', required=True)
    args = parser.parse_args()
    assert args.paper_pause_receipt.startswith('channel_message:')
    manifest_path = RELEASE/'manifest.json'
    assert sha(manifest_path) == args.manifest_sha256
    manifest = json.loads(manifest_path.read_text())
    assert manifest['prepared'] and not manifest['activated']
    assert sha(RELEASE/'marketcow-discovery-collector') == manifest['binary_sha256']
    assert sha(R/'bounded-discovery-candidate-r1/discovery-public-seed.json') == manifest['seed_sha256']
    report_path = R/'logs/direct-rust-0424396-activation.json'
    assert not report_path.exists()
    for name, binding in manifest['units'].items():
        assert str((CONFIG/name).resolve(strict=True)) == binding['path']
        assert sha(CONFIG/name) == binding['sha256']
        if name != API:
            assert sha(RELEASE/'units'/name) == binding['new_sha256']
    proxy = 'marketcow-polymarket-proxy.service'
    proxy_before = status(proxy)['MainPID']
    assert proxy_before != '0'
    report = {'activated': False, 'paper_pause_receipt': args.paper_pause_receipt,
        'manifest_sha256': args.manifest_sha256, 'durable_before_start': {}}
    try:
        stop(API)
        stop(LIVE)
        stop(DISCOVERY)
        for kind in ('scoped', 'discovery'):
            root = R/f'bounded-{kind}-candidate-r1'
            with sqlite3.connect(f'file:{root}/indexes/latest-state.sqlite3?mode=ro', uri=True) as db:
                metadata = dict(db.execute('SELECT * FROM metadata'))
                assert int(metadata['recent_event_bytes']) <= int(metadata['bounded_history_bytes'])
                assert db.execute('PRAGMA quick_check').fetchone()[0] == 'ok'
                report['durable_before_start'][kind] = metadata
        for name in (LIVE, DISCOVERY):
            switch(name, RELEASE/'units'/name)
        # Retire the actual Python API; no dummy active service or auto-restart.
        switch(API, Path('/dev/null'))
        subprocess.run(['systemctl', '--user', 'daemon-reload'], check=True)
        subprocess.run(['systemctl', '--user', 'start', LIVE, DISCOVERY], check=True)
        scope = '54b2ad555edfd47bd1863fc71c7bb6443ca57a50c7433644b78ef3a78a4e4a12'
        report['live'] = baseline(manifest['live_base']+'/v1/prediction-markets/polymarket/live/full-sync?scope_id='+scope,250,True)
        report['discovery'] = baseline(manifest['discovery_base']+'/v1/prediction-markets/polymarket/live/discovery/full-sync',1000,False)
        for kind, key in [('scoped','live'),('discovery','discovery')]:
            assert report[key]['cursor'] >= int(report['durable_before_start'][kind]['latest_cursor'])
        assert status(proxy)['MainPID'] == proxy_before
        report['units'] = {name: status(name) for name in (LIVE, DISCOVERY, API)}
        assert report['units'][API]['MainPID'] == '0'
        assert all(report['units'][name]['ActiveState'] == 'active' for name in (LIVE, DISCOVERY))
        report['activated'] = True
    except BaseException as error:
        report['error'] = repr(error)
        for name in (LIVE, DISCOVERY):
            subprocess.run(['systemctl', '--user', 'stop', name], timeout=100, check=False)
        for name, binding in manifest['units'].items():
            switch(name, Path(binding['path']))
        subprocess.run(['systemctl', '--user', 'daemon-reload'], check=True)
        subprocess.run(['systemctl', '--user', 'start', LIVE, DISCOVERY, API], check=True)
        report['rollback_units_restored'] = True
        raise
    finally:
        report_path.write_text(json.dumps(report, indent=2))
        print(json.dumps(report))


if __name__ == '__main__':
    main()
