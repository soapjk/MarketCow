"""Deploy the user-selected frozen main commit, preserving roots and rollback."""
import argparse
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import subprocess

from activate_u1_direct_rust import baseline, status, stop, switch
from prepare_u1_direct_rust_release import R, CONFIG, LIVE, DISCOVERY, API, sha

COMMIT = '0c49df887ed9e6f56183d1290cfb2e307fff64bb'
SOURCE = Path('/mnt/p44pro/projects/marketcow-main-0c49df887ed9')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--binary-sha256', required=True)
    parser.add_argument('--minimum-live-cursor', type=int, required=True)
    args = parser.parse_args()
    assert args.minimum_live_cursor >= 24841853
    assert (R/'logs/main-0c49df8-build-r2.exit-code').read_text().strip() == '0'
    binary_source = R/'target/release/marketcow-discovery-collector'
    assert sha(binary_source) == args.binary_sha256
    release = R/'releases/main-0c49df887ed9'
    assert not release.exists()
    previous = {name: (CONFIG/name).resolve(strict=True) for name in (LIVE, DISCOVERY)}
    assert all(path.is_relative_to(R/'releases') for path in previous.values())
    proxy = 'marketcow-polymarket-proxy.service'
    proxy_pid = status(proxy)['MainPID']
    assert proxy_pid != '0' and status(API)['MainPID'] == '0'
    os.umask(0o077)
    release.mkdir()
    (release/'units').mkdir()
    (release/'previous-units').mkdir()
    binary = release/'marketcow-discovery-collector'
    shutil.copyfile(binary_source, binary)
    binary.chmod(0o500)
    report = {'source_commit': COMMIT, 'binary_sha256': sha(binary),
              'activated': False, 'minimum_live_cursor': args.minimum_live_cursor,
              'previous': {}, 'durable_before_start': {}}
    for name, path in previous.items():
        text = path.read_text()
        matches = re.findall(r'/mnt/p44pro/[^\s]+/marketcow-discovery-collector(?=\s)', text)
        assert len(matches) == 1
        body = text.replace(matches[0], str(binary), 1)
        logs = re.findall(r'(?<=--log )\S+', body)
        assert len(logs) == 1
        body = body.replace(logs[0], str(R/f'logs/main-0c49df887ed9-{name[:-8]}.log'), 1)
        assert body.replace(str(binary), matches[0], 1).replace(
            str(R/f'logs/main-0c49df887ed9-{name[:-8]}.log'), logs[0], 1) == text
        shutil.copyfile(path, release/'previous-units'/name)
        (release/'units'/name).write_text(body)
        (release/'units'/name).chmod(0o400)
        report['previous'][name] = {'path': str(path), 'sha256': sha(path)}
    subprocess.run(['systemd-analyze', '--user', 'verify',
                    str(release/'units'/LIVE), str(release/'units'/DISCOVERY)], check=True)
    def stop_for_switch(name):
        # A previously failed unit can report exit-code=1 even though it is
        # fully stopped; that state must not block a rollback-safe switch.
        subprocess.run(['systemctl', '--user', 'stop', name], check=True, timeout=100)
        state = status(name)
        assert state['MainPID'] == '0', state

    try:
        stop_for_switch(LIVE)
        stop_for_switch(DISCOVERY)
        for kind in ('scoped', 'discovery'):
            root = R/f'bounded-{kind}-candidate-r1'
            with sqlite3.connect(f'file:{root}/indexes/latest-state.sqlite3?mode=ro', uri=True) as db:
                assert db.execute('PRAGMA quick_check').fetchone()[0] == 'ok'
                metadata = dict(db.execute('SELECT * FROM metadata'))
            assert int(metadata['recent_event_bytes']) <= int(metadata['bounded_history_bytes'])
            if kind == 'scoped':
                assert int(metadata['latest_cursor']) >= args.minimum_live_cursor
            report['durable_before_start'][kind] = metadata
        for name in previous:
            switch(name, release/'units'/name)
        subprocess.run(['systemctl', '--user', 'daemon-reload'], check=True)
        subprocess.run(['systemctl', '--user', 'start', LIVE, DISCOVERY], check=True)
        scope = '54b2ad555edfd47bd1863fc71c7bb6443ca57a50c7433644b78ef3a78a4e4a12'
        report['live'] = baseline('http://192.168.124.3:8793/v1/prediction-markets/polymarket/live/full-sync?scope_id='+scope, 250, True)
        report['discovery'] = baseline('http://192.168.124.3:8795/v1/prediction-markets/polymarket/live/discovery/full-sync', 1000, False)
        for kind, key in [('scoped', 'live'), ('discovery', 'discovery')]:
            assert report[key]['cursor'] >= int(report['durable_before_start'][kind]['latest_cursor'])
        assert report['live']['scope_id'] == scope
        assert status(proxy)['MainPID'] == proxy_pid and status(API)['MainPID'] == '0'
        report['units'] = {name: status(name) for name in previous}
        assert all(value['ActiveState'] == 'active' for value in report['units'].values())
        report['activated'] = True
    except BaseException as error:
        report['error'] = repr(error)
        for name in previous:
            subprocess.run(['systemctl', '--user', 'stop', name], timeout=100, check=False)
            switch(name, previous[name])
        subprocess.run(['systemctl', '--user', 'daemon-reload'], check=True)
        subprocess.run(['systemctl', '--user', 'start', LIVE, DISCOVERY], check=True)
        report['rollback_units_restored'] = True
        raise
    finally:
        (release/'manifest.json').write_text(json.dumps(report, indent=2))
        print(json.dumps(report))


if __name__ == '__main__':
    main()
