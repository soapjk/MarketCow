"""One-time Live-only diagnostic activation after verified Paper pause."""
import argparse
import json
import os
import shutil
import sqlite3
import subprocess

from activate_u1_direct_rust import baseline, status, stop, switch
from prepare_u1_direct_rust_release import R, CONFIG, LIVE, DISCOVERY, API, sha, replace_one


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--paper-pause-receipt', required=True)
    parser.add_argument('--minimum-cursor', required=True, type=int)
    args = parser.parse_args()
    assert args.paper_pause_receipt.startswith('channel_message:')
    assert args.minimum_cursor > 9552018
    assert (R/'logs/public-send-diag-build-r1.exit-code').read_text().strip() == '0'
    expected = 'c55462ae4ebcabb1b39e02d8fe02f45f685421d60d1bb9419f4e028fbd8a4a88'
    source = R/'target/release/marketcow-discovery-collector'
    assert sha(source) == expected
    prior = R/'releases/direct-rust-resume-ac50b7e'
    release = R/'releases/direct-rust-send-diag-6b23ab9'
    assert (CONFIG/LIVE).resolve() == prior/'units'/LIVE
    assert not release.exists()
    untouched = {n: {'pid': status(n)['MainPID'], 'sha': sha(CONFIG/n)}
                 for n in (DISCOVERY, 'marketcow-polymarket-proxy.service', API)}
    os.umask(0o077)
    release.mkdir()
    (release/'units').mkdir()
    binary = release/'marketcow-discovery-collector'
    shutil.copyfile(source, binary)
    binary.chmod(0o500)
    body = replace_one((CONFIG/LIVE).read_text(), str(prior/'marketcow-discovery-collector'), str(binary))
    body = replace_one(body, 'direct-rust-resume-ac50b7e-marketcow-polymarket-collector.log',
                       'direct-rust-send-diag-6b23ab9-marketcow-polymarket-collector.log')
    (release/'units'/LIVE).write_text(body)
    (release/'units'/LIVE).chmod(0o400)
    report = {'binary_sha256': expected, 'source_commit': '6b23ab9',
              'paper_pause_receipt': args.paper_pause_receipt, 'minimum_cursor': args.minimum_cursor,
              'prior_unit_sha256': sha(CONFIG/LIVE), 'untouched': untouched, 'activated': False}
    subprocess.run(['systemd-analyze', '--user', 'verify', str(release/'units'/LIVE)], check=True)
    try:
        stop(LIVE)
        root = R/'bounded-scoped-candidate-r1'
        with sqlite3.connect(f'file:{root}/indexes/latest-state.sqlite3?mode=ro', uri=True) as db:
            state = dict(db.execute('SELECT * FROM metadata'))
            assert db.execute('PRAGMA quick_check').fetchone()[0] == 'ok'
        assert int(state['latest_cursor']) >= args.minimum_cursor
        assert int(state['recent_event_bytes']) <= int(state['bounded_history_bytes'])
        report['durable_before_start'] = state
        switch(LIVE, release/'units'/LIVE)
        subprocess.run(['systemctl', '--user', 'daemon-reload'], check=True)
        subprocess.run(['systemctl', '--user', 'start', LIVE], check=True)
        scope = '54b2ad555edfd47bd1863fc71c7bb6443ca57a50c7433644b78ef3a78a4e4a12'
        report['baseline'] = baseline('http://192.168.124.3:8793/v1/prediction-markets/polymarket/live/full-sync?scope_id='+scope, 250, True)
        assert report['baseline']['cursor'] >= int(state['latest_cursor'])
        for name, previous in untouched.items():
            assert status(name)['MainPID'] == previous['pid'] and sha(CONFIG/name) == previous['sha']
        report['activated'] = True
        report['live_unit'] = status(LIVE)
    except BaseException:
        subprocess.run(['systemctl', '--user', 'stop', LIVE], timeout=100, check=False)
        switch(LIVE, prior/'units'/LIVE)
        subprocess.run(['systemctl', '--user', 'daemon-reload'], check=True)
        subprocess.run(['systemctl', '--user', 'start', LIVE], check=True)
        raise
    finally:
        (release/'manifest.json').write_text(json.dumps(report, indent=2))
    print(json.dumps(report))


if __name__ == '__main__':
    main()
