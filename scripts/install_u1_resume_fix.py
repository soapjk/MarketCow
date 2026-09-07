"""Collector-only follow-up within the acknowledged Paper maintenance."""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess

from activate_u1_direct_rust import status, stop, switch
from prepare_u1_direct_rust_release import R, CONFIG, LIVE, DISCOVERY, API, sha, replace_one


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--binary-sha256', required=True)
    parser.add_argument('--paper-pause-receipt', required=True)
    args = parser.parse_args()
    assert args.paper_pause_receipt == 'channel_message:1620d799-d2f1-4540-898e-fff42cf8ca4a'
    assert (R/'logs/public-api-build-r6.exit-code').read_text().strip() == '0'
    assert sha(R/'target/release/marketcow-discovery-collector') == args.binary_sha256
    assert (CONFIG/API).resolve() == Path('/dev/null') and status(API)['MainPID'] == '0'
    prior = R/'releases/direct-rust-0424396'
    release = R/'releases/direct-rust-resume-ac50b7e'
    assert not release.exists()
    names = (LIVE, DISCOVERY)
    originals = {}
    for name in names:
        assert (CONFIG/name).resolve() == prior/'units'/name
        originals[name] = {'path': str(prior/'units'/name), 'sha256': sha(CONFIG/name)}
    os.umask(0o077)
    release.mkdir()
    (release/'units').mkdir()
    binary = release/'marketcow-discovery-collector'
    shutil.copyfile(R/'target/release/marketcow-discovery-collector',binary)
    binary.chmod(0o500)
    for name in names:
        body = replace_one((CONFIG/name).read_text(),str(prior/'marketcow-discovery-collector'),str(binary))
        body = replace_one(body, str(R/f'logs/direct-rust-0424396-{name.removesuffix(".service")}.log'),
            str(R/f'logs/direct-rust-resume-ac50b7e-{name.removesuffix(".service")}.log'))
        (release/'units'/name).write_text(body)
        (release/'units'/name).chmod(0o400)
    manifest = {'binary_sha256':sha(binary), 'previous_units':originals,
        'paper_pause_receipt':args.paper_pause_receipt, 'source_commit':'ac50b7e'}
    (release/'manifest.json').write_text(json.dumps(manifest,indent=2))
    subprocess.run(['systemd-analyze','--user','verify',*[str(release/'units'/n) for n in names]],check=True)
    proxy = status('marketcow-polymarket-proxy.service')['MainPID']
    assert proxy != '0'
    try:
        for name in names:
            stop(name)
        for name in names:
            switch(name,release/'units'/name)
        subprocess.run(['systemctl','--user','daemon-reload'],check=True)
        subprocess.run(['systemctl','--user','start',*names],check=True)
        assert status('marketcow-polymarket-proxy.service')['MainPID'] == proxy
    except BaseException:
        for name in names:
            subprocess.run(['systemctl','--user','stop',name],check=False,timeout=100)
            switch(name,prior/'units'/name)
        subprocess.run(['systemctl','--user','daemon-reload'],check=True)
        subprocess.run(['systemctl','--user','start',*names],check=True)
        raise
    print(json.dumps({'units':{n:status(n) for n in names}, 'manifest_sha256':sha(release/'manifest.json'),
        'http_ws_verified':False,'proxy_pid':proxy}))


if __name__ == '__main__':
    main()
