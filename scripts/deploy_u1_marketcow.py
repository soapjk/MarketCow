#!/usr/bin/env python3
"""Sync, verify, build and start the U1 collector with one retained version.

Usage: python3 scripts/deploy_u1_marketcow.py
Only user-owned U1 is contacted. Runtime data and service arguments are retained.
"""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time

ROOT = Path('/mnt/p44pro/marketcow-shadow-v3-runtime/linux')
UNIT = 'marketcow-polymarket-collector.service'
DEPLOY = ROOT / 'deployment'


def run(argv, **kwargs):
    return subprocess.run(argv, check=True, **kwargs)


def sha(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def remove_owned(path, parent):
    if path.parent != parent or path.is_symlink():
        raise RuntimeError(f'unsafe cleanup target: {path}')
    if path.exists():
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()


def source_files(repo):
    names = {'Cargo.toml', 'Cargo.lock', 'scripts/run_with_bounded_log.py',
             'scripts/deploy_u1_marketcow.py',
             'docs/architecture/migration/domain-ownership.yaml'}
    for path in (repo / 'crates').rglob('*'):
        if path.is_file() and (path.suffix in ('.rs', '.json') or path.name == 'Cargo.toml'):
            names.add(path.relative_to(repo).as_posix())
    for path in (repo / 'proto').rglob('*.proto'):
        if path.is_file():
            names.add(path.relative_to(repo).as_posix())
    for path in (repo / 'tests/fixtures').iterdir():
        if path.suffix in ('.json', '.jsonl'):
            names.add(path.relative_to(repo).as_posix())
    for name in sorted(names):
        path = repo / name
        if not path.is_file() or path.is_symlink():
            raise RuntimeError(f'missing or linked build input: {name}')
        yield name


def local(args):
    repo = Path(__file__).resolve().parents[1]
    t9 = Path('/Volumes/T9')
    if (not t9.is_mount() or t9.stat().st_dev == Path('/private/tmp').stat().st_dev
            or shutil.disk_usage(t9).free < 2 * 1024**3):
        raise RuntimeError('T9 must be mounted on its own filesystem with 2GiB free')
    scratch = t9 / '.corptie/tmp/marketcow-deploy'
    scratch.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='sync-', dir=scratch) as directory:
        stage = Path(directory)
        names = list(source_files(repo))
        manifest = {name: sha(repo / name) for name in names}
        (stage / 'manifest.json').write_text(json.dumps(manifest, sort_keys=True))
        archive = stage / 'source.tar.gz'
        with tarfile.open(archive, 'w:gz', format=tarfile.USTAR_FORMAT) as tar:
            for name in names:
                tar.add(repo / name, arcname=name, recursive=False)
        ssh = ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10', args.host]
        run([*ssh, f'mkdir -p {DEPLOY}/incoming && chmod 700 {DEPLOY} {DEPLOY}/incoming'])
        run(['scp', '-q', str(archive), str(stage / 'manifest.json'),
             str(repo / 'scripts/deploy_u1_marketcow.py'), f'{args.host}:{DEPLOY}/incoming/'])
        replay_option = (f' --memory-replay-bytes {args.memory_replay_bytes}'
                         if args.memory_replay_bytes is not None else '')
        persistence_option = (f' --persistence-queue-batches {args.persistence_queue_batches}'
                              f' --persistence-queue-bytes {args.persistence_queue_bytes}'
                              if args.persistence_queue_batches is not None else '')
        chainlink_option = (f' --btc-chainlink-credentials '
                            f'{shlex.quote(args.btc_chainlink_credentials)}'
                            if args.btc_chainlink_credentials is not None else '')
        btc_5m_option = f' --btc-5m-clients {args.btc_5m_clients}'
        polynode_option = (' --disable-btc-polynode' if args.disable_btc_polynode else
                           f' --btc-polynode-key-file {shlex.quote(args.btc_polynode_key_file)}')
        uk_ws_option = ' --uk-polymarket-ws-route' if args.uk_polymarket_ws_route else ''
        quick_option = ' --quick' if args.quick else ''
        run([*ssh, f'python3 {DEPLOY}/incoming/deploy_u1_marketcow.py '
             f'--remote --archive-sha256 {sha(archive)} --jobs {args.jobs}'
             f'{replay_option}{persistence_option}{chainlink_option}{btc_5m_option}'
             f'{polynode_option}'
             f'{uk_ws_option}'
             f'{quick_option}'])


def unpack_verified(archive, manifest, destination):
    if destination.exists():
        raise RuntimeError('source staging already exists')
    destination.mkdir(mode=0o700)
    with tarfile.open(archive) as tar:
        entries = tar.getmembers()
        if len(entries) != len(manifest) or len({e.name for e in entries}) != len(entries):
            raise RuntimeError('archive member count or uniqueness differs')
        for entry in entries:
            if (not entry.isfile() or entry.name not in manifest
                    or Path(entry.name).is_absolute() or '..' in Path(entry.name).parts):
                raise RuntimeError('archive contains an unexpected or unsafe member')
            target = destination / entry.name
            target.parent.mkdir(parents=True, exist_ok=True)
            with tar.extractfile(entry) as src, target.open('xb') as dst:
                shutil.copyfileobj(src, dst)
            target.chmod(0o600)
            if sha(target) != manifest[entry.name]:
                raise RuntimeError(f'source checksum differs: {entry.name}')


def referenced_releases():
    """Preserve the current code/config of other services, never their targets."""
    release_root = ROOT / 'releases'
    protected = set()
    texts = []
    for path in (Path.home() / '.config/systemd/user').glob('marketcow*'):
        if path.is_file():
            texts.append(path.read_text())
    # Service configuration may reference another operator/profile beneath a
    # release. Follow those local JSON references without printing their contents.
    seen = set()
    for text in texts:
        for value in re.findall(re.escape(str(release_root)) + r'/[^\s"\\]+', text):
            candidate = Path(value)
            relative = candidate.relative_to(release_root)
            if relative.parts:
                protected.add(relative.parts[0])
            if candidate.suffix == '.json' and candidate.is_file() and candidate not in seen:
                seen.add(candidate)
                texts.append(candidate.read_text())
    return protected


def cleanup_previous_versions():
    removed = []
    releases = ROOT / 'releases'
    protected = referenced_releases()
    for path in sorted(releases.iterdir()):
        if not path.is_dir() or path.is_symlink():
            continue
        if path.name not in protected:
            remove_owned(path, releases)
            removed.append(str(path))
        else:
            for child in path.iterdir():
                if child.name in ('source', 'source-complete', 'target') or child.name.startswith('target-'):
                    remove_owned(child, path)
                    removed.append(str(child))
    # These are exclusively prior isolated build inputs/outputs, not runtime roots.
    remove_owned(ROOT / 'source-builds', ROOT)
    removed.append(str(ROOT / 'source-builds'))
    for path in ROOT.glob('source-market-local-*.tar.gz'):
        remove_owned(path, ROOT)
        removed.append(str(path))
    for path in ROOT.glob('source-market-local-*.sums'):
        remove_owned(path, ROOT)
        removed.append(str(path))
    previous_build = Path('/mnt/p44pro/marketcow-market-local-r3-build')
    if previous_build.exists():
        remove_owned(previous_build, Path('/mnt/p44pro'))
        removed.append(str(previous_build))
    return removed


def rewrite_unit(unit, current, memory_replay_bytes=None,
                 persistence_queue_batches=None, persistence_queue_bytes=None,
                 btc_chainlink_credentials=None, btc_5m_clients=None,
                 btc_polynode_key_file=None, disable_polynode=False):
    matches = re.findall(r'/[^\s\\]+/marketcow-discovery-collector', unit)
    if len(matches) != 1:
        raise RuntimeError('expected exactly one collector executable in formal unit')
    updated = unit.replace(matches[0], str(current / 'marketcow-discovery-collector'))
    wrapper = re.findall(r'/[^\s\\]+/run_with_bounded_log.py', updated)
    if len(wrapper) != 1:
        raise RuntimeError('expected exactly one bounded log wrapper')
    updated = updated.replace(wrapper[0], str(current / 'run_with_bounded_log.py'))
    if memory_replay_bytes is not None:
        setting = re.findall(r'--memory-replay-bytes\s+\d+', updated)
        if len(setting) != 1:
            raise RuntimeError('expected exactly one memory replay byte limit')
        updated = updated.replace(setting[0], f'--memory-replay-bytes {memory_replay_bytes}')
    if persistence_queue_batches is not None:
        for name, value in (('batches', persistence_queue_batches),
                            ('bytes', persistence_queue_bytes)):
            setting = re.findall(r'--persistence-queue-' + name + r'\s+\d+', updated)
            if len(setting) != 1:
                raise RuntimeError(f'expected exactly one persistence queue {name} limit')
            updated = updated.replace(setting[0], f'--persistence-queue-{name} {value}')
    if btc_5m_clients is not None:
        setting = re.findall(r'--btc-5m-clients\s+\d+', updated)
        if len(setting) != 1:
            raise RuntimeError('expected exactly one BTC 5m WS client limit')
        updated = updated.replace(setting[0], f'--btc-5m-clients {btc_5m_clients}')
    if btc_chainlink_credentials is not None:
        option = f'--btc-chainlink-credentials {btc_chainlink_credentials}'
        existing = re.findall(r'--btc-chainlink-credentials\s+[^\s\\]+', updated)
        if len(existing) > 1:
            raise RuntimeError('multiple Chainlink credential options in formal unit')
        if existing:
            updated = updated.replace(existing[0], option)
        else:
            marker = '--btc-research-root '
            if updated.count(marker) != 1:
                raise RuntimeError('expected one BTC research component in formal unit')
            updated = updated.replace(marker, option + ' ' + marker)
    if disable_polynode:
        existing = re.findall(r'--btc-polynode-key-file\s+[^\s\\]+', updated)
        if len(existing) > 1:
            raise RuntimeError('multiple PN1 key options in formal unit')
        if existing:
            updated = updated.replace(existing[0], '', 1)
    elif btc_polynode_key_file is not None:
        option = f'--btc-polynode-key-file {btc_polynode_key_file}'
        existing = re.findall(r'--btc-polynode-key-file\s+[^\s\\]+', updated)
        if len(existing) > 1:
            raise RuntimeError('multiple PN1 key options in formal unit')
        if existing:
            updated = updated.replace(existing[0], option)
        else:
            marker = '--btc-5m-manager-root '
            if updated.count(marker) != 1:
                raise RuntimeError('expected one BTC 5m manager in formal unit')
            updated = updated.replace(marker, option + ' ' + marker)
    return updated


def validate_chainlink_credentials_path(value):
    if value is None:
        return None
    path = Path(value)
    allowed = Path('/home/czx/.config/marketcow/secrets')
    if (path.parent != allowed or not re.fullmatch(r'[A-Za-z0-9_.-]+\.json', path.name)):
        raise RuntimeError('Chainlink credentials must be a private MarketCow secrets JSON path')
    return path


def validate_polynode_key_path(value):
    path = Path(value)
    allowed = {
        Path('/mnt/p44pro/marketcow-shadow-v3-runtime/linux/diagnostics/polynode-probe/api-key'),
        Path('/home/czx/.config/marketcow/secrets/polynode-pn1.key'),
    }
    if path not in allowed:
        raise RuntimeError('PN1 key must use an approved private U1 path')
    return path


def pin_uk_polymarket_ws_route():
    """Route only the public CLOB market WS through the fixed UK Pro node."""
    import yaml
    config_path = ROOT / 'polymarket-proxy/config.yaml'
    if config_path.is_symlink() or not config_path.is_file():
        raise RuntimeError('formal proxy config is absent or linked')
    original = config_path.read_bytes()
    config = yaml.safe_load(original)
    node = '🇬🇧 英国-Pro-01|v202605'
    if node not in {item['name'] for item in config['proxies']}:
        raise RuntimeError('fixed UK Pro node is absent')
    if not any(group.get('name') == 'POLYMARKET' for group in config['proxy-groups']):
        raise RuntimeError('formal Polymarket proxy group is absent')
    config['proxy-groups'] = [group for group in config['proxy-groups']
                              if group.get('name') != 'POLYMARKET_WS']
    config['proxy-groups'].insert(0, {
        'name': 'POLYMARKET_WS', 'type': 'select', 'proxies': [node]})
    rule = 'DOMAIN,ws-subscriptions-clob.polymarket.com,POLYMARKET_WS'
    config['rules'] = [rule] + [item for item in config['rules'] if item != rule]
    replacement = yaml.safe_dump(config, allow_unicode=True).encode()

    def atomic_write(payload):
        with tempfile.NamedTemporaryFile(dir=config_path.parent, delete=False) as stream:
            os.fchmod(stream.fileno(), 0o600)
            stream.write(payload)
            name = stream.name
        Path(name).replace(config_path)

    atomic_write(replacement)
    try:
        run(['systemctl', '--user', 'restart', 'marketcow-polymarket-proxy.service'],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        result = subprocess.run(['systemctl', '--user', 'is-active',
                                 'marketcow-polymarket-proxy.service'],
                                capture_output=True, text=True)
        if result.stdout.strip() != 'active':
            raise RuntimeError('UK WS proxy did not start')
    except Exception:
        atomic_write(original)
        run(['systemctl', '--user', 'restart', 'marketcow-polymarket-proxy.service'],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        raise
    return node


def remote(args):
    if not ROOT.is_mount() or ROOT.resolve() != ROOT:
        raise RuntimeError('U1 runtime data volume is not mounted')
    if subprocess.check_output(['findmnt', '-n', '-o', 'FSTYPE', '-T', str(ROOT)], text=True).strip() != 'ext4':
        raise RuntimeError('runtime filesystem must enforce Unix permissions')
    DEPLOY.mkdir(mode=0o700, exist_ok=True)
    with (DEPLOY / 'deploy.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if subprocess.run(['pgrep', '-x', 'cargo'], stdout=subprocess.DEVNULL).returncode == 0:
            raise RuntimeError('another U1 Cargo build is running; wait for its completion')
        archive = DEPLOY / 'incoming/source.tar.gz'
        if sha(archive) != args.archive_sha256:
            raise RuntimeError('uploaded archive checksum differs')
        manifest = json.loads((DEPLOY / 'incoming/manifest.json').read_bytes())
        staging = DEPLOY / 'source.preparing'
        remove_owned(staging, DEPLOY)
        unpack_verified(archive, manifest, staging)
        source = DEPLOY / 'source'
        remove_owned(source, DEPLOY)
        staging.rename(source)
        (DEPLOY / 'source-manifest.json').write_text(json.dumps(manifest, sort_keys=True))
        target = DEPLOY / 'target'
        # A failed pre-publication build may retain this isolated target for
        # forward-fix retries. Cargo fingerprints every staged source input;
        # the target is removed only after a verified formal start below.
        if target.is_symlink():
            raise RuntimeError('unsafe build target symlink')
        tmp = ROOT / 'tmp'
        tmp.mkdir(mode=0o700, exist_ok=True)
        tmp.chmod(0o700)
        if shutil.disk_usage(ROOT).free < 6 * 1024**3:
            raise RuntimeError('6GiB free required for isolated compilation')
        env = dict(os.environ, TMPDIR=str(tmp), CARGO_TARGET_DIR=str(target),
                   CARGO_INCREMENTAL='0', CARGO_PROFILE_TEST_DEBUG='0',
                   CARGO_BUILD_JOBS=str(args.jobs))
        receipt = {'archive_sha256': args.archive_sha256, 'source_files': len(manifest),
                   'unit': UNIT, 'status': 'building', 'started_at': time.time(),
                   'test_scope': 'local_targeted_only' if args.quick else 'u1_offline_full'}
        log_path = DEPLOY / 'build.log'
        with log_path.open('w') as log:
            if not args.quick:
                run(['nice', '-n', '10', 'cargo', 'test', '--offline', '--locked',
                     '-p', 'marketcow-core', '-p', 'marketcow-polymarket',
                     '--lib', '--quiet'], cwd=source, env=env, stdout=log, stderr=log)
                run(['nice', '-n', '10', 'cargo', 'test', '--offline', '--locked',
                     '-p', 'marketcowd', '--bins', '--quiet'], cwd=source, env=env, stdout=log, stderr=log)
            run(['nice', '-n', '10', 'cargo', 'build', '--offline', '--locked', '--release',
                 '-p', 'marketcowd', '--bin', 'marketcow-discovery-collector'], cwd=source,
                env=env, stdout=log, stderr=log)
        binary = target / 'release/marketcow-discovery-collector'
        with binary.open('rb') as stream:
            elf = stream.read(20)
        if elf[:6] != b'\x7fELF\x02\x01' or int.from_bytes(elf[18:20], 'little') != 62:
            raise RuntimeError('build output must be ELF64 little-endian x86-64')
        current = DEPLOY / 'current'
        current.mkdir(mode=0o700, exist_ok=True)
        candidate = current / 'collector.new'
        shutil.copyfile(binary, candidate)
        candidate.chmod(0o755)
        receipt['binary_sha256'] = sha(candidate)
        unit_path = Path.home() / '.config/systemd/user' / UNIT
        unit = unit_path.read_text()
        credentials = validate_chainlink_credentials_path(args.btc_chainlink_credentials)
        if credentials is not None:
            metadata = credentials.lstat()
            if (not credentials.is_file() or credentials.is_symlink()
                    or metadata.st_uid != os.geteuid()
                    or metadata.st_mode & 0o077 or metadata.st_size > 4096):
                raise RuntimeError('Chainlink credentials are absent or not a private regular file')
        polynode_key = None
        if not args.disable_btc_polynode:
            polynode_key = validate_polynode_key_path(args.btc_polynode_key_file)
            key_metadata = polynode_key.lstat()
            if (not polynode_key.is_file() or polynode_key.is_symlink()
                    or key_metadata.st_uid != os.geteuid()
                    or key_metadata.st_mode & 0o077 or not 1 <= key_metadata.st_size <= 512):
                raise RuntimeError('PN1 key is absent or not a private regular file')
        updated = rewrite_unit(unit, current, args.memory_replay_bytes,
                               args.persistence_queue_batches, args.persistence_queue_bytes,
                               str(credentials) if credentials is not None else None,
                               args.btc_5m_clients, str(polynode_key) if polynode_key else None,
                               disable_polynode=args.disable_btc_polynode)
        run(['systemctl', '--user', 'stop', UNIT])
        if args.uk_polymarket_ws_route:
            receipt['polymarket_ws_route'] = pin_uk_polymarket_ws_route()
        candidate.replace(current / 'marketcow-discovery-collector')
        shutil.copyfile(source / 'scripts/run_with_bounded_log.py', current / 'run_with_bounded_log.py')
        unit_path.write_text(updated)
        unit_path.chmod(0o600)
        run(['systemctl', '--user', 'daemon-reload'])
        run(['systemctl', '--user', 'reset-failed', UNIT])
        run(['systemctl', '--user', 'start', UNIT])
        listen = re.search(r'--public-listen\s+([^\s\\]+)', updated)
        if not listen:
            raise RuntimeError('formal public listen address absent')
        url = 'http://' + listen.group(1) + '/v1/prediction-markets/polymarket/research/btc-5m/status'
        for _ in range(30):
            probe = subprocess.run(['curl', '-fsS', '--max-time', '3', url], capture_output=True)
            if probe.returncode == 0:
                status = json.loads(probe.stdout)
                if status.get('epoch'):
                    (DEPLOY / 'startup-status.json').write_bytes(probe.stdout)
                    receipt['source_epoch'] = status['epoch']
                    break
            time.sleep(2)
        else:
            raise RuntimeError('collector failed the bounded startup health check')
        pid = int(subprocess.check_output(['systemctl', '--user', 'show', UNIT, '-p', 'MainPID', '--value']))
        children = subprocess.check_output(['pgrep', '-P', str(pid)], text=True).split()
        if not any(sha(Path('/proc') / child / 'exe') == receipt['binary_sha256'] for child in children):
            raise RuntimeError('running child executable checksum differs')
        # A first successful HTTP response is not proof that the process
        # survives its next actor completion. Reject restart churn explicitly;
        # this bounded startup check is still not sustained-chain acceptance.
        for _ in range(5):
            time.sleep(2)
            state = dict(line.split('=', 1) for line in subprocess.check_output(
                ['systemctl', '--user', 'show', UNIT, '-p', 'MainPID',
                 '-p', 'NRestarts', '-p', 'ActiveState'], text=True).splitlines())
            if (state.get('ActiveState') != 'active'
                    or int(state.get('MainPID', 0)) != pid
                    or int(state.get('NRestarts', -1)) != 0):
                raise RuntimeError('collector exited/restarted during startup verification')
        receipt['startup_no_restart_seconds'] = 10
        receipt['removed_paths'] = cleanup_previous_versions()
        remove_owned(target, DEPLOY)
        remove_owned(DEPLOY / 'incoming', DEPLOY)
        receipt.update(status='started', finished_at=time.time(), main_pid=pid,
                       unit_sha256=sha(unit_path), startup_status_sha256=sha(DEPLOY / 'startup-status.json'))
        (DEPLOY / 'receipt.json').write_text(json.dumps(receipt, sort_keys=True, indent=2))
        print(json.dumps(receipt, sort_keys=True), flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--host', default='czx@u1')
    parser.add_argument('--jobs', type=int, choices=range(1, 9), default=2)
    parser.add_argument('--quick', action='store_true',
                        help='skip full offline test suite after targeted local verification')
    parser.add_argument('--memory-replay-bytes', type=int, choices=(2 * 1024**3, 6 * 1024**3))
    parser.add_argument('--persistence-queue-batches', type=int, choices=(2048, 16384))
    parser.add_argument('--persistence-queue-bytes', type=int,
                        choices=(512 * 1024**2, 4 * 1024**3))
    parser.add_argument('--btc-chainlink-credentials',
                        help='existing private CLOB L2 credentials file on U1; never uploaded')
    parser.add_argument('--btc-5m-clients', type=int, choices=range(1, 13), default=12,
                        help='bounded global BTC 5m WS budget; 12 gives four slots per market')
    parser.add_argument('--btc-polynode-key-file', default=str(
        ROOT / 'diagnostics/polynode-probe/api-key'),
        help='existing private PN1 key file on U1; key contents are never uploaded')
    parser.add_argument('--disable-btc-polynode', action='store_true',
                        help='omit PN1 subscription without deleting the private key file')
    parser.add_argument('--uk-polymarket-ws-route', action='store_true',
                        help='pin only the public market WebSocket to the UK Pro proxy node')
    parser.add_argument('--remote', action='store_true', help=argparse.SUPPRESS)
    parser.add_argument('--archive-sha256', help=argparse.SUPPRESS)
    args = parser.parse_args()
    if (args.persistence_queue_batches is None) != (args.persistence_queue_bytes is None):
        parser.error('persistence queue batches and bytes must be supplied together')
    validate_chainlink_credentials_path(args.btc_chainlink_credentials)
    if not args.disable_btc_polynode:
        validate_polynode_key_path(args.btc_polynode_key_file)
    remote(args) if args.remote else local(args)
