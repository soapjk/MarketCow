"""Register a truly preheated Discovery candidate; never select or publish it."""
import asyncio
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import time

from marketcow.universe_generation import GenerationStore
from marketcow.universe_generation_preparation import preheat_and_register
from marketcow.universe_owner import SupervisorOwner
from marketcow.universe_systemd import DiscoveryUnitBinding, SystemdUnits, UnitArtifact


def sha(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def main():
    os.umask(0o077)
    root = Path('/mnt/p44pro/marketcow-shadow-v3-runtime/linux')
    release = root/'releases/universe-runtime-r5'
    candidate_root = root/'universe-discovery-candidate-r1'
    user_units = Path('/home/czx/.config/systemd/user')
    preheat = release/'units/marketcow-universe-discovery-preheat-r2.service'
    candidate = release/'units/marketcow-polymarket-discovery.service'
    incumbent = (user_units/'marketcow-polymarket-discovery.service').resolve(strict=True)
    output = root/'logs/universe-discovery-registration-r1.json'
    if output.exists() or candidate.exists():
        raise ValueError('refuse existing operation outputs')
    body = preheat.read_text().replace('--discovery-listen 127.0.0.1:18900', '--discovery-listen 192.168.124.3:8795')
    body = body.replace('Restart=no\nRuntimeMaxSec=180', 'Restart=on-failure')
    body = body.replace('universe-discovery-preheat-r2.log', 'universe-discovery-candidate-public-r1.log')
    with candidate.open('x') as stream:
        stream.write(body); stream.flush(); os.fsync(stream.fileno())
    candidate.chmod(0o400)
    manifest = json.loads((candidate_root/'catalog.json').read_bytes())
    preparation = json.loads((candidate_root/'candidate-preparation.json').read_bytes())
    dependencies = json.loads((candidate_root/'discovery-dependencies.json').read_bytes())
    binding = DiscoveryUnitBinding(generation_id='unregistered', scope_id='not-a-live-scope',
        preheat=UnitArtifact(preheat.name, preheat, sha(preheat)),
        candidate=UnitArtifact(candidate.name, candidate, sha(candidate)),
        incumbent=UnitArtifact(candidate.name, incumbent, sha(incumbent)),
        preheat_endpoint='http://127.0.0.1:18900', public_endpoint='http://192.168.124.3:8795',
        full_sync_bytes=67108864, frame_bytes=16777216, maximum_frames=10, maximum_stream_bytes=67108864,
        projection_id='observed-per-process-at-fullsync', universe_revision=preparation['universe_revision'])
    store_path = root/'phase1/generations-r1.sqlite'
    owner = root/'phase1/runtime-owner.lock'
    with SupervisorOwner(owner):
        store = GenerationStore(store_path, maximum_generations=8, maximum_record_bytes=65536)
        try:
            generation, registered, boundary = asyncio.run(preheat_and_register(store,
                SystemdUnits(release_root=root/'releases', user_unit_root=user_units, command_timeout_seconds=60),
                binding, pool='discovery', selection_id=preparation['selection_id'],
                catalog_revision=manifest['catalog_revision'], market_ids=tuple(sorted(manifest['realtime_universe']['market_ids'])),
                dependency_market_ids=tuple(sorted(x['market_id'] for x in dependencies['dependency_markets'])),
                protected_market_ids=(), parent_selection_id=None, expires_ms=time.time_ns()//1000000+86400000,
                now_ms=lambda: time.time_ns()//1000000, operation_timeout_seconds=90))
            assert store.desired('discovery') is None and store.applied('discovery') is None
        finally:
            store.close()
    assert (user_units/candidate.name).resolve() == incumbent
    result = dict(generation=generation.model_dump(mode='json'), generation_id=generation.generation_id,
        boundary=asdict(boundary), binding=asdict(registered), desired=None, applied=None,
        formal_changed=False, store_path=str(store_path), owner_lock=str(owner))
    with output.open('x') as stream:
        json.dump(result, stream, default=str, sort_keys=True); stream.flush(); os.fsync(stream.fileno())
    print(json.dumps(dict(generation_id=generation.generation_id, boundary=asdict(boundary),
        formal_changed=False, output=str(output)), sort_keys=True))


if __name__ == '__main__':
    main()
