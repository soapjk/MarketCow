#!/usr/bin/env python3
"""Private-configured live.v2 gateway with bounded Rust scope preheating.

Registration/preparation is offline. Startup never traverses Gamma or builds a
catalog. Only the authenticated scope:activate route can start a candidate.
"""
import argparse
import asyncio
import fcntl
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import sys

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
import uvicorn
from marketcow.polymarket_configured_scope import PolymarketConfiguredScope
from marketcow.polymarket_live_read_api import create_polymarket_live_read_app
from marketcow.polymarket_managed_generation import ManagedGenerationFactory
from marketcow.polymarket_scope_activation import LiveGeneration, ScopeActivationGateway, ActivationRequest


def validated_listener(config):
    host = config['host']
    port = config['port']
    address = ipaddress.ip_address(host)
    if (
        type(port) is not int
        or port == 8790
        or not 1024 <= port <= 65535
        or address.is_unspecified
        or address.is_multicast
        or (not address.is_loopback and not (
            address.is_private and config.get('allow_private_lan') is True
        ))
    ):
        raise ValueError('explicit loopback or authorized private-LAN listener required')
    return host, port


async def run(path):
    config = json.loads(path.read_bytes())
    storage = Path(config['storage_root']).resolve(strict=True)
    def inside(value):
        p=Path(value).resolve(strict=True)
        if not p.is_relative_to(storage): raise ValueError('configuration path escapes isolated storage')
        return p
    registry = inside(config['registry_root'])
    token_file = inside(config['admin_token_file'])
    if token_file.stat().st_mode & 0o077: raise ValueError('admin token must be private')
    token = token_file.read_text().strip()
    read_options = dict(config['read_options'])
    read_options['discovery_root'] = inside(read_options['discovery_root'])
    factory = ManagedGenerationFactory(storage_root=storage,
        collector_binary=inside(config['collector_binary']),bridge_binary=inside(config['bridge_binary']),
        supervisor_unit=config['supervisor_unit'],read_options=read_options,
        collector_options=config['collector_options'],proxy_url=config['proxy_url'],
        input_mode=config['input_mode'] if 'input_mode' in config else 'websocket',
        collector_memory_max_mib=config['collector_memory_max_mib'],
        preheat_timeout_seconds=config['preheat_timeout_seconds'])
    # One owner per registry; retained until process shutdown.
    with (registry/'.gateway.lock').open('a') as lease:
        fcntl.flock(lease,fcntl.LOCK_EX|fcntl.LOCK_NB)
        root = inside(config['initial_root'])
        scope_file = root/'configured-scope.json'
        scope = PolymarketConfiguredScope.model_validate_json(scope_file.read_bytes())
        app = create_polymarket_live_read_app(root=root,configured_scope_path=scope_file,
            live_stream_uri=config['initial_live_stream_uri'],**read_options)
        context=app.router.lifespan_context(app)
        await context.__aenter__()
        factory.occupied.add(root)
        async def close():
            await context.__aexit__(None,None,None)
            factory.occupied.discard(root)
        async def unavailable(): raise ValueError('initial_generation_requires_managed_preheating')
        active=LiveGeneration(scope.active_scope_id,hashlib.sha256(scope_file.read_bytes()).hexdigest(),
            app,tuple(m.market_id for m in scope.configured_markets),unavailable,close)
        gateway=ScopeActivationGateway(active=active,registry_root=registry,admin_token=token,
            factory=factory,activation_timeout_seconds=config['activation_timeout_seconds'],
            verification_interval_seconds=config['verification_interval_seconds'])
        pointer_path=registry/'active-live-generation.json'
        if pointer_path.exists():
            pointer=json.loads(pointer_path.read_bytes())
            request=ActivationRequest(schema_version='marketcow.polymarket.scope-activation.v1',
                scope_id=pointer['active_scope_id'],scope_file_sha256=pointer['scope_file_sha256'])
            # Refuse rollback to the bootstrap scope if a published generation cannot recover.
            try:
                artifact=gateway.registered(request)
                restored=await factory(artifact,request.scope_file_sha256)
                await active.close()
                gateway.active=restored
            except BaseException:
                await active.close()
                raise
        host, port = validated_listener(config)
        server=uvicorn.Server(uvicorn.Config(gateway,host=host,port=port,access_log=False))
        await server.serve()


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--config',required=True,type=Path)
    args=parser.parse_args()
    os.umask(0o077)
    asyncio.run(run(args.config.resolve(strict=True)))
