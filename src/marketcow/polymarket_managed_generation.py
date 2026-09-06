"""Prepared generations: systemd owns Rust children, Python only serves reads.

All paths, resource budgets and network endpoints come from private operator
configuration, never from the activation HTTP request.
"""
from __future__ import annotations

import asyncio
import hashlib
from pathlib import Path

import httpx

from .polymarket_configured_scope import PolymarketConfiguredScope
from .polymarket_live import LiveFullSyncResponse
from .polymarket_live_read_api import create_polymarket_live_read_app
from .polymarket_scope_activation import LiveGeneration


class ManagedGenerationFactory:
    def __init__(self, *, storage_root: Path, collector_binary: Path, bridge_binary: Path,
                 supervisor_unit: str, read_options: dict, collector_options: dict,
                 proxy_url: str, preheat_timeout_seconds: float,
                 collector_memory_max_mib: int, input_mode: str = 'websocket'):
        import re
        from urllib.parse import urlsplit
        if not re.fullmatch(r"marketcow-dynamic-live-[a-z0-9-]+\.service", supervisor_unit):
            raise ValueError("explicit isolated MarketCow supervisor required")
        proxy = urlsplit(proxy_url)
        if proxy.scheme != 'http' or proxy.hostname != '127.0.0.1' or not proxy.port or proxy.username or proxy.password or proxy.path not in ('','/'):
            raise ValueError("explicit loopback Polymarket proxy required")
        self.root = storage_root.resolve(strict=True)
        self.collector = collector_binary.resolve(strict=True)
        self.bridge = bridge_binary.resolve(strict=True)
        if not all(p.is_relative_to(self.root) for p in (self.collector,self.bridge)):
            raise ValueError("binaries must remain inside isolated storage")
        self.parent = supervisor_unit
        if input_mode not in ('websocket','rest-poll'):
            raise ValueError('input_mode must be websocket or rest-poll')
        self.input_mode = input_mode
        self.read_options = read_options
        self.collector_options = collector_options
        expected = {'concurrency','request_market_batch_size','response_byte_limit','batch_byte_limit',
                    'poll_seconds','request_timeout_seconds','lifecycle_refresh_seconds',
                    'persistence_queue_batches','persistence_queue_bytes',
                    'websocket_shard_tokens','websocket_recovery_concurrency',
                    'websocket_confirmation_seconds'}
        if set(collector_options) != expected or any(type(v) is not int or v <= 0 for v in collector_options.values()):
            raise ValueError("complete explicit collector configuration required")
        if preheat_timeout_seconds <= 0:
            raise ValueError("positive preheat timeout required")
        if type(collector_memory_max_mib) is not int or not 512 <= collector_memory_max_mib <= 4096:
            raise ValueError("collector memory budget must be 512..4096 MiB")
        self.preheat_timeout = preheat_timeout_seconds
        self.collector_memory_max_mib = collector_memory_max_mib
        self.proxy = proxy_url
        self.occupied: set[Path] = set()
        self.ports: set[int] = set()

    async def command(self, *args):
        process = await asyncio.create_subprocess_exec(*args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        stdout, stderr = await process.communicate()
        if process.returncode:
            raise ValueError(f"managed_runtime_command_failed:{args[0]}:{process.returncode}:{stderr.decode()[:512]}")
        return stdout

    def checked_file(self, root: Path, name: str, sha: str) -> Path:
        path = root / name
        if path.is_symlink() or not path.resolve(strict=True).is_relative_to(root) or path.stat().st_size > 16*1024*1024:
            raise ValueError("prepared_file_path_or_budget_invalid")
        if hashlib.sha256(path.read_bytes()).hexdigest() != sha:
            raise ValueError("prepared_file_hash_mismatch:"+name)
        return path

    async def __call__(self, artifact: dict, artifact_sha: str) -> LiveGeneration:
        expected = {'schema_version','scope_id','root','configured_scope_sha256','collector_plan_sha256',
                    'dependency_plan_sha256','bridge_port'}
        if set(artifact) != expected or artifact['schema_version'] != 'marketcow.polymarket.managed-live-generation.v1':
            raise ValueError("unsupported_prepared_generation")
        relative = Path(artifact['root'])
        if relative.is_absolute() or '..' in relative.parts:
            raise ValueError("prepared_root_must_be_relative")
        root = (self.root/relative).resolve(strict=True)
        if not root.is_relative_to(self.root) or root == self.root or root in self.occupied:
            raise ValueError("prepared_root_not_isolated")
        port = artifact['bridge_port']
        if type(port) is not int or not 1024 <= port <= 65535 or port in (8790,8793,17890) or port in self.ports:
            raise ValueError("isolated_bridge_port_unavailable")
        scope_file = self.checked_file(root,'configured-scope.json',artifact['configured_scope_sha256'])
        plan = self.checked_file(root,'rust-scoped-plan-r1.json',artifact['collector_plan_sha256'])
        dependency = self.checked_file(root,'live-bridge-plan-r1.json',artifact['dependency_plan_sha256'])
        scope = PolymarketConfiguredScope.model_validate_json(scope_file.read_bytes())
        if scope.active_scope_id != artifact['scope_id'] or not 1 <= scope.configured_market_count <= 250:
            raise ValueError("configured_scope_identity_or_count_invalid")
        await self.command('systemctl','--user','is-active','--quiet',self.parent)
        self.occupied.add(root); self.ports.add(port)
        units = []
        context = None
        log_root = self.root/'logs'
        log_root.mkdir(exist_ok=True)
        async def close():
            nonlocal context
            try:
                if context is not None:
                    await context.__aexit__(None,None,None)
                    context = None
            finally:
                for unit in reversed(units):
                    await self.command('systemctl','--user','stop',unit)
                self.occupied.discard(root); self.ports.discard(port)
        async def start(kind, command, memory):
            unit = f'marketcow-dynamic-{artifact_sha[:24]}-{kind}.service'
            await self.command('systemd-run','--user','--quiet','--collect','--unit='+unit,
                '--property=BindsTo='+self.parent,'--property=After='+self.parent,
                '--property=UMask=0077','--property=MemoryMax='+memory,
                '--property=KillSignal=SIGINT','--property=TimeoutStopSec=30',
                '--property=StandardOutput=append:'+str(log_root/(unit+'.log')),
                '--property=StandardError=append:'+str(log_root/(unit+'.log')),
                *command)
            units.append(unit)
        try:
            command = ['/usr/bin/env','-u','ALL_PROXY','-u','all_proxy',
                'HTTPS_PROXY='+self.proxy,'HTTP_PROXY='+self.proxy,'NO_PROXY=127.0.0.1,localhost,::1',
                str(self.collector),'--input-mode',self.input_mode,'--root',str(root),'--plan',str(plan),
                '--plan-sha256',artifact['collector_plan_sha256'],
                '--configured-scope',str(scope_file),'--configured-scope-sha256',artifact['configured_scope_sha256'],
                '--dependency-plan',str(dependency),'--dependency-plan-sha256',artifact['dependency_plan_sha256'],
                '--expected-market-count',str(scope.configured_market_count),
                '--live-listen',f'127.0.0.1:{port}',
                '--live-frame-bytes','33554432','--live-maximum-clients','2']
            for key,value in self.collector_options.items(): command.extend(['--'+key.replace('_','-'),str(value)])
            await start('collector',command,f'{self.collector_memory_max_mib}M')
            # The collector publishes directly from memory. The durable bridge is
            # an offline diagnostic/recovery tool, never the normal live source.
            app = create_polymarket_live_read_app(root=root, configured_scope_path=scope_file,
                live_stream_uri=f'ws://127.0.0.1:{port}', **self.read_options)
            context = app.router.lifespan_context(app)
            await context.__aenter__()
            ids = tuple(m.market_id for m in scope.configured_markets)
            async def verify():
                for unit in units:
                    await self.command('systemctl','--user','is-active','--quiet',unit)
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://isolated') as client:
                    # A registered scope is already content-addressed and bounded.
                    # Keep the public ad-hoc market_id query cap at 100 while
                    # allowing an exact configured scope to contain up to 250.
                    response = await client.get(
                        '/v1/prediction-markets/polymarket/live/full-sync',
                        params={'scope_id': scope.active_scope_id},
                    )
                if response.status_code != 200:
                    raise ValueError('candidate_full_sync_rejected:'+response.text[:1024])
                result = LiveFullSyncResponse.model_validate_json(response.content)
                if (result.scope_id != scope.active_scope_id or result.catalog_revision != scope.catalog_revision
                        or set(result.scope_market_ids) != set(ids)
                        or len(result.snapshot.items) != len(ids)
                        or {frame.market_id for frame in result.snapshot.items} != set(ids)):
                    raise ValueError('candidate_atomic_readiness_mismatch')
                return result.cursor
            deadline = asyncio.get_running_loop().time()+self.preheat_timeout
            while True:
                try:
                    await verify()
                    break
                except ValueError:
                    if asyncio.get_running_loop().time() >= deadline: raise
                    await asyncio.sleep(0.5)
            return LiveGeneration(scope.active_scope_id,artifact_sha,app,ids,verify,close)
        except BaseException:
            await asyncio.shield(close())
            raise
