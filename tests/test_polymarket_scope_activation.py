import asyncio
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx
from fastapi import FastAPI

from marketcow.polymarket_scope_activation import LiveGeneration, ScopeActivationGateway


class ActivationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.closed = []
        self.created = []
        self.fail = False
        self.gate = None
        self.initial = self.generation('a'*64, 'b'*64, 100)
        async def factory(artifact, sha):
            self.created.append(artifact)
            if self.gate is not None:
                await self.gate.wait()
            return self.generation(artifact['scope_id'], sha, artifact['count'])
        self.gateway = ScopeActivationGateway(active=self.initial, registry_root=self.root,
            admin_token='x'*32, factory=factory, activation_timeout_seconds=1,
            verification_interval_seconds=0.001)
        self.client = httpx.AsyncClient(transport=httpx.ASGITransport(app=self.gateway), base_url='http://test')

    async def asyncTearDown(self):
        await asyncio.gather(*self.gateway.retirements, return_exceptions=True)
        await self.client.aclose()
        self.tmp.cleanup()

    def generation(self, sid, sha, count):
        app = FastAPI()
        @app.get('/scope')
        async def scope(): return {'scope_id':sid, 'count':count}
        cursor = 0
        async def verify():
            nonlocal cursor
            if self.fail: raise ValueError('missing_dependency_book')
            cursor += 1
            return cursor
        async def close(): self.closed.append(sid)
        return LiveGeneration(sid, sha, app, tuple(map(str,range(count))), verify, close)

    def register(self, sid, count):
        body = json.dumps({'scope_id':sid, 'count':count}).encode()
        (self.root / f'{sid}.json').write_bytes(body)
        return {'schema_version':'marketcow.polymarket.scope-activation.v1',
                'scope_id':sid, 'scope_file_sha256':hashlib.sha256(body).hexdigest()}

    async def activate(self, body):
        return await self.client.post(self.gateway.path,json=body,headers={'Authorization':'Bearer '+'x'*32})

    async def test_expand_shrink_same_gateway_and_idempotency(self):
        for sid, count in [('c'*64,250),('d'*64,50)]:
            body = self.register(sid,count)
            response = await self.activate(body)
            self.assertEqual(response.status_code,200,response.text)
            self.assertEqual(response.json()['status'],'activated_ready')
            self.assertEqual((await self.client.get('/scope')).json()['count'],count)
            self.assertEqual(json.loads((self.root/'active-live-generation.json').read_text())['active_scope_id'],sid)
            self.assertEqual((await self.activate(body)).json()['status'],'already_active')
        self.assertEqual(len(self.created),2)

    async def test_unready_preserves_old_scope_and_closes_candidate(self):
        self.fail = True
        response = await self.activate(self.register('c'*64,250))
        self.assertEqual(response.status_code,409)
        self.assertIn('missing_dependency_book',response.text)
        self.assertIs(self.gateway.active,self.initial)
        self.assertFalse((self.root/'active-live-generation.json').exists())
        self.assertEqual(self.closed,['c'*64])

    async def test_auth_hash_symlink_and_count_fail_closed(self):
        body = self.register('c'*64,251)
        self.assertEqual((await self.client.post(self.gateway.path,json=body)).status_code,401)
        bad = {**body,'scope_file_sha256':'0'*64}
        self.assertEqual((await self.activate(bad)).status_code,422)
        self.assertFalse(self.created)
        self.assertEqual((await self.activate(body)).status_code,409)
        self.assertIs(self.gateway.active,self.initial)
        path=self.root/('c'*64+'.json'); path.unlink(); path.symlink_to('/dev/null')
        self.assertEqual((await self.activate(body)).status_code,422)

    async def test_persistence_failure_never_publishes(self):
        body = self.register('c'*64,250)
        with patch('marketcow.polymarket_scope_activation._atomic_replace',side_effect=OSError('disk full')):
            with self.assertRaises(OSError): await self.activate(body)
        self.assertIs(self.gateway.active,self.initial)
        self.assertEqual(self.closed,['c'*64])

    async def test_overlapping_activation_is_rejected(self):
        self.gate = asyncio.Event()
        body = self.register('c'*64,250)
        first = asyncio.create_task(self.activate(body))
        for _ in range(100):
            if self.created: break
            await asyncio.sleep(0.001)
        self.assertEqual((await self.activate(body)).status_code,409)
        self.gate.set()
        self.assertEqual((await first).status_code,200)

    async def test_websocket_is_retired_with_resync(self):
        connected=asyncio.Event(); messages=[]
        async def socket_app(scope,receive,send):
            await send({'type':'websocket.accept'})
            connected.set()
            await asyncio.Event().wait()
        self.initial.app=socket_app
        async def receive(): return {'type':'websocket.connect'}
        async def send(message): messages.append(message)
        task=asyncio.create_task(self.gateway({'type':'websocket','path':'/stream'},receive,send))
        await connected.wait()
        self.assertEqual((await self.activate(self.register('c'*64,250))).status_code,200)
        await asyncio.wait_for(task,1)
        frame=json.loads(messages[-2]['text'])
        self.assertEqual(frame['reason'],'scope_changed')
        self.assertEqual(messages[-1],{'type':'websocket.close','code':1012})

    async def test_old_http_finishes_before_runtime_closes(self):
        started=asyncio.Event(); release=asyncio.Event()
        async def app(scope,receive,send):
            started.set(); await release.wait()
            await send({'type':'http.response.start','status':200,'headers':[]})
            await send({'type':'http.response.body','body':b'old'})
        self.initial.app=app
        old=asyncio.create_task(self.client.get('/old'))
        await started.wait()
        self.assertEqual((await self.activate(self.register('c'*64,250))).status_code,200)
        await asyncio.sleep(0.001)
        self.assertNotIn('a'*64,self.closed)
        release.set()
        self.assertEqual((await old).text,'old')
        await asyncio.gather(*self.gateway.retirements)
        self.assertIn('a'*64,self.closed)


if __name__ == '__main__': unittest.main()
