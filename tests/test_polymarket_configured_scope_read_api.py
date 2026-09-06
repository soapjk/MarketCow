import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from fastapi.testclient import TestClient
from marketcow.polymarket_contracts import content_sha256
from marketcow.polymarket_live_read_api import create_polymarket_live_read_app


class ConfiguredScopeReadTests(unittest.TestCase):
    def test_content_addressed_scope_reads_all_250_without_widening_ad_hoc_cap(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            revision = 'a' * 64
            markets = [{
                'market_id': str(index),
                'condition_id': f'condition-{index}',
                'token_ids': [f'yes-{index}', f'no-{index}'],
                'end_at': '2027-01-01T00:00:00Z',
            } for index in range(250)]
            body = {
                'catalog_revision': revision,
                'configured_markets': markets,
                'mode': 'shadow',
            }
            scope = {
                **body,
                'schema_version': 'marketcow.polymarket.scope-discovery.v1',
                'configured_market_count': 250,
                'active_scope_id': content_sha256(body),
            }
            path = root / 'configured-scope.json'
            path.write_text(json.dumps(scope))
            (root / 'catalog.json').write_text(json.dumps({
                'catalog_revision': revision,
            }))
            (root / 'scope-runtime.json').write_text(json.dumps({
                'scope_id': scope['active_scope_id'],
                'manifest_sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
            }))
            app = create_polymarket_live_read_app(
                root=root, discovery_root=root, configured_scope_path=path,
                stable_snapshot_max_book_age_seconds=3.5,
                stable_read_wait_seconds=6, stable_read_poll_seconds=0.025,
                executor_workers=2, discovery_depth_notionals=('10',),
                discovery_maximum_book_age_ms=5000,
                live_stream_uri='ws://127.0.0.1:1',
            )
            projection = app.state.polymarket_live_projection
            projection._scope_id = scope['active_scope_id']
            captured = []

            def full_sync_json(reader, selected, *, _phase_ms):
                captured.append(selected)
                return b'{}', object()

            projection.full_sync_json = full_sync_json
            with TestClient(app) as client:
                response = client.get(
                    '/v1/prediction-markets/polymarket/live/full-sync',
                    params={'scope_id': scope['active_scope_id']},
                )
                self.assertEqual(response.status_code, 200, response.text)
                self.assertEqual(captured, [[str(index) for index in range(250)]])
                too_many = client.get(
                    '/v1/prediction-markets/polymarket/live/full-sync',
                    params=[
                        ('scope_id', scope['active_scope_id']),
                        *(("market_id", str(index)) for index in range(101)),
                    ],
                )
                self.assertEqual(too_many.status_code, 400, too_many.text)
                self.assertEqual(
                    too_many.json()['detail']['code'],
                    'polymarket_scope_too_large',
                )

    def test_binding_and_module_failure_isolation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            revision = 'a' * 64
            body = {'catalog_revision': revision, 'configured_markets': [], 'mode': 'shadow'}
            scope = {**body, 'schema_version': 'marketcow.polymarket.scope-discovery.v1',
                     'configured_market_count': 0, 'active_scope_id': content_sha256(body)}
            path = root / 'configured-scope.json'
            path.write_text(json.dumps(scope))
            (root / 'catalog.json').write_text(json.dumps({'catalog_revision': revision}))
            (root / 'scope-runtime.json').write_text(json.dumps({
                'scope_id': scope['active_scope_id'],
                'manifest_sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
            }))
            app = create_polymarket_live_read_app(
                root=root, discovery_root=root, configured_scope_path=path,
                stable_snapshot_max_book_age_seconds=3.5,
                stable_read_wait_seconds=6, stable_read_poll_seconds=0.025,
                executor_workers=2, discovery_depth_notionals=('10',),
                discovery_maximum_book_age_ms=5000,
                live_stream_uri='ws://127.0.0.1:1',
            )
            client = TestClient(app)
            url = '/v1/prediction-markets/polymarket/live/scope'
            self.assertEqual(client.get(url).json(), scope)
            captured = []
            projection = app.state.polymarket_live_projection
            projection.market_ids = lambda: [str(i) for i in range(256)]
            def health_json(reader, selected, *, _phase_ms):
                captured.append(selected)
                return b'{}'
            projection.health_json = health_json
            self.assertEqual(client.get('/v1/prediction-markets/polymarket/live/health').status_code, 200)
            self.assertEqual(captured, [[]])  # Explicit empty scope, not 256 hydrated dependencies.
            scope['configured_market_count'] = 1
            path.write_text(json.dumps(scope))
            self.assertEqual(client.get(url).status_code, 409)
            self.assertEqual(client.get('/v1/prediction-markets/polymarket/live/discovery/status').status_code, 200)
            path.unlink()
            self.assertEqual(client.get(url).status_code, 409)


if __name__ == '__main__':
    unittest.main()
