import json
from pathlib import Path
import tempfile
import unittest
import shutil

from scripts.relocate_polymarket_live_source import digest, relocate


class RelocationTests(unittest.TestCase):
    def fixture(self, base):
        source = base / 'source'
        source.mkdir()
        original = Path('/old/isolated/live')
        manifest = {'catalog_revision': 'catalog'}
        for name in ['normalized_catalog', 'catalog_index', 'candidate_snapshot']:
            path = source / name
            path.write_bytes(b'fixture-' + name.encode())
            manifest[name] = {'path': str(original / name), 'sha256': digest(path)}
        (source / 'catalog.json').write_text(json.dumps(manifest))
        (source / 'index').write_bytes(b'index fixture')
        (source / 'state-index.json').write_text(json.dumps({'path': str(original / 'index')}))
        scope = base / 'scope.json'
        scope.write_text(json.dumps({'active_scope_id': 'scope', 'catalog_revision': 'catalog',
                                    'mode': 'shadow', 'configured_markets': []}))
        (source / 'scope-runtime.json').write_text(json.dumps({'manifest_sha256': digest(scope), 'scope_id': 'scope'}))
        return source, original, scope

    def test_relocation_preserves_source_and_scope(self):
        with tempfile.TemporaryDirectory() as temp:
            base = Path(temp)
            source, original, scope = self.fixture(base)
            before = digest(source / 'catalog.json')
            target = (base / 'target').resolve()
            relocate(source, target, original, scope)
            self.assertEqual(digest(source / 'catalog.json'), before)
            self.assertEqual(digest(target / 'configured-scope.json'), digest(scope))
            self.assertEqual(json.loads((target / 'state-index.json').read_text())['path'], str(target / 'index'))
            self.assertTrue(json.loads((target / 'relocation-report.json').read_text())['complete'])
            with self.assertRaises(ValueError):
                relocate(source, target, original, scope)

    def test_corruption_and_cross_root_references_rejected(self):
        for corrupt in [True, False]:
            with self.subTest(corrupt=corrupt), tempfile.TemporaryDirectory() as temp:
                base = Path(temp)
                source, original, scope = self.fixture(base)
                if corrupt:
                    (source / 'catalog_index').write_bytes(b'corruption')
                else:
                    manifest = json.loads((source / 'catalog.json').read_text())
                    manifest['catalog_index']['path'] = '/outside/catalog_index'
                    (source / 'catalog.json').write_text(json.dumps(manifest))
                with self.assertRaises(ValueError):
                    relocate(source, base / 'target', original, scope)
                self.assertFalse((base / 'target').exists())

    def test_relocating_previous_receipt_and_resuming_verified_copy(self):
        with tempfile.TemporaryDirectory() as temp:
            base=Path(temp)
            source,original,scope=self.fixture(base)
            receipt=source/'relocation-report.json'
            receipt.write_text('{"complete":true,"source_root":"historical"}')
            previous=digest(receipt)
            staging=base/'target.preparing'
            shutil.copytree(source,staging)
            inode=(staging/'normalized_catalog').stat().st_ino
            (staging/'index').write_bytes(b'partial corruption')
            relocate(source,base/'target',original,scope,resume=True)
            target=base/'target'
            self.assertEqual((target/'normalized_catalog').stat().st_ino,inode)
            self.assertEqual(digest(target/'index'),digest(source/'index'))
            self.assertEqual(digest(target/'relocation-provenance'/f'{previous}.json'),previous)
            self.assertEqual(digest(receipt),previous)


if __name__ == '__main__':
    unittest.main()
