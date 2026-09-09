"""Prepare only the existing Discovery list as a labelled migration test."""
import json
import time
from pathlib import Path

from marketcow.universe_discovery_candidate import prepare_discovery_candidate
from marketcow.universe_prepared_source import PreparedCatalogSource


def main():
    root = Path('/mnt/p44pro/marketcow-shadow-v3-runtime/linux')
    original = root/'bounded-discovery-candidate-r1'
    manifest = json.loads((original/'catalog.json').read_bytes())
    source = PreparedCatalogSource(root/'phase1/catalog-r1.sqlite',
        expected_sha256='f34ac11f5140c133229094bbf1c91b25080b61a5262f2c2629f7b78995373c50', max_row_bytes=2097152)
    started = time.monotonic()
    try:
        report = prepare_discovery_candidate(source_root=original,
            target_root=root/'universe-discovery-candidate-r1', catalog_source=source,
            market_ids=tuple(sorted(manifest['realtime_universe']['market_ids'])),
            selection_id='test-incumbent:'+manifest['realtime_universe']['universe_id'],
            policy_version='incumbent-migration-test-not-new-ranking', maximum_dependencies=2000,
            # Offline metadata closure audit budget, not deployed acquisition profile.
            maximum_tokens=6000, maximum_row_bytes=2097152, maximum_artifact_bytes=67108864,
            maximum_catalog_copy_bytes=134217728, maximum_source_bytes=2147483648,
            maximum_relation_members=16000, depth_quantities=['10'], maximum_book_age_ms=5000)
    finally:
        source.close()
    print(json.dumps(dict(report, elapsed_seconds=time.monotonic()-started), sort_keys=True))


if __name__ == '__main__':
    main()
