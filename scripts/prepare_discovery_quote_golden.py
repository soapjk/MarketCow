"""Offline cross-language oracle from captured live metadata/books, not a service."""
import argparse
from datetime import datetime
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

from marketcow.polymarket_discovery import PolymarketDiscoveryStore
from marketcow.polymarket_live import LiveBook, LiveMarket


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', type=Path, required=True)
    parser.add_argument('--sha256', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    raw = args.input.read_bytes()
    assert hashlib.sha256(raw).hexdigest() == args.sha256
    full = json.loads(raw)
    books = {token: LiveBook.model_validate(book) for token, book in full['snapshot']['books'].items()}
    store = object.__new__(PolymarketDiscoveryStore)
    store.depth_notionals = ('10', '50', '100', '500')
    store.maximum_book_age_ms = 5000
    observed = datetime.fromisoformat(full['freshness_checked_at'])
    cases = []
    for raw_market in full['bootstrap']['markets']:
        market = LiveMarket.model_validate(raw_market)
        complete = all(relation.complete for relation in market.relations)
        expected = store._market_quote(market, books,
            metadata=SimpleNamespace(resolution_source=None), relation_complete=complete,
            has_gap=False, catalog_revision=full['catalog_revision'],
            boundary_cursor=full['cursor'], observed_at=observed).model_dump(mode='json')
        cases.append({'market': raw_market, 'relation_complete': complete, 'has_gap': False,
            'settlement': None, 'expected': expected})
    # Null settlement and no gaps are explicit oracle inputs; this artifact
    # doesn't assert the captured source had no gaps or no settlement evidence.
    output = {'input_sha256': args.sha256, 'catalog_revision': full['catalog_revision'],
        'cursor': full['cursor'], 'observed_at': full['freshness_checked_at'],
        'quantities': list(store.depth_notionals), 'maximum_book_age_ms': 5000,
        'books': full['snapshot']['books'], 'cases': cases}
    with args.output.open('x') as stream:
        json.dump(output, stream, separators=(',', ':'))
    with args.output.open('rb') as stream:
        print(json.dumps({'cases': len(cases), 'sha256': hashlib.file_digest(stream, 'sha256').hexdigest(), 'path': str(args.output)}))


if __name__ == '__main__':
    main()
