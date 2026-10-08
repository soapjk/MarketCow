from __future__ import annotations

from collections.abc import AsyncIterator, Callable, Iterator
from typing import Awaitable

from .errors import SnapshotChangedError
from .models import CanonicalBarPage, HistoricalBar, HistoricalManifest


class CanonicalSeries:
    def __init__(
        self,
        first_page: CanonicalBarPage,
        fetch_page: Callable[[str], CanonicalBarPage],
    ) -> None:
        self.manifest = first_page.manifest
        self._first_page = first_page
        self._fetch_page = fetch_page
        self._consumed = False

    def __iter__(self) -> Iterator[HistoricalBar]:
        if self._consumed:
            raise RuntimeError("canonical series iterators are single-use")
        self._consumed = True
        page = self._first_page
        while True:
            if page.manifest.snapshot_id != self.manifest.snapshot_id:
                raise SnapshotChangedError(
                    "canonical snapshot changed while iterating",
                    code="canonical_snapshot_changed",
                )
            yield from page.bars
            if page.next_cursor is None:
                return
            page = self._fetch_page(page.next_cursor)


class AsyncCanonicalSeries:
    def __init__(
        self,
        first_page: CanonicalBarPage,
        fetch_page: Callable[[str], Awaitable[CanonicalBarPage]],
    ) -> None:
        self.manifest: HistoricalManifest = first_page.manifest
        self._first_page = first_page
        self._fetch_page = fetch_page
        self._consumed = False

    def __aiter__(self) -> AsyncIterator[HistoricalBar]:
        if self._consumed:
            raise RuntimeError("canonical series iterators are single-use")
        self._consumed = True
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[HistoricalBar]:
        page = self._first_page
        while True:
            if page.manifest.snapshot_id != self.manifest.snapshot_id:
                raise SnapshotChangedError(
                    "canonical snapshot changed while iterating",
                    code="canonical_snapshot_changed",
                )
            for bar in page.bars:
                yield bar
            if page.next_cursor is None:
                return
            page = await self._fetch_page(page.next_cursor)
