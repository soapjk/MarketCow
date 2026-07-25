from __future__ import annotations

from typing import Iterable, Sequence, Tuple


MigrationIdentity = Tuple[int, str]


def _text(value: object) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="strict")
    return str(value)


def validate_migration_history(
    applied: Iterable[MigrationIdentity],
    available: Sequence[tuple],
    backend: str,
) -> set[int]:
    """Require an applied migration history to be an exact known prefix."""
    expected = [(int(row[0]), _text(row[1])) for row in available]
    actual = sorted((int(version), _text(description)) for version, description in applied)
    versions = [version for version, _ in actual]

    if len(versions) != len(set(versions)):
        raise RuntimeError(f"{backend} migration history contains duplicate versions")
    known = dict(expected)
    unknown = [version for version in versions if version not in known]
    if unknown:
        raise RuntimeError(
            f"{backend} schema is newer than this binary: unknown migrations {unknown}"
        )
    mismatched = [
        version for version, description in actual if known[version] != description
    ]
    if mismatched:
        raise RuntimeError(
            f"{backend} migration descriptions do not match this binary: {mismatched}"
        )
    expected_prefix = [version for version, _ in expected[:len(actual)]]
    if versions != expected_prefix:
        raise RuntimeError(
            f"{backend} migration history is not a contiguous known prefix"
        )
    return set(versions)
