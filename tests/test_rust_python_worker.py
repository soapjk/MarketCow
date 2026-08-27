from pathlib import Path

import pytest

from python.marketcow_workers.worker import safe_staging_path


def test_staging_path_is_contained(tmp_path: Path) -> None:
    path = safe_staging_path(tmp_path.resolve(), "task-123", "result.json")
    assert path == tmp_path.resolve() / "task-123" / "result.json"


@pytest.mark.parametrize("filename", ["../secret", "/etc/passwd", "nested/file"])
def test_staging_path_escape_is_rejected(tmp_path: Path, filename: str) -> None:
    with pytest.raises(ValueError):
        safe_staging_path(tmp_path.resolve(), "task-123", filename)

