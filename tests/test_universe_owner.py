import subprocess
import sys

import pytest

from marketcow.universe_owner import SupervisorOwner


def test_exclusive_and_exception_release(tmp_path):
    path = tmp_path / "owner.lock"
    with pytest.raises(LookupError):
        with SupervisorOwner(path):
            with pytest.raises(RuntimeError, match="already running"):
                with SupervisorOwner(path):
                    pass
            result = subprocess.run([sys.executable, "-c",
                "from pathlib import Path; from marketcow.universe_owner import SupervisorOwner; "
                "owner=SupervisorOwner(Path(__import__('sys').argv[1])); owner.__enter__()", str(path)],
                capture_output=True, timeout=5)
            assert result.returncode != 0
            assert b"supervisor already running" in result.stderr
            raise LookupError
    inode = path.stat().st_ino
    with SupervisorOwner(path):
        assert path.stat().st_ino == inode


def test_symlink_and_public_permissions_rejected(tmp_path):
    target = tmp_path / "target"
    target.touch(mode=0o600)
    link = tmp_path / "link"
    link.symlink_to(target)
    with pytest.raises(OSError):
        with SupervisorOwner(link):
            pass
    target.chmod(0o644)
    with pytest.raises(ValueError, match="private"):
        with SupervisorOwner(target):
            pass
