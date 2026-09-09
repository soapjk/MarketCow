"""Process-lifetime local supervisor ownership; not a remote lease.

Keep the descriptor open across every prepare/publish/rollback operation. Never
unlink the lock file: another process may already be waiting on that inode.
"""
import fcntl
import os
import stat
from pathlib import Path


class SupervisorOwner:
    def __init__(self, path: Path):
        self.path = path
        self.fd = None

    def __enter__(self):
        if self.fd is not None:
            raise RuntimeError("supervisor owner already acquired")
        fd = os.open(self.path, os.O_CREAT | os.O_RDWR | os.O_CLOEXEC | os.O_NOFOLLOW, 0o600)
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_nlink != 1:
                raise ValueError("unsafe supervisor owner file")
            if info.st_mode & 0o077:
                raise ValueError("supervisor owner permissions must be private")
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise RuntimeError("supervisor already running") from exc
            self.fd = fd
            return self
        except BaseException:
            os.close(fd)
            raise

    def __exit__(self, *_):
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None

