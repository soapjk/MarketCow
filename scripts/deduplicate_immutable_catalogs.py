"""Offline, exact-target deduplication; never touches state/index/event files."""
import hashlib
import os
from pathlib import Path
import stat


def digest(path):
    with path.open('rb') as source:
        return hashlib.file_digest(source, 'sha256').hexdigest()


def deduplicate(roots):
    groups = {}
    for root in roots:
        for directory in ('catalogs', 'raw/gamma-catalog'):
            for path in (root / directory).glob('*.jsonl'):
                if path.is_symlink() or not stat.S_ISREG(path.stat().st_mode):
                    raise ValueError(f'not a regular immutable file: {path}')
                groups.setdefault((directory, path.name), []).append(path)
    saved = 0
    for paths in groups.values():
        source = paths[0]
        expected = digest(source)
        verified = [(path, path.stat(), digest(path)) for path in paths]
        if any(value != expected for _, _, value in verified):
            raise ValueError(f'immutable content differs: {source.name}')
        # Immutable catalog payloads must never be edited in place after sharing.
        for path, before, _ in verified:
            current = path.stat()
            if (current.st_ino, current.st_size, current.st_mtime_ns) != (
                    before.st_ino, before.st_size, before.st_mtime_ns):
                raise ValueError('file changed during verification')
            path.chmod(0o444)
        for path, before, _ in verified[1:]:
            if os.path.samefile(source, path):
                continue
            if source.stat().st_dev != before.st_dev:
                raise ValueError('deduplication must remain on one filesystem')
            temporary = path.with_name(path.name + '.dedup-pending')
            os.link(source, temporary)
            os.replace(temporary, path)
            fd = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
            saved += before.st_blocks * 512
            print(f'deduplicated {path} sha256={expected}', flush=True)
    print(f'reclaimed_allocated_bytes={saved}', flush=True)


if __name__ == '__main__':
    base = Path('/mnt/p44pro/marketcow-shadow-v3-runtime/linux')
    deduplicate([base / name for name in (
        'discovery-source-r1', 'dynamic-live-candidate-r1', 'scoped-live-source-r1')])
