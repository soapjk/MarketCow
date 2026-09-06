"""Bound diagnostic output, forward shutdown, preserve the child's exit status."""
import argparse
import os
from pathlib import Path
import signal
import subprocess


def run(path, limit, command):
    path = Path(path)
    paths = [path, Path(str(path)+'.1'), Path(str(path)+'.2')]
    if any(p.is_symlink() for p in paths):
        raise ValueError('diagnostic log symlink rejected')
    path.parent.mkdir(parents=True, exist_ok=True)
    child = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    def forward(signum, _frame):
        if child.poll() is None:
            child.send_signal(signum)
    signal.signal(signal.SIGINT, forward)
    signal.signal(signal.SIGTERM, forward)
    output = path.open('ab', buffering=0)
    try:
        size = path.stat().st_size
        while True:
            chunk = os.read(child.stdout.fileno(), min(65536, limit))
            if not chunk:
                break
            if size + len(chunk) > limit:
                output.close()
                if paths[1].exists():
                    os.replace(paths[1], paths[2])
                os.replace(path, paths[1])
                output = path.open('wb', buffering=0)
                size = 0
            output.write(chunk)
            size += len(chunk)
        return child.wait()
    finally:
        output.close()
        if child.poll() is None:
            child.terminate()
            try:
                child.wait(timeout=5)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--log', required=True)
    parser.add_argument('--max-bytes', required=True, type=int)
    parser.add_argument('command', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.max_bytes < 65536 or not args.command or args.command[0] != '--':
        parser.error('explicit byte budget >=65536 and -- command required')
    os.umask(0o077)
    result = run(args.log, args.max_bytes, args.command[1:])
    raise SystemExit(result if result >= 0 else 128-result)
