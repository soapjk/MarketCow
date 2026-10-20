#!/usr/bin/env python3
"""Export a source-only SDK candidate; no Git/network/publish/service actions."""
import argparse
from pathlib import Path
import shutil

REPOSITORY = Path(__file__).resolve().parents[1]
SOURCE_ROOTS = ("crates/marketcow-wire", "crates/marketcow-sdk", "python-native",
                "python/src", "python/tests", "python/examples", ".github/workflows")
SOURCE_FILES = ("Cargo.toml", "Cargo.lock", ".gitignore", "python/pyproject.toml",
                "python/uv.lock", "python/.gitignore")
SUFFIXES = {".rs", ".toml", ".lock", ".proto", ".py", ".pyi", ".yml", ".yaml"}
SKIP_PARTS = {"__pycache__", "target", "build", "dist", ".venv", ".git", ".corptie"}


def sdk_files(repository=REPOSITORY):
    source = repository / "sdk"
    names = set(SOURCE_FILES)
    for root in SOURCE_ROOTS:
        directory = source / root
        if directory.is_symlink() or not directory.is_dir():
            raise ValueError(f"missing or linked source directory: {root}")
        for path in directory.rglob("*"):
            relative = path.relative_to(source)
            if any(part in SKIP_PARTS or part.startswith(".venv") for part in relative.parts):
                continue
            if path.is_symlink():
                raise ValueError(f"linked SDK input: {relative}")
            if path.is_file() and (path.suffix in SUFFIXES or path.name == "py.typed"):
                names.add(relative.as_posix())
    for name in sorted(names):
        path = source / name
        if not path.is_file() or path.is_symlink():
            raise ValueError(f"missing or linked SDK input: {name}")
        yield name, path


def export(destination, repository=REPOSITORY):
    destination = Path(destination).absolute()
    # A candidate is immutable once made. Never merge/overwrite a checkout.
    if destination.exists() or destination.is_symlink():
        raise ValueError("destination must not exist")
    inputs = list(sdk_files(repository))
    license_file = repository / "LICENSE"
    if license_file.is_symlink() or not license_file.is_file():
        raise ValueError("missing or linked license")
    destination.mkdir(parents=True)
    for name, original in inputs:
        target = destination / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(original, target)
    shutil.copyfile(license_file, destination / "LICENSE")
    # Byte comparison in this explicit cold export, never a data hash.
    for name, original in inputs:
        if original.read_bytes() != (destination / name).read_bytes():
            raise RuntimeError(f"source changed during export: {name}")
    print(f"exported {len(inputs) + 1} source files to {destination}; no git or publish action")
    return len(inputs) + 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    export(parser.parse_args().output)
