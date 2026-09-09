"""Build a local source bundle and exact hash manifest; never upload or install."""

import argparse
import hashlib
import json
from pathlib import Path
import tarfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-directory", type=Path, required=True)
    args = parser.parse_args()
    if not args.output_directory.is_absolute():
        raise ValueError("absolute new output directory required")
    root = Path(__file__).resolve().parents[1]
    paths = sorted((root / "src/marketcow").rglob("*.py"))
    paths += sorted((root / "tests").glob("test_catalog_*.py"))
    paths += [
        root / name
        for name in (
            "tools/catalog-capture/Cargo.toml",
            "tools/catalog-capture/Cargo.lock",
            "crates/marketcowd/examples/catalog_capture.rs",
            "pyproject.toml",
            "docs/catalog-incremental-development.md",
            "docs/catalog-refresh-worker.example.json",
            "scripts/catalog_changes.py",
        )
    ]
    paths = sorted(set(paths))
    entries = []
    for path in paths:
        if path.is_symlink() or not path.is_file():
            raise ValueError(f"not a regular source file: {path.relative_to(root)}")
        with path.open("rb") as stream:
            sha = hashlib.file_digest(stream, "sha256").hexdigest()
        entries.append(dict(path=str(path.relative_to(root)), bytes=path.stat().st_size, sha256=sha))
    args.output_directory.mkdir()
    archive = args.output_directory / "catalog-maintenance-r1.tar.gz"
    with tarfile.open(archive, "w:gz") as bundle:
        for path, item in zip(paths, entries):
            bundle.add(path, arcname=item["path"], recursive=False)
    with tarfile.open(archive, 'r:gz') as bundle:
        for item in entries:
            stream = bundle.extractfile(item['path'])
            if stream is None:
                raise ValueError('bundle entry missing')
            with stream:
                if hashlib.file_digest(stream, 'sha256').hexdigest() != item['sha256']:
                    raise ValueError('source changed during packaging')
    with archive.open("rb") as stream:
        sha = hashlib.file_digest(stream, "sha256").hexdigest()
    manifest = dict(
        schema_version="marketcow.catalog-source-bundle.v1",
        archive=archive.name,
        archive_sha256=sha,
        source_included=True,
        files=entries,
        online_deployment=False,
        official_capture_complete=False,
    )
    with (args.output_directory / "manifest.json").open("x") as stream:
        json.dump(manifest, stream, ensure_ascii=False, indent=2)
    print(
        json.dumps(
            dict(
                output=str(args.output_directory),
                files=len(entries),
                archive_bytes=archive.stat().st_size,
                archive_sha256=sha,
            )
        )
    )


if __name__ == "__main__":
    main()
