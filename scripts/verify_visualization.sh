#!/bin/sh
set -eu

repository_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)

cd "$repository_root/web"
npm ci
npm run build

cd "$repository_root"
MARKETCOW_HOME=$(mktemp -d) uv run python -m unittest discover -s tests -q
uv run ruff check src tests

cd "$repository_root/web"
npm run typecheck
npm test
npm run lint
npm audit

cd "$repository_root"
git diff --check
