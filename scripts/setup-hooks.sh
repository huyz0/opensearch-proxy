#!/usr/bin/env bash
# Point git at the repo's tracked hooks. Run once after cloning.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
git config core.hooksPath .githooks
chmod +x .githooks/* 2>/dev/null || true
echo "hooks installed: core.hooksPath -> .githooks"
