#!/usr/bin/env bash
set -euo pipefail

assets="$PWD/.github/snapshot-fidelity"
artifacts="$PWD/_artifacts"
source="$RUNNER_TEMP/snapshot-fidelity-source"
mkdir -p "$artifacts"
git worktree add --detach "$source" 6a57479a7c6e9efb757da92419356edacaafe132
git -C "$source" apply --check "$assets/candidate.patch"
git -C "$source" apply "$assets/candidate.patch"
python3 "$assets/validate.py" --repo "$source" --assets "$assets" --output "$artifacts"
