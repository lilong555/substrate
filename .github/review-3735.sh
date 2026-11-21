#!/usr/bin/env bash
set -euo pipefail
review_artifacts="$PWD/_artifacts"
mkdir -p "$review_artifacts"
git rev-parse HEAD^ > "$review_artifacts/tested-source.txt"
git fetch https://github.com/agent-substrate/substrate.git main:refs/remotes/origin/main
git worktree add --detach "$RUNNER_TEMP/review-source" HEAD^
cd "$RUNNER_TEMP/review-source"
if [[ "$1" == "race" ]]; then
  go test -race -count=3 -shuffle=on ./cmd/ateworker-gvisor 2>&1 | tee "$review_artifacts/gvisor-race.log"
  go test -race ./... 2>&1 | tee "$review_artifacts/all-race.log"
else
  hack/verify-all.sh 2>&1 | tee "$review_artifacts/verify.log"
fi
