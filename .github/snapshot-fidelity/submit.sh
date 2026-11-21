#!/usr/bin/env bash
set -euo pipefail

check="$1"
artifacts="$PWD/_artifacts"
source="$RUNNER_TEMP/snapshot-fidelity-submit"
mkdir -p "$artifacts"
git fetch https://github.com/lilong555/substrate.git fix/suspend-persisted-fidelity
git fetch https://github.com/agent-substrate/substrate.git main:refs/remotes/upstream/main
git clone --shared --no-checkout "$PWD" "$source"
git -C "$source" checkout --detach "$SOURCE_REVISION"
git -C "$source" fetch https://github.com/agent-substrate/substrate.git main:refs/remotes/upstream/main
cd "$source"
git rev-parse HEAD > "$artifacts/source-commit.txt"
sha256sum cmd/ateapi/internal/controlapi/workflow_suspend{,_test,_async_test}.go > "$artifacts/source-sha256.txt"

if [[ "$check" == race ]]; then
  set +e
  go test -race -p=4 ./... -count=1 -timeout=15m -json > "$artifacts/race.jsonl" 2>&1
  result=$?
  set -e
  printf '%s\n' "$result" > "$artifacts/exit-code.txt"
  tail -n 20 "$artifacts/race.jsonl"
else
  set +e
  bash hack/verify-all.sh > "$artifacts/verify.log" 2>&1
  result=$?
  set -e
  printf '%s\n' "$result" > "$artifacts/exit-code.txt"
  tail -n 70 "$artifacts/verify.log"
fi

git diff --exit-code > "$artifacts/generated.patch"
exit "$result"
