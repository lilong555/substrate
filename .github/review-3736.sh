#!/usr/bin/env bash
# Copyright 2026 The Agent Substrate Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
set -euo pipefail
script_dir="$(cd "$(dirname "$0")" && pwd)"
export ARTIFACTS="$PWD/_artifacts"
mkdir -p "$ARTIFACTS"
git rev-parse HEAD^ > "$ARTIFACTS/tested-source.txt"
git fetch https://github.com/agent-substrate/substrate.git main:refs/remotes/origin/main
git worktree add --detach "$RUNNER_TEMP/review-source" HEAD^
cd "$RUNNER_TEMP/review-source"
git diff --exit-code "$(cat "$ARTIFACTS/tested-source.txt")" HEAD
if [[ "$1" == race ]]; then
  go test -race -count=3 -shuffle=on -json \
    -run 'TestReportActorCrashed|TestCrashActorReportedByWorker|TestResumeActorExecutionUID|TestWatchSandboxReportsItsExecutionAfterRestart|TestWatchVMReportsItsExecutionAfterRestart|TestReportCrashed' \
    ./cmd/ateapi/internal/controlapi ./cmd/ateapi/internal/controlapi/functionaltest \
    ./cmd/ateapi/internal/workerservice ./cmd/atelet ./cmd/ateworker-gvisor \
    ./cmd/ateworker-microvm ./internal/ateworker/register > "$ARTIFACTS/regressions.jsonl" 2>&1
  python3 "$script_dir/validate-crash-identity.py"
  go test -race -json ./... > "$ARTIFACTS/all-race.jsonl" 2>&1
else
  hack/verify-all.sh 2>&1 | tee "$ARTIFACTS/verify.log"
fi
git diff --exit-code
test -z "$(git ls-files --others --exclude-standard)"
