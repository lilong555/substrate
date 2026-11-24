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

"""Check that recovery regressions detect a missing execution comparison."""

import json
import os
from pathlib import Path
import subprocess

root = Path.cwd()
artifacts = Path(os.environ["ARTIFACTS"])
source = root / "cmd/ateapi/internal/controlapi/workflow_worker_crash.go"
comparison = " || actor.GetStatus().GetWorkerAssignment().GetActorExecutionUid() != executionUID"
text = source.read_text()
assert text.count(comparison) == 1
mutant = artifacts / "crash_without_comparison.go"
mutant.write_text(text.replace(comparison, ""))
overlay = artifacts / "negative-overlay.json"
overlay.write_text(json.dumps({"Replace": {str(source): str(mutant)}}) + "\n")
log = artifacts / "negative-control.jsonl"
with log.open("w") as output:
    result = subprocess.run([
        "go", "test", "-race", "-count=1", "-json", "-overlay", str(overlay),
        "-run", "^TestReportActorCrashed(AcrossRecovery|AfterResumeRetry)$",
        "./cmd/ateapi/internal/controlapi/functionaltest",
    ], stdout=output, stderr=subprocess.STDOUT)
failures = set()
for line in log.read_text().splitlines():
    try:
        event = json.loads(line)
    except json.JSONDecodeError:
        continue
    if event.get("Action") == "fail" and event.get("Test"):
        failures.add(event["Test"])
expected = {
    "TestReportActorCrashedAcrossRecovery/delayed_report_after_recovery",
    "TestReportActorCrashedAcrossRecovery/retried_report_after_recovery",
    "TestReportActorCrashedAfterResumeRetry/run",
    "TestReportActorCrashedAfterResumeRetry/restore",
}
passed = result.returncode == 1 and expected <= failures
summary = {"passed": passed, "exit_code": result.returncode,
           "expected_failures": sorted(expected), "actual_failures": sorted(failures)}
(artifacts / "negative-control-summary.json").write_text(json.dumps(summary, indent=2) + "\n")
assert passed, summary
