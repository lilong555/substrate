"""Validate suspend replay descriptors against PostgreSQL and snapshottracker."""

import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time


BASE = "6a57479a7c6e9efb757da92419356edacaafe132"
WORKFLOW = "cmd/ateapi/internal/controlapi/workflow_suspend.go"
REGRESSION = "TestSuspendActor_RetryAfterTemplateRecreation"
CONTROL = "TestSuspendActorAsync_AteletPaused_ForwardsPreferredFidelity"


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def events(path):
    return [json.loads(line) for line in path.read_text().splitlines() if line.startswith("{")]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, required=True)
    parser.add_argument("--assets", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    repo, assets, output = args.repo.resolve(), args.assets.resolve(), args.output.resolve()
    records = {}
    source_paths = [WORKFLOW, "cmd/ateapi/internal/controlapi/workflow_suspend_test.go",
                    "cmd/ateapi/internal/controlapi/workflow_suspend_async_test.go"]
    sources = {name: digest(repo / name) for name in source_paths}
    baseline_file = output / "baseline-workflow.go"
    baseline_file.write_bytes(subprocess.check_output(["git", "show", BASE + ":" + WORKFLOW], cwd=repo))
    baseline_overlay = output / "baseline-overlay.json"
    baseline_overlay.write_text(json.dumps({"Replace": {str(repo / WORKFLOW): str(baseline_file)}}))

    def save():
        record = {
            "base": BASE, "recorded_at": datetime.now(timezone.utc).isoformat(),
            "source_sha256": sources, "baseline_workflow_sha256": digest(baseline_file),
            "patch_sha256": digest(assets / "candidate.patch"),
            "runner_sha256": digest(Path(__file__)),
            "assets_sha256": {name: digest(assets / name) for name in
                              ("controlapi_tracker_test.go", "tracker_server_test.go")},
            "environment": {key: os.environ.get(key) for key in
                            ("GOMAXPROCS", "GOMEMLIMIT", "REQUIRE_DOCKER")},
            "results": records,
        }
        (output / "result.json").write_text(json.dumps(record, indent=2) + "\n")

    def run(label, command, timeout=900, env=None):
        log = output / (label + ".log")
        print(f"::group::{label}", flush=True)
        print(" ".join(command), flush=True)
        start = time.monotonic()
        with log.open("w") as stream:
            result = subprocess.run(command, cwd=repo, env=env, stdout=stream,
                                    stderr=subprocess.STDOUT, timeout=timeout)
        records[label] = {"command": command, "exit_code": result.returncode,
                          "elapsed_seconds": round(time.monotonic() - start, 2),
                          "log_sha256": digest(log)}
        print(json.dumps(records[label]), flush=True)
        print("\n".join(log.read_text().splitlines()[-8:]), flush=True)
        print("::endgroup::", flush=True)
        save()
        return result.returncode, log

    def summarize(log):
        terminal = [event for event in events(log) if event.get("Test") and
                    event.get("Action") in ("pass", "fail", "skip")]
        return {"counts": dict(Counter(event["Action"] for event in terminal)),
                "top_level_counts": dict(Counter(event["Action"] for event in terminal
                                                 if "/" not in event["Test"])),
                "failures": [event["Test"] for event in terminal if event["Action"] == "fail"],
                "skips": [event["Test"] for event in terminal if event["Action"] == "skip"]}

    try:
        version, _ = run("go-version", ["go", "version"])
        assert version == 0
        code, log = run("format", ["gofmt", "-l", *source_paths])
        assert code == 0 and not log.read_text().strip(), "unformatted candidate"
        code, _ = run("diff-check", ["git", "diff", "--check"])
        assert code == 0

        code, log = run("baseline-regression", ["go", "test", "-race", "-p=4",
                         f"-overlay={baseline_overlay}", "./cmd/ateapi/internal/controlapi",
                         f"-run=^{REGRESSION}$", "-count=1", "-shuffle=on", "-timeout=10m", "-json"])
        summary = summarize(log)
        records["baseline-regression"]["tests"] = summary
        expected = {REGRESSION, REGRESSION + "/memory_to_rootfs", REGRESSION + "/rootfs_to_memory"}
        assert code == 1 and set(summary["failures"]) == expected, summary
        assert summary["counts"].get("pass") == 1 and not summary["skips"], summary
        save()

        code, log = run("fixed-regression", ["go", "test", "-race", "-p=4",
                         "./cmd/ateapi/internal/controlapi", f"-run=^({REGRESSION}|{CONTROL})$",
                         "-count=3", "-shuffle=on", "-timeout=10m", "-json"])
        summary = summarize(log)
        records["fixed-regression"]["tests"] = summary
        assert code == 0 and not summary["failures"] and not summary["skips"], summary
        assert summary["top_level_counts"].get("pass") == 6, summary
        save()

        tracker_overlay = output / "tracker-overlay.json"
        tracker_overlay.write_text(json.dumps({"Replace": {
            str(repo / "cmd/atelet/internal/snapshottracker/evidence_tracker_server_test.go"):
            str(assets / "tracker_server_test.go")}}))
        tracker_binary = output / "tracker.test"
        code, _ = run("build-tracker", ["go", "test", "-c", "-race", "-p=4",
                      f"-overlay={tracker_overlay}", "./cmd/atelet/internal/snapshottracker",
                      "-o", str(tracker_binary)])
        assert code == 0
        code, _ = run("tracker-protocol-control", [str(tracker_binary),
                      "-test.run=^TestEvidenceTrackerProtocol$", "-test.v", "-test.timeout=60s"])
        assert code == 0
        records["build-tracker"]["binary_sha256"] = digest(tracker_binary)

        for baseline in (True, False):
            label = "tracker-baseline" if baseline else "tracker-fixed"
            directory = output / label
            directory.mkdir()
            overlay = directory / "overlay.json"
            replacements = {str(repo / "cmd/ateapi/internal/controlapi/local_tracker_test.go"):
                            str(assets / "controlapi_tracker_test.go")}
            if baseline:
                replacements[str(repo / WORKFLOW)] = str(baseline_file)
            overlay.write_text(json.dumps({"Replace": replacements}))
            binary = directory / "controlapi.test"
            code, _ = run(label + "-build", ["go", "test", "-c", "-race", "-p=4",
                          f"-overlay={overlay}", "./cmd/ateapi/internal/controlapi", "-o", str(binary)])
            assert code == 0
            env = os.environ.copy()
            env["SUBSTRATE_TRACKER_READY_FILE"] = str(directory / "ready.json")
            env["SUBSTRATE_TRACKER_LOG_FILE"] = str(directory / "tracker.jsonl")
            with (directory / "server.log").open("w") as server_log:
                server = subprocess.Popen([str(tracker_binary), "-test.run=^TestEvidenceTrackerServer$",
                                           "-test.v", "-test.timeout=240s"], cwd=repo, env=env,
                                          stdout=server_log, stderr=subprocess.STDOUT)
                try:
                    deadline = time.monotonic() + 30
                    ready = directory / "ready.json"
                    while not ready.exists():
                        if server.poll() is not None or time.monotonic() > deadline:
                            raise RuntimeError("tracker did not become ready")
                        time.sleep(0.05)
                    env["SUBSTRATE_TRACKER_ADDR"] = json.loads(ready.read_text())["address"]
                    code, log = run(label, [str(binary), "-test.run=^TestLocalSuspendTrackerReplay$",
                                           "-test.v", "-test.timeout=180s"], timeout=200, env=env)
                finally:
                    server.terminate()
                    try:
                        server_code = server.wait(timeout=20)
                    except subprocess.TimeoutExpired:
                        server.kill()
                        server.wait()
                        raise
            transcript = log.read_text()
            requests = events(directory / "tracker.jsonl")
            counts = dict(Counter(event["action"] for event in requests))
            codes = dict(Counter(event["grpc_code"] for event in requests))
            observed_states = [line for line in transcript.splitlines() if "persisted_state=" in line]
            records[label].update({"actions": counts, "grpc_codes": codes,
                                   "states": observed_states, "server_exit_code": server_code,
                                   "binary_sha256": digest(binary),
                                   "tracker_log_sha256": digest(directory / "tracker.jsonl")})
            save()
            assert len(requests) == 6 and len(observed_states) == 3 and "--- SKIP:" not in transcript
            assert server_code == 0, server_code
            if baseline:
                assert code == 1 and counts == {"new_commit": 3, "committed_replay": 1, "rejected": 2}, counts
                assert codes.get("FailedPrecondition") == 2, codes
                assert sum("persisted_state=ACTOR_STATE_CRASHED" in line for line in observed_states) == 2
            else:
                assert code == 0 and counts == {"new_commit": 3, "committed_replay": 3}, counts
                assert codes == {"OK": 6}, codes
                assert all("persisted_state=ACTOR_STATE_SUSPENDED" in line for line in observed_states)

        code, log = run("affected-race", ["go", "test", "-race", "-p=4",
                         "./cmd/ateapi/internal/controlapi/...", "./cmd/ateapi/internal/store/...",
                         "./cmd/atelet/internal/snapshottracker", "-count=1", "-shuffle=on",
                         "-timeout=15m", "-json"], timeout=1200)
        summary = summarize(log)
        records["affected-race"]["tests"] = summary
        assert code == 0 and not summary["failures"] and not summary["skips"], summary
        assert summary["top_level_counts"].get("pass", 0) > 100, summary
        assert all(digest(repo / name) == value for name, value in sources.items()), "source drifted"
        records["completed"] = True
        save()
        print(json.dumps(records, indent=2), flush=True)
    finally:
        save()


if __name__ == "__main__":
    main()
