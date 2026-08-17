#!/usr/bin/env python3
"""Run the Redis module in a real, isolated HLDS/Metamod/AMXX process."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time


ROOT = Path(__file__).resolve().parents[2]
COMPOSE = Path(__file__).with_name("compose.yml")
PROJECT = "redis-module-runtime"


def run(command: list[str], env: dict[str, str], timeout: int = 300, check: bool = True) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        command,
        cwd=ROOT,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=timeout,
        check=False,
    )
    if check and result.returncode != 0:
        raise RuntimeError(f"Command failed ({result.returncode}): {' '.join(command)}\n{result.stdout}")
    return result


def compose(arguments: list[str], env: dict[str, str], **kwargs: object) -> subprocess.CompletedProcess[str]:
    return run(
        ["docker", "compose", "-f", str(COMPOSE), "-p", PROJECT, *arguments],
        env,
        **kwargs,
    )


def rcon(command: str, env: dict[str, str]) -> str:
    return compose(
        ["run", "--rm", "--no-deps", "controller", "python", "/tests/rcon.py", command],
        env,
        timeout=30,
    ).stdout


def logs(env: dict[str, str]) -> str:
    return compose(["logs", "--no-color", "hlds"], env, timeout=30).stdout


def wait_for_log(pattern: str, env: dict[str, str], minimum: int = 1, timeout: int = 60) -> str:
    deadline = time.monotonic() + timeout
    compiled = re.compile(pattern)
    latest = ""
    while time.monotonic() < deadline:
        latest = logs(env)
        if len(compiled.findall(latest)) >= minimum:
            return latest
        time.sleep(1)
    raise RuntimeError(f"Timed out waiting for /{pattern}/ (minimum={minimum}).\n{latest[-8000:]}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--artifact", required=True, type=Path)
    parser.add_argument("--report", type=Path, default=ROOT / "artifacts" / "redis-runtime-report.json")
    args = parser.parse_args()

    artifact = args.artifact.resolve()
    if not artifact.is_file() or artifact.suffix != ".so":
        parser.error("--artifact must identify the built Redis .so file")
    if ROOT not in artifact.parents:
        parser.error("--artifact must stay inside the repository checkout")

    env = os.environ.copy()
    env["REDIS_MODULE_PATH"] = str(artifact)
    env["REDIS_RUNTIME_PROJECT"] = PROJECT
    phases: list[dict[str, object]] = []
    started = time.time()

    try:
        compose(["up", "-d", "--build", "redis", "hlds"], env, timeout=1800)
        wait_for_log(
            r"\[Redis Runtime\]\[CONNECTION\] request=42000 status=0 error=none",
            env,
            timeout=120,
        )
        rcon("amxx modules", env)

        rcon("redis_runtime_probe", env)
        wait_for_log(r"\[Redis Runtime\]\[PROBE_CALLBACK\] result=PASS", env)
        phases.append({"name": "module_load_and_callback", "status": "passed"})

        rcon("redis_xadd_validate", env)
        wait_for_log(r"\[Redis XADD Validation\]\[SUMMARY\].*result=PASS", env, timeout=60)
        phases.append({"name": "xadd_contract", "status": "passed"})

        compose(["stop", "redis"], env, timeout=60)
        time.sleep(3)
        rcon("redis_runtime_backpressure", env)
        wait_for_log(r"\[Redis Runtime\]\[BACKPRESSURE\] result=PASS", env)
        compose(["start", "redis"], env, timeout=60)

        deadline = time.monotonic() + 75
        recovered = False
        while time.monotonic() < deadline:
            rcon("redis_runtime_status", env)
            if re.search(r"\[Redis Runtime\]\[RECOVERY\] result=PASS", logs(env)):
                recovered = True
                break
            time.sleep(2)
        if not recovered:
            raise RuntimeError("Redis queue did not drain cleanly after reconnection")
        phases.append({"name": "outage_backpressure_and_recovery", "status": "passed"})

        rcon("redis_runtime_restore_defaults", env)
        wait_for_log(r"\[Redis Runtime\]\[DEFAULTS\] result=PASS", env)
        rcon("changelevel de_dust2", env)
        time.sleep(5)
        rcon("redis_xadd_validate", env)
        wait_for_log(
            r"\[Redis XADD Validation\]\[SUMMARY\].*result=PASS",
            env,
            minimum=2,
            timeout=75,
        )
        phases.append({"name": "map_change_callback_lifecycle", "status": "passed"})

        compose(["exec", "-T", "redis", "redis-cli", "XLEN", "amxx:test:runtime"], env, timeout=30)
        status = "passed"
        error = None
    except Exception as exception:
        status = "failed"
        error = str(exception)
        print(error, file=sys.stderr)
    finally:
        diagnostic_logs = logs(env) if compose(["ps", "-q", "hlds"], env, check=False).stdout.strip() else ""
        compose(["down", "--volumes", "--remove-orphans"], env, timeout=120, check=False)

    report = {
        "suite": "redis_module_hlds_runtime",
        "status": status,
        "artifact": str(artifact.relative_to(ROOT)),
        "duration_seconds": round(time.time() - started, 3),
        "phases": phases,
        "error": error,
        "hlds_log_tail": diagnostic_logs[-12000:],
    }
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({k: v for k, v in report.items() if k != "hlds_log_tail"}, indent=2))
    return 0 if status == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
