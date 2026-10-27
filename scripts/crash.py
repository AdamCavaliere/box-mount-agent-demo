"""The crash moment: kill -9 an agent's worker while its model call is in flight.

    uv run python scripts/crash.py booth-reviewer      # or extractor, compliance, booth-extractor, ...

Waits (up to 3 minutes) until the agent running in that sandbox has a model call in progress, then
kills the worker process. The sandbox's supervisor restarts it in about a second, and Temporal
retries the interrupted model call there once its heartbeat times out (5 s): the agent carries on
from that exact step, and nothing it already wrote to Box is redone.
"""

import json
import os
import subprocess
import sys
import time

ROLE_TYPES = {"extractor": "ExtractorAgent", "compliance": "ComplianceAgent", "reviewer": "ContractReviewerAgent"}


def temporal(*args: str):
    out = subprocess.run(["temporal", *args, "-o", "json"], capture_output=True, text=True, check=True).stdout
    return json.loads(out or "null")


def main() -> None:
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    sandbox = sys.argv[1]
    booth = sandbox.startswith("booth-")
    role = sandbox.removeprefix("booth-")
    if role not in ROLE_TYPES:
        sys.exit(f"crash works on agent sandboxes: {', '.join(ROLE_TYPES)} (prefix booth- for the booth)")
    queue = f"{'booth' if booth else 'box'}-{role}"
    os.environ.setdefault("TEMPORAL_ADDRESS", "127.0.0.1:7233")
    print(f"waiting for {ROLE_TYPES[role]} on {queue} to start a model call...", flush=True)
    deadline = time.time() + 180
    while time.time() < deadline:
        runs = temporal("workflow", "list", "--limit", "1", "--query",
                        f"WorkflowType='{ROLE_TYPES[role]}' AND TaskQueue='{queue}' AND ExecutionStatus='Running'")
        for run in runs or []:
            wf = run["execution"]["workflowId"]
            for act in temporal("workflow", "describe", "-w", wf).get("pendingActivities", []):
                name = act["activityType"]["name"]
                if "model" in name and act.get("state", "").endswith("STARTED"):
                    worker = act.get("lastWorkerIdentity", "")
                    subprocess.run(["docker", "exec", f"sandbox-{sandbox}", "pkill", "-9", "-f", "[d]emo.worker"], check=False)
                    print(f"{time.strftime('%H:%M:%S')}  killed {worker} (kill -9) during {name}, attempt {act.get('attempt')}")
                    print(f"           workflow {wf}: watch the same call resume as the next attempt in the Temporal UI")
                    return
        time.sleep(0.5)
    sys.exit("no model call in flight within 3 minutes: start a run first")


if __name__ == "__main__":
    main()
