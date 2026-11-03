"""Fewer workflow-stream events in Temporal history, so the Temporal UI stays readable.

The harness streams turn events (model text, tool starts/ends) into each agent workflow and its
subscribers poll for them. Every activity-side flush is a Signal and every poll that returns is an
Update, each with its own workflow task: with the defaults (100 ms model flushes, 50 ms tool
flushes, 10 ms poll cooldown) a specialist's history was ~1,600 events for ~40 real activities.

DEMO_STREAM_MODE (read by the workers and by scripts/harness_cli.py):
  quiet  (default) model text flushed every 2 s, a tool's start and end flushed together,
         subscribers poll at most once a second. The UI updates in small jumps.
  off    as quiet, and model calls are not streamed at all: each turn's text appears when the
         model call finishes.
  minimal  as off, and subscribers poll at most every 5 s. The cleanest Temporal timeline (the
         booth default); the Harness UI updates in 5-second steps.
  live   the harness defaults: smooth, token-by-token UI, very busy history.
"""

import os
from datetime import timedelta

MODE = os.environ.get("DEMO_STREAM_MODE", "quiet").lower()
if MODE not in ("live", "quiet", "off", "minimal"):
    raise SystemExit(f"DEMO_STREAM_MODE must be live, quiet, off or minimal, got {MODE!r}")

STREAM_MODEL_CALLS = MODE in ("live", "quiet")
MODEL_FLUSH = timedelta(milliseconds=100) if MODE == "live" else timedelta(seconds=2)
# Longer than any tool runs, so a tool's start and end go out in one flush when it finishes.
TOOL_FLUSH = timedelta(seconds=30)
MIN_POLL_COOLDOWN = timedelta(seconds=5) if MODE == "minimal" else timedelta(seconds=1)


def install() -> None:
    """Apply the tool-flush and poll-cooldown settings to this process. Call before connecting."""
    if MODE == "live":
        return
    from temporalio.contrib.workflow_streams import WorkflowStreamClient

    from temporal_agent_harness.harness import agent_workflow

    # Read at call time by AgentWorkflowRunner.publisher_from_activity (activity tools' events).
    agent_workflow.DEFAULT_PUBLISH_BATCH_INTERVAL = TOOL_FLUSH

    # The harness subscribers (the UI's stream cursor, and a parent waiting on a subagent) pass a
    # 10 ms cooldown; raise it so each poll collects a batch instead of a single event.
    original = WorkflowStreamClient.subscribe
    if getattr(original, "_demo_quiet", False):
        return

    def subscribe(self, *args, poll_cooldown: timedelta = MIN_POLL_COOLDOWN, **kwargs):
        return original(self, *args, poll_cooldown=max(poll_cooldown, MIN_POLL_COOLDOWN), **kwargs)

    subscribe._demo_quiet = True  # type: ignore[attr-defined]
    WorkflowStreamClient.subscribe = subscribe  # type: ignore[method-assign]
