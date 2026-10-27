"""Handing files between agents that share nothing but a Box folder.

The writer fingerprints what it wrote (``fingerprint``); the coordinator passes that
``FileRef`` on; the reader's first step is ``await_handoff``, which checks its OWN sandbox's Box
Mount for that exact version. Until Box Mount has synced it down, the activity fails with a
retryable error and Temporal retries it on a short backoff, so the wait is durable and every
check shows in the timeline.

Box Mount learns about remote changes from Box's event stream. If it misses a notification, a
file can sit unsynced until the next change in the folder. From the second check on, the reader
asks the sandbox supervisor to rewrite a small pulse file: the daemon uploads it, sees its own change
echo back, and re-lists remote changes, which picks up anything it missed.

No ``from __future__ import annotations`` (activity tool signatures are read concretely).
"""

import hashlib
import time
from pathlib import Path
from datetime import timedelta

from temporalio import activity
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError
from temporalio.workflow import ActivityConfig

from temporal_agent_harness.harness import agent

from .models import FileRef, HandoffReceipt
from .sandbox import MOUNT_ROOT, mount_status, sandbox_name

PULSE_REQUEST = Path("/run/boxmount/requests/pulse")  # the sandbox supervisor writes the pulse

HANDOFF_ACTIVITY = ActivityConfig(
    start_to_close_timeout=timedelta(seconds=15),
    retry_policy=RetryPolicy(
        initial_interval=timedelta(seconds=2),
        backoff_coefficient=1.5,
        maximum_interval=timedelta(seconds=5),
        maximum_attempts=72,  # ~5 minutes of checks
        non_retryable_error_types=["InvalidPath"],
    ),
)


def _relative(path: str) -> str:
    p = path.removeprefix("/box/").lstrip("/")
    if ".." in p.split("/"):
        raise ApplicationError(f"invalid path {path!r}", type="InvalidPath", non_retryable=True)
    return p


def _pulse() -> None:
    """Ask the sandbox supervisor to write this sandbox's pulse file (see entrypoint.sh). Never
    raises: a missed pulse only means waiting for the next one."""
    try:
        PULSE_REQUEST.write_text(f"{time.time():.3f}\n")
    except OSError as err:
        activity.logger.warning("could not request a sync pulse: %s", err)


@agent.activity_tool_defn(name="record_file_version_for_handoff", inherently_safe=True,
                           activity_config=ActivityConfig(start_to_close_timeout=timedelta(seconds=15)))
async def fingerprint(path: str) -> FileRef:
    """Fingerprint a file this agent wrote, so the next agent can wait for exactly this version."""
    rel = _relative(path)
    data = (MOUNT_ROOT / rel).read_bytes()
    return FileRef(path=rel, sha256=hashlib.sha256(data).hexdigest(), size=len(data), written_in=sandbox_name())


@agent.activity_tool_defn(name="wait_for_file_to_sync_from_box", inherently_safe=True, activity_config=HANDOFF_ACTIVITY)
async def await_handoff(ref: FileRef) -> HandoffReceipt:
    """Wait until another agent's output has synced into this sandbox through Box Mount."""
    attempt = activity.info().attempt
    rel = _relative(ref.path)
    target = MOUNT_ROOT / rel
    try:
        have = hashlib.sha256(target.read_bytes()).hexdigest()
    except FileNotFoundError:
        have = None
    if have == ref.sha256:
        return HandoffReceipt(path=rel, sha256=have, received_in=sandbox_name(),
                              written_in=ref.written_in, attempts=attempt)
    healthy, status = mount_status()
    if attempt >= 2 and healthy:
        _pulse()
    state = "not here yet" if have is None else f"older version here ({have[:8]})"
    raise ApplicationError(
        f"{rel} @ {ref.sha256[:8]} from {ref.written_in}: {state} in {sandbox_name()}"
        + ("" if healthy else f"; Box Mount not syncing: {status}"),
        type="HandoffNotSynced",
    )


@agent.activity_tool_defn(name="wait_for_edit_in_box", inherently_safe=True, activity_config=HANDOFF_ACTIVITY)
async def await_change(path: str, previous_sha256: str) -> FileRef:
    """Wait until a file in this sandbox's Box Mount differs from a version already seen, e.g. a
    person's edit in the Box web app syncing down. Returns the new version's fingerprint."""
    attempt = activity.info().attempt
    rel = _relative(path)
    data = (MOUNT_ROOT / rel).read_bytes()
    sha = hashlib.sha256(data).hexdigest()
    if sha != previous_sha256:
        return FileRef(path=rel, sha256=sha, size=len(data), written_in="Box (synced into " + sandbox_name() + ")")
    healthy, status = mount_status()
    if attempt >= 2 and healthy:
        _pulse()
    raise ApplicationError(
        f"{rel} unchanged in {sandbox_name()} (still {sha[:8]}): waiting for an edit in Box to sync"
        + ("" if healthy else f"; Box Mount not syncing: {status}"),
        type="HandoffNotSynced",
    )
