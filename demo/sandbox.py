"""What this sandbox knows about itself: its name, and whether its Box Mount daemon is syncing.

The daemon runs as ``boxsvc`` and its state directory is private to that user, so the worker
(user ``agent``) can't run ``box-mount status`` itself. The entrypoint's supervisor runs it every
couple of seconds and writes the result to ``STATUS_FILE``; a stale or non-running status means
nothing written to the mount is reaching Box.
"""

import os
import platform
import socket
import time
from pathlib import Path

MOUNT_ROOT = Path(os.environ.get("BOX_MOUNT_DIR", "/mnt/box"))
STATUS_FILE = Path(os.environ.get("BOX_MOUNT_STATUS_FILE", "/run/boxmount/status"))
STATUS_MAX_AGE_S = 15.0


def sandbox_name() -> str:
    return socket.gethostname()


def machine() -> str:
    return f"{platform.system()} {platform.machine()}"


def mount_status() -> tuple[bool, str]:
    """(healthy, the daemon's last status line). Healthy = reported `status=running` recently."""
    try:
        age = time.time() - STATUS_FILE.stat().st_mtime
        line = STATUS_FILE.read_text().strip()
    except FileNotFoundError:
        return False, "no status reported yet (is the box-mount daemon starting?)"
    if age > STATUS_MAX_AGE_S:
        return False, f"status is {age:.0f}s old (supervisor stopped?): {line}"
    return "status=running" in line, line
