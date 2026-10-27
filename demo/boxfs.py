"""``BoxMountFS``: the agents' ``/box`` mount, over this sandbox's Box Mount directory.

An ``ActivityFileSystem``, so the harness runs every method as an activity
(``vfs.box-mount.read``, ``vfs.box-mount.write``, ...) on the worker polling the agent's task
queue, which is the sandbox that agent lives in. A script's ``open("/box/Incoming/x.pdf")``
therefore reads the file Box Mount synced into *that* sandbox, and a write lands in its mount
and is uploaded to Box by its daemon. Agents in other sandboxes see it once their own daemon
syncs it down (see ``handoff.await_handoff``).

Governance on top of Box's own permissions:

- An agent may only write the paths its config allows (one writer per file, so Box never has
  to make conflict copies). Anything else is a ``PermissionError`` the script sees.
- Writes refuse to run while the daemon isn't syncing. That error is retryable, so Temporal
  retries it instead of letting the agent write into a folder nobody is uploading.

A config is untrusted input (the console's file viewer sends one back), so it can only narrow
what an agent may do inside the mount, never point outside it.
"""

from __future__ import annotations

import errno
import hashlib
import os
from fnmatch import fnmatchcase
from pathlib import Path, PurePosixPath

from pydantic import BaseModel, Field

from temporal_agent_harness.harness import agent

from .sandbox import MOUNT_ROOT, mount_status, sandbox_name

# Plumbing folders in the Box folder that agents don't need to see.
HIDDEN = frozenset({"_sync", "_latency"})


class BoxMountConfig(BaseModel):
    """Which paths this agent may write, as globs relative to the Box folder."""

    writable: list[str] = Field(
        default_factory=list,
        description="e.g. ['Work/INV-20417/extraction.json']; empty means read-only.",
    )


def _hidden(name: str) -> bool:
    return name in HIDDEN or name.startswith(".")


class BoxMountFS(agent.ActivityFileSystem[BoxMountConfig], name="box-mount"):
    def __init__(self, config: BoxMountConfig) -> None:
        super().__init__(config)
        for pattern in config.writable:
            p = PurePosixPath(pattern)
            if p.is_absolute() or ".." in p.parts:
                raise ValueError(f"writable pattern '{pattern}' must be relative to the Box folder")
        self._root = MOUNT_ROOT.resolve()

    # -- helpers ---------------------------------------------------------------------------

    def _on_disk(self, path: PurePosixPath) -> Path:
        resolved = (self._root / path).resolve()
        if resolved != self._root and self._root not in resolved.parents:
            raise PermissionError(errno.EACCES, "Path is outside the Box folder", str(path))
        return resolved

    def _check_writable(self, path: PurePosixPath) -> None:
        if not any(fnmatchcase(str(path), pattern) for pattern in self.config.writable):
            allowed = ", ".join(self.config.writable) or "nothing (read-only)"
            raise PermissionError(
                errno.EACCES, f"This agent may only write: {allowed}", str(path)
            )

    def _require_sync(self) -> None:
        healthy, detail = mount_status()
        if not healthy:
            # Not an OSError, so it's retryable: Temporal retries until the daemon is back.
            raise RuntimeError(f"Box Mount is not syncing in {sandbox_name()}: {detail}")

    # -- reads -----------------------------------------------------------------------------

    async def stat(self, path: PurePosixPath) -> agent.FileStat | None:
        target = self._on_disk(path)
        try:
            found = target.stat()
        except FileNotFoundError:
            return None
        is_dir = target.is_dir()
        return agent.FileStat(is_dir=is_dir, size=0 if is_dir else found.st_size, mtime=found.st_mtime)

    async def read(self, path: PurePosixPath, offset: int = 0, length: int | None = None) -> bytes:
        with self._on_disk(path).open("rb") as f:
            f.seek(offset)
            return f.read(-1 if length is None else length)

    async def list(self, path: PurePosixPath) -> list[str]:
        return sorted(n for n in os.listdir(self._on_disk(path)) if not _hidden(n))

    # -- writes ----------------------------------------------------------------------------

    async def write(self, path: PurePosixPath, data: bytes) -> None:
        self._check_writable(path)
        self._require_sync()
        target = self._on_disk(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.is_file() and hashlib.sha256(target.read_bytes()).digest() == hashlib.sha256(data).digest():
            return  # a retried or replayed write that already landed: nothing to upload
        try:
            # In place (same inode), so Box records a new version of the same file.
            target.write_bytes(data)
        except PermissionError:
            # Box Mount makes files it downloads read-only to the group. This file was synced in
            # from another sandbox (e.g. a failed-over worker retrying a write), so replace it.
            # Box sees a new file rather than a version: acceptable for this rare path.
            tmp = target.with_name(f".{target.name}.{os.getpid()}.tmp")
            tmp.write_bytes(data)
            os.replace(tmp, target)

    async def mkdir(self, path: PurePosixPath) -> None:
        # Only folders that lead to something this agent may write.
        prefix = f"{path}/"
        if not any(pattern.startswith(prefix) for pattern in self.config.writable):
            self._check_writable(path)
        self._on_disk(path).mkdir(parents=True, exist_ok=True)

    async def delete(self, path: PurePosixPath) -> None:
        self._check_writable(path)
        self._require_sync()
        target = self._on_disk(path)
        target.rmdir() if target.is_dir() else target.unlink()

    async def rename(self, path: PurePosixPath, target: PurePosixPath) -> None:
        self._check_writable(path)
        self._check_writable(target)
        self._require_sync()
        self._on_disk(path).rename(self._on_disk(target))
