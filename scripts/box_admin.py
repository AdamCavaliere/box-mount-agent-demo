"""Seed or reset the demo's Box folder, from inside a sandbox (as boxsvc), so Box Mount syncs it.

    python scripts/box_admin.py seed [--booth]   # copy /app/seed into the mount (booth: contract + playbook only)
    python scripts/box_admin.py reset [--booth]  # remove case outputs, strays and stale _sync/ pulse files (booth: everything under Work/ and Reviewed/)

Removed files go to the Box trash, where they can be restored. Only `just reset` should run
`reset`: Box Mount 0.6.0 can map a re-created folder (same inode) to the trashed Box folder in
OTHER sandboxes, so the recipe deletes from one fresh sandbox and then re-creates all of them.
"""

import shutil
import sys
from pathlib import Path

MOUNT = Path("/mnt/box")
SEED = Path("/app/seed")
CASE = "INV-20417"
# Test files, plus strays Box Mount misplaced at the top level after the inode-reuse bug.
SCRATCH = ["Work/_latency", "Work/_acl_test", "sandbox-hello.txt", "extraction.json",
           "compliance.json", "review_memo.md", "decision.md"]


def seed(booth: bool = False) -> None:
    for src in sorted(SEED.rglob("*")):
        if booth and "Invoice" in src.name:
            continue
        if src.is_file():
            dst = MOUNT / src.relative_to(SEED)
            if dst.is_file() and dst.read_bytes() == src.read_bytes():
                print(f"unchanged {dst.relative_to(MOUNT)}")
                continue  # identical: don't make Box record a pointless new version
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(src, dst)
            print(f"restored {dst.relative_to(MOUNT)}")
    (MOUNT / "Work").mkdir(exist_ok=True)
    (MOUNT / "Reviewed").mkdir(exist_ok=True)


# Sandboxes that exist today. Their own pulse files in _sync/ are kept (rewritten in place by their
# supervisors); any other pulse file belongs to a removed sandbox (e.g. compliance-b, reviewer-b).
CURRENT_SANDBOXES = {
    "sandbox-coordinator", "sandbox-extractor", "sandbox-compliance", "sandbox-reviewer",
    "sandbox-booth-coordinator", "sandbox-booth-extractor", "sandbox-booth-reviewer",
}


def stale_pulse_files() -> list[str]:
    """_sync/ files from removed sandboxes, and conflict copies Box Mount made of pulse files."""
    sync = MOUNT / "_sync"
    if not sync.is_dir():
        return []
    return [f"_sync/{p.name}" for p in sorted(sync.iterdir())
            if p.is_file() and (".conflict." in p.name or p.stem not in CURRENT_SANDBOXES)]


BOOTH_TOP_LEVEL = {"Incoming", "Playbook", "Work", "Reviewed", "_sync"}


def reset(booth: bool = False) -> None:
    strays = [p.name for p in MOUNT.glob("sandbox-*.txt")]  # misplaced pulse files
    strays += stale_pulse_files()
    if booth:  # every run and every published memo
        cases = [f"{top}/{p.name}" for top in ("Work", "Reviewed") for p in (MOUNT / top).glob("*")]
        # The booth folder holds only these; anything else at the top was misplaced by Box Mount.
        strays += [p.name for p in MOUNT.iterdir() if p.name not in BOOTH_TOP_LEVEL]
    else:
        cases = [f"Work/{CASE}", f"Reviewed/{CASE}"]
    for rel in [*cases, *SCRATCH, *strays]:
        target = MOUNT / rel
        if target.is_dir():
            shutil.rmtree(target)
        elif target.exists():
            target.unlink()
        else:
            continue
        print(f"removed {rel}")


if __name__ == "__main__":
    {"seed": seed, "reset": reset}[sys.argv[1]](booth="--booth" in sys.argv[2:])
