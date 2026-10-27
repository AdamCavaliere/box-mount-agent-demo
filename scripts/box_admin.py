"""Seed or reset the demo's Box folder, from inside a sandbox (as boxsvc), so Box Mount syncs it.

    python scripts/box_admin.py seed [--booth]   # copy /app/seed into the mount (booth: contract + playbook only)
    python scripts/box_admin.py reset [--booth]  # remove case outputs and strays (booth: everything under Work/ and Reviewed/)

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


def reset(booth: bool = False) -> None:
    strays = [p.name for p in MOUNT.glob("sandbox-*.txt")]  # misplaced pulse files
    if booth:  # every run and every published memo
        cases = [f"{top}/{p.name}" for top in ("Work", "Reviewed") for p in (MOUNT / top).glob("*")]
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
