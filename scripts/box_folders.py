"""The demo's Box folder, managed from your Mac through the Box API (secrets/box-jwt-config.json).

    uv run python scripts/box_folders.py setup           # once: create the agents' folders, restore the seed
    uv run python scripts/box_folders.py reset           # clear runs and published memos, restore the seed
    uv run python scripts/box_folders.py playbook-reset  # put the original playbook back (after add-on A)
    uv run python scripts/box_folders.py tree            # what is in Box now, with versions

The demo folder (DEMO_BOX_FOLDER_ID in .env) holds one folder per agent, so each agent's Box
Mount sees only its own files:

    Extraction/   the Extractor's mount: Incoming/<contract>.pdf, Work/
    Review/       the Reviewer's mount:  Playbook/playbook.md, Work/
    Reviewed/     published memos (no sandbox mounts it)

`setup` writes the two folder IDs to .env (DEMO_EXTRACTION_FOLDER_ID, DEMO_REVIEW_FOLDER_ID) and
moves anything left at the top from the old single-folder layout to the Box trash. Deleted items
go to the Box trash, where they can be restored. Run `reset` only while the agents' sandboxes are
down (the recipes do that): Box Mount can map a re-created folder to the trashed one.
"""

import hashlib
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
ENV = ROOT / ".env"
SEED = ROOT / "seed"
CONTRACT = "Halvorsen_Master_Supply_Agreement.pdf"


def _env() -> None:
    import os
    for line in ENV.read_text().splitlines():
        key, sep, value = line.partition("=")
        if sep and key.startswith("DEMO_") and value.strip():
            os.environ.setdefault(key.strip(), value.strip())


_env()
from box_sdk_gen import BoxAPIError  # noqa: E402
from box_sdk_gen.managers.uploads import UploadFileAttributes, UploadFileAttributesParentField, UploadFileVersionAttributes  # noqa: E402

from demo import box_api  # noqa: E402

LAYOUT = ["Extraction", "Extraction/Incoming", "Extraction/Work", "Review", "Review/Playbook", "Review/Work", "Reviewed"]
SEEDS = {f"Extraction/Incoming/{CONTRACT}": SEED / "Incoming" / CONTRACT,
         "Review/Playbook/playbook.md": SEED / "Playbook" / "playbook.md"}
TOP = {"Extraction", "Review", "Reviewed"}


def _items(folder_id: str):
    return box_api.client().folders.get_folder_items(folder_id, fields=["name", "type"], limit=1000).entries or []


def _kind(item) -> str:
    return str(getattr(item.type, "value", item.type))


def _trash(item, label: str) -> None:
    if _kind(item) == "folder":
        box_api.client().folders.delete_folder_by_id(item.id, recursive=True)
    else:
        box_api.client().files.delete_file_by_id(item.id)
    print(f"trashed {label}")


def restore(path: str) -> None:
    """Upload the seed file to `path`, as a new version if Box has different content."""
    src = SEEDS[path]
    data = src.read_bytes()
    current = box_api.version(path)
    if current is not None and current.sha1 == hashlib.sha1(data).hexdigest():
        print(f"unchanged {path} (v{current.version_id})")
        return
    parent = box_api.folder_id(path.rpartition("/")[0], create=True)
    try:
        with src.open("rb") as f:
            if current is None:
                box_api.client().uploads.upload_file(
                    UploadFileAttributes(name=src.name, parent=UploadFileAttributesParentField(id=parent)), f)
            else:
                box_api.client().uploads.upload_file_version(current.file_id, UploadFileVersionAttributes(name=src.name), f)
    except BoxAPIError as err:
        if "locked" in str(err).lower():
            sys.exit(f"Box refused {path}: it is LOCKED in Box. Unlock it in the Box web app (… > Unlock, "
                     "or close it in Box Edit), then rerun.")
        raise
    print(f"restored {path} (v{box_api.version(path).version_id})")


def _write_env(values: dict[str, str]) -> None:
    lines = ENV.read_text().splitlines()
    for key, value in values.items():
        lines = [line for line in lines if not line.startswith(f"{key}=")]
        lines.append(f"{key}={value}")
    ENV.write_text("\n".join(lines) + "\n")


def setup() -> None:
    if not SEEDS[f"Extraction/Incoming/{CONTRACT}"].is_file():
        sys.exit("no seed files: run `uv run --group seed python scripts/make_seed.py` first")
    for path in LAYOUT:
        box_api.folder_id(path, create=True)
    for item in _items(box_api.root_id()):
        if item.name not in TOP:  # the old single-folder layout (Incoming/, Playbook/, Work/, _sync/)
            _trash(item, item.name)
    for path in SEEDS:
        restore(path)
    ids = {"DEMO_EXTRACTION_FOLDER_ID": box_api.folder_id("Extraction"),
           "DEMO_REVIEW_FOLDER_ID": box_api.folder_id("Review")}
    _write_env(ids)
    print("wrote to .env: " + ", ".join(f"{k}={v}" for k, v in ids.items()))


def reset() -> None:
    for area in ("Extraction/Work", "Review/Work", "Reviewed"):
        for item in _items(box_api.folder_id(area, create=True)):
            _trash(item, f"{area}/{item.name}")
    for area in ("Extraction", "Review"):  # strays next to the expected folders (keep the mounts' _sync/)
        keep = {"Incoming", "Work", "_sync"} if area == "Extraction" else {"Playbook", "Work", "_sync"}
        for item in _items(box_api.folder_id(area)):
            if item.name not in keep:
                _trash(item, f"{area}/{item.name}")
    for path in SEEDS:
        restore(path)


def playbook_reset() -> None:
    restore("Review/Playbook/playbook.md")


def tree(path: str = "", depth: int = 0) -> None:
    for item in sorted(_items(box_api.folder_id(path) or box_api.root_id()), key=lambda i: i.name):
        child = f"{path}/{item.name}".strip("/")
        if _kind(item) == "folder":
            print(f"{'  ' * depth}{item.name}/")
            tree(child, depth + 1)
        else:
            v = box_api.version(child)
            print(f"{'  ' * depth}{item.name}  v{v.version_id}  {v.sha1[:8]}  {v.modified_by}")


if __name__ == "__main__":
    commands = {"setup": setup, "reset": reset, "playbook-reset": playbook_reset, "tree": tree}
    if len(sys.argv) != 2 or sys.argv[1] not in commands:
        sys.exit(__doc__)
    commands[sys.argv[1]]()
