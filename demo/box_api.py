"""The coordinator's view of Box: file versions and server-side copies, through the Box API.

The coordinator has no Box Mount, so no file content ever syncs into its sandbox. It reads what
Box knows about each file (ID, version, SHA-1, who changed it) and copies exact versions from one
agent's folder to the other's on the Box side. The agents never get Box credentials: they only see
their own mounted folder.

The demo's Box folder (DEMO_BOX_FOLDER_ID):
    Extraction/   mounted by the Extractor:  Incoming/<contract>.pdf, Work/<case>/<run>/extraction.json
    Review/       mounted by the Reviewer:   Playbook/playbook.md, Work/<case>/<run>/... (memo, findings)
    Reviewed/     no sandbox mounts it:      approved memos, copied here by the coordinator

Paths below are relative to that folder. Credentials: the JWT app config at DEMO_BOX_JWT_CONFIG
(in the coordinator's sandbox) or secrets/box-jwt-config.json (on the host).
"""

import os
from pathlib import Path, PurePosixPath

from box_sdk_gen import BoxAPIError, BoxClient, BoxJWTAuth, JWTConfig
from box_sdk_gen.managers.files import CopyFileParent
from box_sdk_gen.managers.folders import CreateFolderParent

from .models import BoxVersion

WEB = "https://app.box.com"
FILE_FIELDS = ["name", "sha1", "size", "file_version", "modified_at", "modified_by"]

_client: BoxClient | None = None
_folders: dict[str, str] = {}  # folder path -> Box folder ID (folders are never renamed here)


def root_id() -> str:
    folder = os.environ.get("DEMO_BOX_FOLDER_ID", "")
    if not folder:
        raise RuntimeError("DEMO_BOX_FOLDER_ID is not set")
    return folder


def client() -> BoxClient:
    global _client
    if _client is None:
        path = os.environ.get("DEMO_BOX_JWT_CONFIG") or str(
            Path(__file__).resolve().parent.parent / "secrets" / "box-jwt-config.json")
        _client = BoxClient(BoxJWTAuth(JWTConfig.from_config_file(path)))
    return _client


def url(file_id: str) -> str:
    return f"{WEB}/file/{file_id}"


def _child(folder_id: str, name: str, kind: str):
    marker = None
    while True:
        page = client().folders.get_folder_items(folder_id, fields=["name", "type"], usemarker=True,
                                                 marker=marker, limit=1000)
        for item in page.entries or []:
            if item.name == name and str(getattr(item.type, "value", item.type)) == kind:
                return item
        marker = getattr(page, "next_marker", None)
        if not marker:
            return None


def folder_id(path: str, create: bool = False) -> str | None:
    """The Box folder ID at `path` ('' is the demo folder), creating missing folders if asked."""
    path = path.strip("/")
    if not path:
        return root_id()
    if path in _folders:
        return _folders[path]
    parent_path, _, name = path.rpartition("/")
    parent = folder_id(parent_path, create)
    if parent is None:
        return None
    item = _child(parent, name, "folder")
    if item is None:
        if not create:
            return None
        try:
            item = client().folders.create_folder(name, CreateFolderParent(id=parent))
        except BoxAPIError as err:  # created meanwhile (e.g. by a retried attempt)
            if err.response_info.status_code != 409:
                raise
            item = _child(parent, name, "folder")
    _folders[path] = item.id
    return item.id


def version(path: str) -> BoxVersion | None:
    """The current version of the file at `path`, or None if there's no such file."""
    parent_path, _, name = path.strip("/").rpartition("/")
    parent = folder_id(parent_path)
    item = _child(parent, name, "file") if parent else None
    if item is None:
        return None
    return _version(path, client().files.get_file_by_id(item.id, fields=FILE_FIELDS))


def _version(path: str, f) -> BoxVersion:
    by = f.modified_by
    return BoxVersion(path=path.strip("/"), file_id=f.id, version_id=f.file_version.id if f.file_version else "",
                      sha1=f.sha_1 or "", size=f.size or 0, modified_at=str(f.modified_at or ""),
                      modified_by=(by.name or by.login or "") if by else "")


def copy(source: BoxVersion, dest_path: str) -> BoxVersion:
    """Copy exactly `source`'s version to `dest_path`, server-side (no content passes through here).

    Safe to retry: if `dest_path` already holds that content, it is returned as is. If it holds
    something else, the copy goes next to it with a version suffix (never overwriting)."""
    dest_path = dest_path.strip("/")
    existing = version(dest_path)
    if existing is not None:
        if existing.sha1 == source.sha1:
            return existing
        stem, suffix = PurePosixPath(dest_path).stem, PurePosixPath(dest_path).suffix
        parent = str(PurePosixPath(dest_path).parent)
        n = 2
        while (existing := version(f"{parent}/{stem}_v{n}{suffix}")) is not None and existing.sha1 != source.sha1:
            n += 1
        dest_path = f"{parent}/{stem}_v{n}{suffix}"
        if existing is not None:
            return existing
    parent_path, _, name = dest_path.rpartition("/")
    parent = folder_id(parent_path, create=True)
    copied = client().files.copy_file(source.file_id, CopyFileParent(id=parent), name=name,
                                      version=source.version_id or None, fields=FILE_FIELDS)
    return _version(dest_path, copied)
