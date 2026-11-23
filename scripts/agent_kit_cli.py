"""`temporal-agent-kit` (serve, session-manager) with readable session workflow IDs.

The UI doesn't choose a session ID, so the Agent Kit mints `agent-session-<uuid>`. When a
create-session request has none, this fills in `<agent key>-<MMDD-HHMMSS>-<4 hex>` (e.g.
`box-relay-1015-142615-a3f9`), so the Temporal UI says what each session is and when it started.

    uv run python scripts/agent_kit_cli.py serve agents.toml ...
"""

import json
import sys
import tomllib
import uuid
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

def _agent_keys(argv: list[str]) -> dict[str, str]:
    """workflow_type -> registry key, from the agents.toml files named on the command line."""
    keys: dict[str, str] = {}
    for arg in argv:
        if arg.endswith(".toml") and Path(arg).is_file():
            for entry in tomllib.loads(Path(arg).read_text()).get("agents", []):
                keys[entry["workflow_type"]] = entry["key"]
    return keys


class ReadableSessionIds:
    """ASGI middleware: give POST /api/sessions a readable session_id when the caller sent none."""

    def __init__(self, app, keys: dict[str, str]) -> None:
        self.app, self.keys = app, keys

    async def __call__(self, scope, receive, send):
        if not (scope["type"] == "http" and scope["method"] == "POST" and scope["path"] == "/api/sessions"):
            return await self.app(scope, receive, send)
        body, more = b"", True
        while more:
            message = await receive()
            body += message.get("body", b"")
            more = message.get("more_body", False)
        try:
            data = json.loads(body)
        except ValueError:
            data = None
        if isinstance(data, dict) and not data.get("session_id"):
            key = self.keys.get(data.get("agent_workflow_type", ""), "agent")
            data["session_id"] = f"{key}-{datetime.now():%m%d-%H%M%S}-{uuid.uuid4().hex[:4]}"
            body = json.dumps(data).encode()
        delivered = False

        async def replay():
            nonlocal delivered
            if not delivered:
                delivered = True
                return {"type": "http.request", "body": body, "more_body": False}
            return await receive()

        headers = [(k, v) for k, v in scope["headers"] if k != b"content-length"]
        headers.append((b"content-length", str(len(body)).encode()))
        return await self.app(dict(scope, headers=headers), replay, send)


from temporal_agent_kit.web import serve as _serve  # noqa: E402

_create_app = _serve.create_app


def _create_app_with_readable_ids(*registry_paths):
    app = _create_app(*registry_paths)
    app.add_middleware(ReadableSessionIds, keys=_agent_keys([str(p) for p in registry_paths]))
    return app


_serve.create_app = _create_app_with_readable_ids

from temporal_agent_kit.web.cli import main  # noqa: E402

main()
