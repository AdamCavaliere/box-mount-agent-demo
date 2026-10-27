"""`temporal-agent-harness` (serve, session-manager) with the demo's settings applied:

- quiet stream mode (see demo/quiet_streams.py);
- a separate session-manager per UI when DEMO_SESSION_MANAGER_ID is set (the booth UI uses
  `booth-session-manager`): the manager keeps the agent list it started with, so the booth UI
  needs its own to offer only Box Relay;
- readable session workflow ids: the UI doesn't choose one, so the harness mints
  `agent-session-<uuid>`. When a create-session request has none, this fills in
  `<agent key>-<MMDD-HHMMSS>-<4 hex>` (e.g. `contract-review-1015-142615-a3f9`), so the Temporal UI
  says which demo each session is.

    uv run python scripts/harness_cli.py serve agents.toml ...
"""

import json
import os
import sys
import tomllib
import uuid
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from demo.quiet_streams import MODE, install  # noqa: E402

install()
print(f"stream mode: {MODE}", file=sys.stderr)


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


from temporal_agent_harness.web import serve as _serve  # noqa: E402

_create_app = _serve.create_app


def _create_app_with_readable_ids(*registry_paths):
    manager_id = os.environ.get("DEMO_SESSION_MANAGER_ID")
    if manager_id:
        from temporal_agent_harness.web.app import create_agent_harness_app
        from temporal_agent_harness.web.registry import load_agent_registries

        app = create_agent_harness_app(registry=load_agent_registries(registry_paths),
                                       manager_workflow_id=manager_id)
        print(f"session manager: {manager_id}", file=sys.stderr)
    else:
        app = _create_app(*registry_paths)
    app.add_middleware(ReadableSessionIds, keys=_agent_keys([str(p) for p in registry_paths]))
    return app


_serve.create_app = _create_app_with_readable_ids

from temporal_agent_harness.web.cli import main  # noqa: E402

main()
