"""Host functions for Code Mode scripts. The sandbox can import only asyncio and pathlib, so
JSON goes through these (each call shows in the timeline as a tool call).

No ``from __future__ import annotations`` (tool signatures are read concretely).
"""

import json
from typing import Any

from temporal_agent_kit import agent


@agent.tool_defn(inherently_safe=True)
async def parse_json(text: str) -> Any:
    """Parse JSON text (e.g. a file you read) into dicts and lists."""
    return json.loads(text)


@agent.tool_defn(inherently_safe=True)
async def to_json(value: Any) -> str:
    """Serialize dicts and lists to pretty-printed JSON text, ready to write to a file."""
    return json.dumps(value, indent=2)


SCRIPT_HELPERS = [parse_json, to_json]
