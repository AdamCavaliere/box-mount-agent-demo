"""Activity-backed agent tools whose start/end events come from the workflow, not the activity.

The harness's `agent.activity_tool_defn` publishes a tool's start and end from inside the activity,
and each flush reaches the workflow as a Signal (`__temporal_workflow_stream_publish`) plus a
workflow task: one or two per tool call, cluttering the agent's Temporal history. This builds the
same tool from public pieces instead: a plain Temporal activity (same name, same sandbox, same
timeouts and retries) wrapped in the harness's inline `agent.tool_defn`, which publishes the tool's
lifecycle from the workflow itself. The Harness UI shows the same tool cards and approvals apply as
before; the history just loses the signals.

Workers register the activities from `ACTIVITIES`.

No `from __future__ import annotations` (the harness reads tool annotations concretely).
"""

import inspect
from datetime import timedelta
from typing import Any, Callable, get_type_hints

from temporalio import activity, workflow

from temporal_agent_harness.harness import agent

ACTIVITIES: list[Callable[..., Any]] = []


def quiet_activity_tool(*, name: str, inherently_safe: bool = False, activity_config: Any = None):
    """Like `agent.activity_tool_defn(name=..., inherently_safe=..., activity_config=...)`, minus the signals."""
    config = dict(activity_config or {"start_to_close_timeout": timedelta(seconds=30)})

    def decorate(fn: Callable[..., Any]) -> Callable[..., Any]:
        ACTIVITIES.append(activity.defn(name=name)(fn))
        signature = inspect.signature(fn)
        result_type = get_type_hints(fn).get("return")

        async def tool(*args: Any, **kwargs: Any) -> Any:
            bound = signature.bind(*args, **kwargs)
            bound.apply_defaults()
            return await workflow.execute_activity(
                name, args=list(bound.arguments.values()), result_type=result_type, **config
            )

        tool.__name__ = tool.__qualname__ = name
        tool.__doc__ = fn.__doc__
        tool.__module__ = fn.__module__
        tool.__signature__ = signature  # type: ignore[attr-defined]
        tool.__annotations__ = dict(fn.__annotations__)
        return agent.tool_defn(inherently_safe=inherently_safe)(tool)

    return decorate
