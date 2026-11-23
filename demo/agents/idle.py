"""Close a session by itself after a stretch with no messages.

Sessions are long-lived on purpose: Box Relay's `re-review` needs the run it already did.
But one left open forever clutters the Temporal UI, so each session watches for inactivity and,
after DEMO_IDLE_CLOSE_MINUTES with no message, closes itself through the Agent Kit's close handler
(what the UI's close button triggers via the `close` signal; a workflow can't signal itself,
Temporal answers EXTERNAL_WORKFLOW_EXECUTION_NOT_FOUND). The workflow then completes normally. A turn that is
running, including one waiting on a human approval, never counts as idle.
"""

import asyncio
from datetime import timedelta
from typing import Callable

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from ..config import IDLE_CLOSE_MINUTES


async def close_when_idle(
    label: str, turns: Callable[[], int], busy: Callable[[], bool], close: Callable[[], None]
) -> None:
    if IDLE_CLOSE_MINUTES <= 0:
        return
    idle = timedelta(minutes=IDLE_CLOSE_MINUTES)
    while True:
        seen = turns()
        try:
            await workflow.wait_condition(lambda: turns() != seen or busy(), timeout=idle)
        except asyncio.TimeoutError:
            break
        await workflow.wait_condition(lambda: not busy())  # a turn ran; restart the clock after it
    workflow.set_current_details(f"**{label}** · closed after {IDLE_CLOSE_MINUTES:g} min with no messages")
    close()
