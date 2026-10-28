"""The agent worker for one sandbox: ``DEMO_ROLE`` picks which agent it hosts.

    DEMO_ROLE=extractor python -m demo.worker

It polls that role's task queue, so the role's model calls, tools and ``/box`` file operations all
run here, next to this sandbox's Box Mount. Run two sandboxes with the same role and Temporal
spreads the work across them, and moves it when one dies.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from datetime import timedelta

from temporalio.client import Client
from temporalio.envconfig import ClientConfig
from temporalio.worker import Worker

from temporal_agent_harness.ai_sdks.openai_agents import ModelActivityParameters, OpenAIAgentsPlugin
from temporal_agent_harness.ai_sdks.openai_agents_harness import harness_observer_factory, stream_to_provider
from temporal_agent_harness.plugin import AgentHarnessPlugin

from .agents.coordinator import CoordinatorAgentWorkflow
from .agents.relay import BoxRelayAgentWorkflow
from .agents.specialists import (
    ComplianceAgentWorkflow,
    ContractReviewerAgentWorkflow,
    ExtractorAgentWorkflow,
)
from .boxfs import BoxMountFS
from .config import QUEUES
from .quiet_streams import MODE, MODEL_FLUSH, install as install_quiet_streams
from .quiet_tools import ACTIVITIES as TOOL_ACTIVITIES
from .handoff import await_change, await_handoff, fingerprint
from .sandbox import sandbox_name
from .tools import (
    BOOTH_TOOLS,
    COMPLIANCE_TOOLS,
    COORDINATOR_TOOLS,
    EXTRACTOR_TOOLS,
    REVIEWER_TOOLS,
    sandbox_info,
)

ALL_ROLE_TOOLS = [*EXTRACTOR_TOOLS, *COMPLIANCE_TOOLS, *REVIEWER_TOOLS, *COORDINATOR_TOOLS, *BOOTH_TOOLS]

ROLES = {
    "coordinator": (CoordinatorAgentWorkflow, COORDINATOR_TOOLS),
    "extractor": (ExtractorAgentWorkflow, EXTRACTOR_TOOLS),
    "compliance": (ComplianceAgentWorkflow, COMPLIANCE_TOOLS),
    "reviewer": (ContractReviewerAgentWorkflow, REVIEWER_TOOLS),
    # The booth demo's relay runs on the coordinator queue of its stack (booth-coordinator).
    "relay": (BoxRelayAgentWorkflow, []),
}


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s", force=True)
    role = os.environ.get("DEMO_ROLE", "")
    if role not in ROLES:
        sys.exit(f"error: DEMO_ROLE must be one of {sorted(ROLES)}, got {role!r}")
    if not os.environ.get("OPENAI_API_KEY"):
        sys.exit("error: OPENAI_API_KEY is not set (put it in .env.local)")
    workflow_cls, _ = ROLES[role]
    install_quiet_streams()

    openai_plugin = OpenAIAgentsPlugin(
        model_params=ModelActivityParameters(
            start_to_close_timeout=timedelta(minutes=2),
            # Short, so a crashed worker's in-flight model call is retried within seconds once the
            # sandbox restarts the worker (the harness heartbeats every timeout/2 in the background).
            heartbeat_timeout=timedelta(seconds=5),
            stream_to_provider=stream_to_provider,
            # Each flush is a Signal into the agent workflow (see quiet_streams).
            streaming_batch_interval=MODEL_FLUSH,
        ),
        observer_factory=harness_observer_factory,
    )
    harness_plugin = AgentHarnessPlugin(
        # Register every activity tool on every worker: a workflow may run a tool its model isn't
        # given (e.g. the reviewer reads the contract before its turn), and an unregistered
        # activity just retries forever.
        tools=[sandbox_info, fingerprint, await_handoff, await_change, *ALL_ROLE_TOOLS],
        filesystems=[BoxMountFS],
    )
    connect = ClientConfig.load_client_connect_config()
    client = await Client.connect(**connect, plugins=[openai_plugin, harness_plugin])  # harness last
    task_queue = QUEUES["coordinator" if role == "relay" else role]
    # Tool activities are plain Temporal activities (see quiet_tools), registered here.
    worker = Worker(client, task_queue=task_queue, workflows=[workflow_cls], activities=TOOL_ACTIVITIES)
    print(f"[{sandbox_name()}] {role} worker polling {task_queue} at {connect.get('target_host')}"
          f" (stream mode: {MODE})", flush=True)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
