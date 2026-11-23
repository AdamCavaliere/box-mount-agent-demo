"""The agent worker for one sandbox: ``DEMO_ROLE`` picks which agent it hosts.

    DEMO_ROLE=extractor python -m demo.worker

It polls that role's Task Queue, so the role's model calls, tools and ``/box`` file operations all
run here, next to this sandbox's Box Mount (the coordinator has none: it uses the Box API).
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

from temporal_agent_kit.ai_sdks.openai_agents import (
    ModelActivityParameters,
    OpenAIAgentsPlugin,
    kit_observer_factory,
)
from temporal_agent_kit.plugin import AgentKitPlugin

from .agents.coordinator_workflow import BoxRelayAgentWorkflow
from .agents.model_labels import LabelSummary, labelled_stream_to_provider
from .agents.specialist_workflows import ContractReviewerAgentWorkflow, ExtractorAgentWorkflow
from .boxfs import BoxMountFS
from .config import QUEUES
from .handoff_activities import wait_for_file_to_sync_from_box, record_file_version_for_handoff
from .sandbox import sandbox_name
from .custody_activities import CUSTODY_TOOLS
from .databricks_activities import DATABRICKS_TOOLS
from .activities import COORDINATOR_TOOLS, EXTRACTOR_TOOLS, REVIEWER_TOOLS, check_sandbox_and_box_mount

ALL_TOOLS = [check_sandbox_and_box_mount, record_file_version_for_handoff, wait_for_file_to_sync_from_box, *EXTRACTOR_TOOLS, *REVIEWER_TOOLS, *COORDINATOR_TOOLS,
             *CUSTODY_TOOLS, *DATABRICKS_TOOLS]

# Which workflow each sandbox's worker hosts. The coordinator is the outer harness (no model of its
# own); the Extractor and Reviewer are the inner harnesses it starts as subagents.
ROLES = {
    "coordinator": BoxRelayAgentWorkflow,
    "extractor": ExtractorAgentWorkflow,
    "reviewer": ContractReviewerAgentWorkflow,
}


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s", force=True)
    role = os.environ.get("DEMO_ROLE", "")
    if role not in ROLES:
        sys.exit(f"error: DEMO_ROLE must be one of {sorted(ROLES)}, got {role!r}")
    if not os.environ.get("OPENAI_API_KEY"):
        sys.exit("error: OPENAI_API_KEY is not set (put it in .env.local)")
    workflow_cls = ROLES[role]

    openai_plugin = OpenAIAgentsPlugin(
        model_params=ModelActivityParameters(
            start_to_close_timeout=timedelta(minutes=2),
            # Short, so a crashed worker's in-flight model call is retried within seconds once the
            # sandbox restarts the worker (the Agent Kit heartbeats every timeout/2 in the background).
            heartbeat_timeout=timedelta(seconds=5),
            # Each model call shows as what the agent is doing ("Contract reviewer · drafting redlines ..."),
            # not as the model id, in the Agent Kit UI and the Temporal UI.
            stream_to_provider=labelled_stream_to_provider,
            summary_override=LabelSummary(),
        ),
        observer_factory=kit_observer_factory,
    )
    kit_plugin = AgentKitPlugin(
        # Register every activity tool on every worker. The coordinator runs some tools on the
        # agents' Task Queues (e.g. read_supplier_from_extraction in the Extractor's sandbox), and an
        # activity no worker on its queue has registered just retries forever.
        tools=ALL_TOOLS,
        filesystems=[BoxMountFS],
    )
    connect = ClientConfig.load_client_connect_config()
    client = await Client.connect(**connect, plugins=[openai_plugin, kit_plugin])  # harness last
    task_queue = QUEUES[role]
    # The Agent Kit plugin registers each tool's activity (from tools=...) and the /box file activities.
    worker = Worker(client, task_queue=task_queue, workflows=[workflow_cls])
    print(f"[{sandbox_name()}] {role} worker polling {task_queue} at {connect.get('target_host')}", flush=True)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
