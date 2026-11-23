"""Names every process agrees on. Read once at import (outside the workflow sandbox)."""

import os

MODEL = os.environ.get("DEMO_MODEL", "gpt-5.1")
# Reasoning effort for every model call ("low" keeps a run around a minute and a half).
REASONING_EFFORT = os.environ.get("DEMO_REASONING_EFFORT") or None
# Sessions close themselves after this long with no messages (0 = never). See agents/idle.py.
IDLE_CLOSE_MINUTES = float(os.environ.get("DEMO_IDLE_CLOSE_MINUTES", "10"))

# One Task Queue per sandbox. Each is polled only by that sandbox's worker, so an agent's model
# calls, tools and /box file operations run where its Box Mount is.
QUEUES = {role: f"box-{role}" for role in ("coordinator", "extractor", "reviewer")}

WORKFLOW_TYPES = {
    "coordinator": "BoxRelayAgent",
    "extractor": "ExtractorAgent",
    "reviewer": "ContractReviewerAgent",
}

BOX_MOUNT_DESCRIPTION = (
    "This agent's own Box folder, synced into this sandbox by Box Mount. Files reach the other "
    "agent only through Box. Paths are relative to /box: e.g. Incoming/ (the contract, for the "
    "Extractor), Playbook/playbook.md (Legal's rules, for the Reviewer; people edit it in Box), "
    "and Work/<case_id>/<run>/ for each agent's output."
)
