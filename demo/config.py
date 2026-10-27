"""Names every process agrees on. Read once at import (outside the workflow sandbox)."""

import os

MODEL = os.environ.get("DEMO_MODEL", "gpt-5.1")
# Optional reasoning effort for every model call (e.g. "low" for the booth, where speed matters).
REASONING_EFFORT = os.environ.get("DEMO_REASONING_EFFORT") or None
# Sessions close themselves after this long with no messages (0 = never). See agents/idle.py.
IDLE_CLOSE_MINUTES = float(os.environ.get("DEMO_IDLE_CLOSE_MINUTES", "30"))

# One task queue per agent role. Each is polled only by that role's sandbox(es), so an agent's
# tools and file operations run where its Box Mount is. The prefix keeps stacks apart: the full
# demo uses box-*, the booth demo (Box Relay, its own Box folder) uses booth-*.
QUEUE_PREFIX = os.environ.get("DEMO_QUEUE_PREFIX", "box")
QUEUES = {role: f"{QUEUE_PREFIX}-{role}" for role in ("coordinator", "extractor", "compliance", "reviewer")}

WORKFLOW_TYPES = {
    "coordinator": "CaseCoordinatorAgent",
    "extractor": "ExtractorAgent",
    "compliance": "ComplianceAgent",
    "reviewer": "ContractReviewerAgent",
    "relay": "BoxRelayAgent",
}

# The inline tools Code Mode dispatches for each file operation (gated unless allowed by name).
FS_TOOLS = ["fs_stat", "fs_read", "fs_list", "fs_write", "fs_mkdir", "fs_delete", "fs_rename", "fs_seed"]

BOX_MOUNT_DESCRIPTION = (
    "The shared Box folder for procurement cases, synced into this sandbox by Box Mount. It is "
    "the only way agents share files. Incoming/ holds vendor documents; Playbook/playbook.md "
    "holds Legal and AP's rules (edited by people in Box); Work/<case_id>/ holds each agent's "
    "output; Reviewed/<case_id>/ holds the final decision."
)
