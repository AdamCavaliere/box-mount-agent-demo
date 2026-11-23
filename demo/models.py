"""The data that moves between the coordinator, the agents and their tools.

- Handoffs: ``BoxVersion`` (a file version as Box reports it), ``FileRef`` (that version as an
  agent sees it in its own mount) and ``HandoffReceipt`` (proof it synced in).
- Work: ``CaseAssignment`` and ``CaseTask`` go to an agent; ``CaseResult`` comes back.
- The coordinator's own checks: ``CounterpartyCheck`` (ERP and sanctions) and ``SpendContext``
  (Databricks).
- State the Agent Kit UI shows live: ``CaseBoard`` > ``Case`` > ``CaseStep`` and ``Custody``.

No ``from __future__ import annotations``: the Agent Kit and the pydantic data converter read these
annotations concretely.
"""

from typing import Literal

from pydantic import BaseModel, Field

from temporal_agent_kit.state import AgentState


class FileRef(BaseModel):
    """A specific version of a file, as the agent receiving it sees it: what one agent hands the next."""

    path: str = Field(description="Relative to the agent's own Box folder, e.g. 'Work/HALVORSEN-MSA/<run>/extraction.json'")
    sha1: str = Field(description="SHA-1 of the content, as Box reports it for a file version")
    size: int
    written_in: str = Field(description="The sandbox that wrote it")


class BoxVersion(BaseModel):
    """One version of a file as Box reports it (the coordinator's view, read with the Box API)."""

    path: str = Field(description="Relative to the demo's Box folder, e.g. 'Extraction/Incoming/contract.pdf'")
    file_id: str
    version_id: str
    sha1: str
    size: int
    modified_at: str = ""
    modified_by: str = ""

    def ref(self, mount: str, written_in: str) -> "FileRef":
        """This version as a handoff for the sandbox that mounts `mount` (e.g. 'Review/')."""
        return FileRef(path=self.path.removeprefix(mount), sha1=self.sha1, size=self.size, written_in=written_in)


class HandoffReceipt(BaseModel):
    """Proof a handed-off file reached this sandbox through Box Mount."""

    path: str
    sha1: str
    received_in: str
    written_in: str
    attempts: int = Field(description="Checks it took for Box Mount to sync the file here")


class CaseAssignment(BaseModel):
    """An agent's init data: the case it works on, and this run's folder for its outputs."""

    case_id: str
    work_dir: str = Field(description="e.g. 'Work/HALVORSEN-MSA/run-1120-143012'")
    output_suffix: str = Field(default="", description="Appended to output names, e.g. '_v2' for a re-review")


class CaseTask(BaseModel):
    """One unit of work handed to an agent."""

    instructions: str
    documents: list[str] = Field(default_factory=list, description="Source documents the agent always reads first")
    inputs: list[FileRef] = Field(default_factory=list, description="Exact file versions to wait for in its own mount")


class CaseResult(BaseModel):
    """What an agent hands back: its summary, the inputs it received, the files it wrote."""

    agent: str
    sandbox: str
    summary: str
    received: list[HandoffReceipt] = Field(default_factory=list)
    outputs: list[FileRef] = Field(default_factory=list)


class FileText(BaseModel):
    """A text file's contents, and the sandbox that read it."""

    path: str
    text: str
    read_in: str


class SandboxInfo(BaseModel):
    """Which sandbox an agent runs in, and the state of its Box Mount."""

    sandbox: str
    machine: str
    user: str
    box_mount: str
    box_mount_healthy: bool
    box_folder_entries: list[str]
    box_credentials_visible_to_agent: bool


# -- the coordinator's own checks -----------------------------------------------------------

class CounterpartyCheck(BaseModel):
    """The coordinator's fixed check of the contract's supplier: ERP vendor master and sanctions lists."""

    legal_name: str
    vendor_id: str = ""
    payment_terms_on_file: str = ""
    cleared: bool
    summary: str = Field(description="One line for the chat, the approval card and the Reviewer's task")


class SpendContext(BaseModel):
    """The coordinator's Databricks step: the supplier's spend, and what the contract's payment terms
    would mean against it."""

    available: bool
    summary: str = Field(description="The figures and what the payment terms mean against them, in one line")
    source: str = Field(default="", description="The Databricks table the figures came from")


# -- live state in the Agent Kit UI (the coordinator's AGENT STATE pane) -----------------------

class Custody(AgentState):
    """One file version the coordinator pinned, and what its checks found since."""

    file: str
    version: str
    sha1: str
    pinned_at: str
    status: str = "pinned"
    note: str = ""


StepStatus = Literal["waiting", "running", "done", "failed"]


class CaseStep(AgentState):
    """One agent's part in a case."""

    agent: str
    status: StepStatus = "waiting"
    sandbox: str = ""
    outputs: list[str] = []
    note: str = ""


class Case(AgentState):
    """One run of the review."""

    case_id: str
    run: str = ""
    documents: list[str] = []
    steps: list[CaseStep] = []
    custody: list[Custody] = []


class CaseBoard(AgentState):
    """Every run in this session."""

    cases: list[Case] = []
