"""Data shared across agents, tools and the coordinator's case board.

No ``from __future__ import annotations``: the harness and the pydantic data converter read these
annotations concretely.
"""

from typing import Literal

from pydantic import BaseModel, Field

from temporal_agent_harness.harness.state import HarnessState


class FileRef(BaseModel):
    """A specific version of a file in the Box folder: what one agent hands the next."""

    path: str = Field(description="Relative to the Box folder, e.g. 'Work/INV-20417/extraction.json'")
    sha256: str
    size: int
    written_in: str = Field(description="The sandbox that wrote it")


class HandoffReceipt(BaseModel):
    """Proof a handed-off file reached this sandbox through Box Mount."""

    path: str
    sha256: str
    received_in: str
    written_in: str
    attempts: int = Field(description="Checks it took for Box Mount to sync the file here")


class CaseAssignment(BaseModel):
    """A specialist agent's init data: the case it works on, and this run's folder for outputs."""

    case_id: str
    work_dir: str = Field(description="e.g. 'Work/INV-20417/run-1014-090512'")
    output_suffix: str = Field(default="", description="Appended to output names, e.g. '_v2' for a re-review")


class CaseTask(BaseModel):
    """One unit of work handed to a specialist."""

    instructions: str
    documents: list[str] = Field(default_factory=list, description="Source documents in the Box folder")
    inputs: list[FileRef] = Field(default_factory=list, description="Other agents' outputs to wait for")


class CaseResult(BaseModel):
    """What a specialist hands back: its summary, the inputs it received, the files it wrote."""

    agent: str
    sandbox: str
    summary: str
    received: list[HandoffReceipt] = Field(default_factory=list)
    outputs: list[FileRef] = Field(default_factory=list)


class SandboxInfo(BaseModel):
    """Which sandbox an agent runs in, and the state of its Box Mount."""

    sandbox: str
    machine: str
    user: str
    box_mount: str
    box_mount_healthy: bool
    box_folder_entries: list[str]
    box_credentials_visible_to_agent: bool


# -- the coordinator's case board (AGENT STATE pane) ---------------------------------------

StepStatus = Literal["waiting", "running", "done", "failed"]


class CaseStep(HarnessState):
    """One agent's part in a case."""

    agent: str
    status: StepStatus = "waiting"
    sandbox: str = ""
    outputs: list[str] = []
    note: str = ""


class Case(HarnessState):
    """A vendor packet moving through the pipeline."""

    case_id: str
    run: str = ""
    documents: list[str] = []
    steps: list[CaseStep] = []
    decision: str = ""


class CaseBoard(HarnessState):
    """Every case this coordinator has handled."""

    cases: list[Case] = []
