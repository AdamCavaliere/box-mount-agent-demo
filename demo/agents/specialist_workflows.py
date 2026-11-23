"""The two inner harnesses: the Extractor and the Contract reviewer. The coordinator starts each
as a subagent on its own Task Queue, so it runs in its own sandbox and touches only that sandbox's
Box Mount, which syncs only that agent's own Box folder.

Each one, per run: waits for the exact versions of its inputs to sync in through Box Mount, does
its work, writes its deliverable into its folder (and may write nothing else), and hands back a
fingerprint of what it wrote.
"""

# No `from __future__ import annotations`: the Agent Kit reads tool and handler annotations
# concretely, and stringized ones fail to resolve.

from datetime import timedelta
from pathlib import PurePosixPath

from temporalio import workflow
from temporalio.contrib.workflow_streams import WorkflowStream

with workflow.unsafe.imports_passed_through():
    from temporal_agent_kit import agent
    from temporal_agent_kit.agent_protocol import AgentConfig, MidTurn, ToolApprovalPolicy
    from temporal_agent_kit.agent_workflow import AgentWorkflowRunner

    from ..boxfs import BoxMountConfig, BoxMountFS
    from ..config import BOX_MOUNT_DESCRIPTION, WORKFLOW_TYPES
    from ..models import CaseAssignment, CaseResult, CaseTask
    from ..scripthelpers import SCRIPT_HELPERS
    from ..activities import EXTRACTOR_TOOLS, REVIEWER_TOOLS, read_pdf_text_from_box, read_handed_off_file_from_box, check_sandbox_and_box_mount
    from .common import run_specialist

COMMON = """\
You work alone in your own Linux sandbox. Your own Box folder is synced into it at /box by Box \
Mount; files reach the other agent only through Box. Read inputs from it and write your \
deliverable into it with the `run_python_on_box_folder` tool (Python with open() and pathlib; there is no json \
module, so use the `parse_json` and `to_json` host functions). You may \
only write the file(s) you are told to write. Call `check_sandbox_and_box_mount` once at the start. Copy values \
from tool results; never invent them. When done, reply with a short plain-text summary (at most \
5 bullet points) of what you found and what you wrote."""

EXTRACTOR = COMMON + """

You are the Document Extraction agent for supplier contracts. The next agent reviews the \
contract from your extraction.json alone, without the PDF, so be complete and exact. The \
contract's full text is already in your task (from `read_pdf_text_from_box`).
Write extraction.json with keys: case_id; contract: {path, title, parties, supplier: {legal_name, country} (the \
supplying party exactly as the contract names it; country only if the contract states it, else \
null), clauses: list of {heading, text} with \
each clause's text copied verbatim, key_terms: {payment_days, late_interest, initial_term_years, \
renewal_term_years, non_renewal_notice_days, price_increase_cap_pct, price_increase_notice_days, \
supplier_liability_cap, customer_liability_capped (true/false), indemnification (who indemnifies \
whom, or "mutual"), customer_termination_for_convenience (true/false), \
insurance_gl_per_occurrence_usd, governing_law}}; extracted_in (your sandbox). Use null for a term \
the contract doesn't state."""

REVIEWER = COMMON + """

You are the Contract Review agent for Legal.
1. Work from the extraction.json the Extractor handed you: its contents (every clause verbatim, \
plus key_terms) are already in your task. Do not look for the contract PDF. Read \
/box/Playbook/playbook.md for the rules (people edit it in Box, so always read it fresh).
2. Compare every clause of the contract to every row of the playbook. For each deviation, call `get_approved_clause_language` for the \
approved fallback language. Then call `score_contract_risk` with every finding \
(clause, severity from the playbook's escalation column, issue).
3. Write review_memo.md: a title with the case id, the risk score and rating, a table of findings \
(clause, contract says, playbook rule, severity), a "Business context" section, proposed redlines \
with the approved language, and a one-line recommendation ('legal_review' or 'signature'). End \
with 'Reviewed in <sandbox>'. In "Business context", restate the "Facts from the coordinator" in \
your task (the playbook's standard, the ERP's terms on file, spend from Databricks) with their \
figures and sources, and say which finding each one bears on (e.g. what the contract's payment \
terms cost against the spend). The company standard is always the playbook's; the ERP's terms are \
only what this supplier is paid on today, so never call them a standard or policy. If your task \
has no such facts, write "None provided."
4. Write review_findings.json, the same findings as data (it is loaded into a database once a \
person approves the memo): {"case_id", "risk_score" (integer), "rating", "recommendation", \
"findings": [{"clause", "contract_says", "playbook_rule", "severity", "issue"}]}, one entry per \
row of the memo's findings table, all values strings except risk_score."""


class _Specialist:
    """Shared shape: init binds /box (writable only at this agent's outputs) and Code Mode."""

    NAME: str
    INSTRUCTIONS: str
    TOOLS: list
    OUTPUTS: tuple[str, ...]

    def _setup(self, config: AgentConfig, data: CaseAssignment, box_decl, stream: WorkflowStream) -> None:
        self._runner = AgentWorkflowRunner(
            config,
            stream=stream,
            # Every file read is a tool call; a human click per read would stall the pipeline.
            approval_policy_default=ToolApprovalPolicy.dangerously_skip_all(),
        )
        self._outputs = [
            f"{data.work_dir}/{PurePosixPath(name).stem}{data.output_suffix}{PurePosixPath(name).suffix}"
            for name in self.OUTPUTS
        ]
        self._box = box_decl.bind(BoxMountConfig(writable=self._outputs), timeout=timedelta(seconds=60))
        self._run_code = agent.code_mode_tool(SCRIPT_HELPERS, name="run_python_on_box_folder", mounts=[self._box])

    async def _exists(self, path: str) -> bool:
        return await self._box.backend.stat(PurePosixPath(path)) is not None

    def _prefetch(self, task: CaseTask) -> list:
        """Tool calls this agent's job always needs, run before its model turn."""
        return []

    async def _process(self, task: CaseTask) -> CaseResult:
        return await run_specialist(
            self._runner,
            agent_name=self.NAME,
            instructions=self.INSTRUCTIONS,
            tools=[self._run_code, check_sandbox_and_box_mount, *self.TOOLS],
            task=task,
            outputs=self._outputs,
            file_exists=self._exists,
            prefetch=self._prefetch(task),
        )


@agent.defn(name=WORKFLOW_TYPES["extractor"])
class ExtractorAgentWorkflow(_Specialist):
    NAME, INSTRUCTIONS, TOOLS, OUTPUTS = "Extractor", EXTRACTOR, EXTRACTOR_TOOLS, ("extraction.json",)
    box = agent.vfs_mount("/box", BoxMountFS, description=BOX_MOUNT_DESCRIPTION)

    def _prefetch(self, task: CaseTask) -> list:
        # Always read the contract first (a visible read_pdf_text_from_box call), so the clause text
        # the Reviewer will depend on comes from the document, not the model's memory.
        return [(read_pdf_text_from_box, {"path": d}) for d in task.documents]

    @agent.init
    def __init__(self, config: AgentConfig, data: CaseAssignment) -> None:
        # WorkflowStream must be constructed in @agent.init itself, not in a helper.
        self._setup(config, data, self.box, WorkflowStream())

    @agent.accepts(mid_turn=MidTurn.ENQUEUE)
    async def process(self, task: CaseTask) -> CaseResult:
        """Extract the supplier contract into Work/<case>/<run>/extraction.json."""
        return await self._process(task)


@agent.defn(name=WORKFLOW_TYPES["reviewer"])
class ContractReviewerAgentWorkflow(_Specialist):
    NAME, INSTRUCTIONS, TOOLS, OUTPUTS = "Contract reviewer", REVIEWER, REVIEWER_TOOLS, ("review_memo.md", "review_findings.json")
    box = agent.vfs_mount("/box", BoxMountFS, description=BOX_MOUNT_DESCRIPTION)

    def _prefetch(self, task: CaseTask) -> list:
        # The review depends on the Extractor's output: read the handed-off extraction.json up front
        # (a visible read_handed_off_file_from_box call), so the model can't skip it.
        return [(read_handed_off_file_from_box, {"path": ref.path}) for ref in task.inputs if ref.path.endswith("extraction.json")]

    @agent.init
    def __init__(self, config: AgentConfig, data: CaseAssignment) -> None:
        # WorkflowStream must be constructed in @agent.init itself, not in a helper.
        self._setup(config, data, self.box, WorkflowStream())

    @agent.accepts(mid_turn=MidTurn.ENQUEUE)
    async def process(self, task: CaseTask) -> CaseResult:
        """Review the contract against the playbook into Work/<case>/<run>/review_memo.md (and the same
        findings as data in review_findings.json)."""
        return await self._process(task)
