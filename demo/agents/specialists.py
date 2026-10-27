"""The three specialist agents. Each is started by the coordinator as a subagent on its own task
queue, so it runs in its own sandbox and touches only that sandbox's Box Mount.

Each one, per case: waits for its inputs to sync in through Box Mount, does its work with
simulated enterprise tools, writes its deliverable into the shared Box folder (and may write
nothing else), and hands back a fingerprint of what it wrote.
"""

# No `from __future__ import annotations`: the harness reads tool and handler annotations
# concretely, and stringized ones fail to resolve.

from datetime import timedelta
from pathlib import PurePosixPath

from temporalio import workflow
from temporalio.contrib.workflow_streams import WorkflowStream

with workflow.unsafe.imports_passed_through():
    from temporal_agent_harness.harness import agent
    from temporal_agent_harness.harness.agent_protocol import AgentConfig, MidTurn, ToolApprovalPolicy
    from temporal_agent_harness.harness.agent_workflow import AgentWorkflowRunner

    from ..boxfs import BoxMountConfig, BoxMountFS
    from ..config import BOX_MOUNT_DESCRIPTION, WORKFLOW_TYPES
    from ..models import CaseAssignment, CaseResult, CaseTask
    from ..scripthelpers import SCRIPT_HELPERS
    from ..tools import COMPLIANCE_TOOLS, EXTRACTOR_TOOLS, REVIEWER_TOOLS, ocr_document, read_box_text, sandbox_info
    from .common import run_specialist

COMMON = """\
You work alone in your own Linux sandbox. The only thing you share with the other agents is a \
Box folder that Box Mount syncs into your sandbox at /box. Read inputs from it and write your \
deliverable into it with the `run_python_on_box_folder` tool (Python with open() and pathlib; there is no json \
module, so use the `parse_json` and `to_json` host functions). You may \
only write the file(s) you are told to write. Call `check_sandbox_and_box_mount` once at the start. Copy values \
from tool results; never invent them. When done, reply with a short plain-text summary (at most \
5 bullet points) of what you found and what you wrote."""

EXTRACTOR = COMMON + """

You are the Document Extraction agent for vendor packets (invoices and contracts). The next agent \
reviews the contract from your extraction.json alone, without the PDF, so be complete and exact.
1. `classify_document_type` each source document.
2. For an invoice, call `extract_invoice_fields_from_pdf`. The contract's full text is already in \
your task (from `read_pdf_text_from_box`).
3. Write extraction.json with keys: case_id; documents (list of {path, type}); invoice (the \
extracted fields, or null); contract: {path, title, parties, clauses: list of {heading, text} with \
each clause's text copied verbatim, key_terms: {payment_days, late_interest, initial_term_years, \
renewal_term_years, non_renewal_notice_days, price_increase_cap_pct, price_increase_notice_days, \
supplier_liability_cap, customer_liability_capped (true/false), indemnification (who indemnifies \
whom, or "mutual"), customer_termination_for_convenience (true/false), \
insurance_gl_per_occurrence_usd, governing_law}}; extracted_in (your sandbox). Use null for a term \
the contract doesn't state."""

COMPLIANCE = COMMON + """

You are the Vendor and Compliance agent in accounts payable.
1. Read the extraction.json you were handed and /box/Playbook/playbook.md (its invoice controls \
are the rules you apply; they may have changed since you last saw them).
2. Call `erp_lookup_vendor_record` with the invoice's vendor tax ID, `erp_match_invoice_to_purchase_order` with its \
PO number and total, and `screen_vendor_against_sanctions_lists` for the vendor. If a system call fails, it is retried \
for you; just wait for the result.
3. Write compliance.json with keys: case_id, vendor ({vendor_id, status, name}), bank_check \
({invoice_bank, invoice_account_last4, master_bank, master_account_last4, match}), po_match (the \
tool result), sanctions (the tool result), recommendation ('approve', 'hold' or 'reject'), \
reasons (list of strings citing the playbook rule), checked_in (your sandbox)."""

REVIEWER = COMMON + """

You are the Contract Review agent for Legal.
1. Work from the extraction.json the Extractor handed you: its contents (every clause verbatim, \
plus key_terms) are already in your task. Do not look for the contract PDF. Read \
/box/Playbook/playbook.md for the rules (people edit it in Box, so always read it fresh).
2. Compare every clause of the contract to every row of the playbook. For each deviation, call `get_approved_clause_language` for the \
approved fallback language. Then call `score_contract_risk` with every finding \
(clause, severity from the playbook's escalation column, issue).
3. Write review_memo.md: a title with the case id, the risk score and rating, a table of findings \
(clause, contract says, playbook rule, severity), proposed redlines with the approved language, \
and a one-line recommendation ('legal_review' or 'signature'). End with 'Reviewed in <sandbox>'."""


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
        self._box = box_decl.bind(BoxMountConfig(writable=self._outputs), timeout=timedelta(seconds=20))
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
            tools=[self._run_code, sandbox_info, *self.TOOLS],
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
        return [(ocr_document, {"path": d}) for d in task.documents if "invoice" not in d.lower()]

    @agent.init
    def __init__(self, config: AgentConfig, data: CaseAssignment) -> None:
        # WorkflowStream must be constructed in @agent.init itself, not in a helper.
        self._setup(config, data, self.box, WorkflowStream())

    @agent.accepts(mid_turn=MidTurn.ENQUEUE)
    async def process(self, task: CaseTask) -> CaseResult:
        """Extract structured data from a vendor packet into Work/<case>/extraction.json."""
        return await self._process(task)


@agent.defn(name=WORKFLOW_TYPES["compliance"])
class ComplianceAgentWorkflow(_Specialist):
    NAME, INSTRUCTIONS, TOOLS, OUTPUTS = "Compliance", COMPLIANCE, COMPLIANCE_TOOLS, ("compliance.json",)
    box = agent.vfs_mount("/box", BoxMountFS, description=BOX_MOUNT_DESCRIPTION)

    @agent.init
    def __init__(self, config: AgentConfig, data: CaseAssignment) -> None:
        # WorkflowStream must be constructed in @agent.init itself, not in a helper.
        self._setup(config, data, self.box, WorkflowStream())

    @agent.accepts(mid_turn=MidTurn.ENQUEUE)
    async def process(self, task: CaseTask) -> CaseResult:
        """Check vendor, bank details, PO match and sanctions into Work/<case>/compliance.json."""
        return await self._process(task)


@agent.defn(name=WORKFLOW_TYPES["reviewer"])
class ContractReviewerAgentWorkflow(_Specialist):
    NAME, INSTRUCTIONS, TOOLS, OUTPUTS = "Contract reviewer", REVIEWER, REVIEWER_TOOLS, ("review_memo.md",)
    box = agent.vfs_mount("/box", BoxMountFS, description=BOX_MOUNT_DESCRIPTION)

    def _prefetch(self, task: CaseTask) -> list:
        # The review depends on the Extractor's output: read the handed-off extraction.json up front
        # (a visible read_handed_off_file_from_box call), so the model can't skip it.
        return [(read_box_text, {"path": ref.path}) for ref in task.inputs if ref.path.endswith("extraction.json")]

    @agent.init
    def __init__(self, config: AgentConfig, data: CaseAssignment) -> None:
        # WorkflowStream must be constructed in @agent.init itself, not in a helper.
        self._setup(config, data, self.box, WorkflowStream())

    @agent.accepts(mid_turn=MidTurn.ENQUEUE)
    async def process(self, task: CaseTask) -> CaseResult:
        """Review the contract against the playbook into Work/<case>/review_memo.md."""
        return await self._process(task)
