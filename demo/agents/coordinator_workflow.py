"""The coordinator: the session you talk to in the Harness UI.

It never touches another agent's files. It starts each specialist as a subagent on that
specialist's task queue (so in that specialist's sandbox), and passes along only *fingerprints*
of the files they wrote. The files themselves travel through Box: the writer's Box Mount uploads
them, the reader's Box Mount downloads them, and the reader waits (durably) for that exact version.

Its own two actions with consequences, the AP decision and contract routing, wait for a human
to approve them in the UI. Everything else runs without a click.
"""

# No `from __future__ import annotations`: the harness reads tool and handler annotations
# concretely, and stringized ones fail to resolve.

import asyncio
from datetime import timedelta
from pathlib import PurePosixPath

from temporalio import workflow
from temporalio.contrib.workflow_streams import WorkflowStream

with workflow.unsafe.imports_passed_through():
    from agents import TResponseInputItem

    from temporal_agent_harness.harness import agent
    from temporal_agent_harness.harness.agent import Injected
    from temporal_agent_harness.harness.agent_protocol import (
        AgentConfig,
        MidTurn,
        TextMessage,
        TextReply,
        ToolApprovalPolicy,
    )
    from temporal_agent_harness.harness.agent_workflow import AgentWorkflowRunner
    from temporal_agent_harness.harness.code_mode.vfs import VFSMount
    from temporal_agent_harness.harness.state import StateRef

    from ..boxfs import BoxMountConfig, BoxMountFS
    from ..config import BOX_MOUNT_DESCRIPTION, FS_TOOLS, QUEUES, WORKFLOW_TYPES
    from ..models import Case, CaseAssignment, CaseBoard, CaseResult, CaseStep, CaseTask, FileRef
    from ..scripthelpers import SCRIPT_HELPERS
    from ..handoff_activities import fingerprint
    from ..activities import route_contract, sandbox_info, submit_ap_decision
    from .common import run_model_turn
    from .idle import close_when_idle

INSTRUCTIONS = """\
You coordinate intake of vendor packets (an invoice plus a supplier contract) for Pinecrest \
Manufacturing. You run in your own sandbox; a shared Box folder is synced into it by Box Mount at \
/box. You do the coordination; three specialist agents, each in its own sandbox, do the work:

- `ask_extractor_agent(case_id, documents)`: starts a new run of the case; the Extractor reads the \
packet and writes extraction.json into the run's folder, Work/<case_id>/<run>/.
- `ask_compliance_agent(case_id)`: the Compliance agent checks vendor, bank details, PO match and \
sanctions, and writes compliance.json into the run's folder. Needs extraction first.
- `ask_contract_reviewer_agent(case_id)`: the Contract reviewer checks the contract against the playbook \
and writes review_memo.md into the run's folder. Needs extraction first.
Each tool result lists the exact paths it wrote (`outputs`); use those.

To process a packet:
1. Call `check_sandbox_and_box_mount`, then use `run_python_on_box_folder` to list /box/Incoming. The case id is the invoice \
number in the invoice's file name (e.g. INV-20417).
2. Call `ask_extractor_agent` with every document in the packet (paths like 'Incoming/<file>').
3. Call `ask_compliance_agent` and `ask_contract_reviewer_agent` together in the same response, so they run \
in parallel in their own sandboxes.
4. Use `run_python_on_box_folder` to print compliance.json and review_memo.md (under /box/, at the paths the tools \
returned). Then call `write_decision_to_box` with the actual values you read: amounts, variance, bank \
accounts, each redline. It writes /box/Reviewed/<case_id>/<run>/decision.md.
5. Only after `write_decision_to_box` succeeds, call `record_accounts_payable_decision` and `route_contract_to_legal_or_signature`. A person \
approves each one in the console; if they reject, say so and stop.
6. Reply with a brief summary.

If asked to re-review (for example after someone edits the playbook in Box), call \
`ask_contract_reviewer_agent` again: it reads the latest playbook and writes a new version of the memo. Keep replies short and concrete."""


# Injected parameters come last and keyword-only: the OpenAI adapter passes the model's arguments
# positionally, so anything before them would collide.


def _case(board: CaseBoard, case_id: str) -> Case:
    for case in board.cases:
        if case.case_id == case_id:
            return case
    board.cases.append(Case(case_id=case_id))
    return board.cases[-1]


def _set_step(board: StateRef[CaseBoard], case_id: str, step: CaseStep) -> None:
    with board.mutate() as draft:
        # In place: a draft node can't be moved into a new list, only replaced or appended.
        case = _case(draft, case_id)
        for i, existing in enumerate(case.steps):
            if existing.agent == step.agent:
                case.steps[i] = step
                return
        case.steps.append(step)


async def _delegate(
    runner: AgentWorkflowRunner,
    board: StateRef[CaseBoard],
    handoffs: dict[str, FileRef],
    role: str,
    case_id: str,
    task: CaseTask,
) -> CaseResult:
    """Run one specialist as a subagent in its own sandbox and record what it handed back."""
    _set_step(board, case_id, CaseStep(agent=role, status="running", note=f"queue {QUEUES[role]}"))
    workflow.set_current_details(f"**Vendor Packet Intake** · {case_id} · {role} working in its own sandbox")
    run = next(c.run for c in board.current.cases if c.case_id == case_id)
    handle = await runner.start_subagent(
        role, WORKFLOW_TYPES[role], QUEUES[role],
        data=CaseAssignment(case_id=case_id, work_dir=f"Work/{case_id}/{run}"),
    )
    try:
        reply = await runner.run_subagent_turn(handle, "process", task.model_dump(mode="json"))
    except Exception as err:
        _set_step(board, case_id, CaseStep(agent=role, status="failed", note=str(err)[:200]))
        raise
    finally:
        await runner.stop_subagent(handle)
    result = CaseResult.model_validate(reply)
    for ref in result.outputs:
        handoffs[ref.path] = ref
    _set_step(board, case_id, CaseStep(
        agent=role, status="done", sandbox=result.sandbox,
        outputs=[f"{r.path} @ {r.sha256[:8]}" for r in result.outputs],
    ))
    return result


PLAYBOOK = "Playbook/playbook.md"


async def _snapshot(runner: AgentWorkflowRunner, paths: list[str]) -> list[FileRef]:
    """Fingerprint files as this (the coordinator's) sandbox sees them right now. Source documents
    and the playbook reach each specialist's sandbox through Box Mount too, so the specialist
    waits for these exact versions before it starts: the same version the coordinator saw."""
    return [await runner.run_tool(str(workflow.uuid4()), fingerprint, path=p) for p in paths]


def _extraction(handoffs: dict[str, FileRef], board: StateRef[CaseBoard], case_id: str) -> FileRef:
    run = next((c.run for c in board.current.cases if c.case_id == case_id), "")
    ref = handoffs.get(f"Work/{case_id}/{run}/extraction.json")
    if ref is None:
        raise ValueError(f"No extraction for {case_id} yet: call ask_extractor_agent first.")
    return ref


@agent.tool_defn(inherently_safe=True)
async def ask_extractor_agent(
    case_id: str,
    documents: list[str],
    *,
    runner: Injected[AgentWorkflowRunner],
    board: Injected[StateRef[CaseBoard]],
    handoffs: Injected[dict],
) -> CaseResult:
    """Have the Extractor agent (in its own sandbox) extract the packet into Work/<case_id>/extraction.json."""
    # Every extraction starts a new run with its own output folder. Box Mount must never see a
    # path deleted and then reused: a write into a folder whose Box copy was trashed can get stuck.
    run = f"run-{workflow.now():%m%d-%H%M%S}"
    with board.mutate() as draft:
        case = _case(draft, case_id)
        case.documents = documents
        case.run = run
    task = CaseTask(instructions=f"Case {case_id}: extract this vendor packet.", documents=documents,
                    inputs=await _snapshot(runner, documents))
    return await _delegate(runner, board, handoffs, "extractor", case_id, task)


@agent.tool_defn(inherently_safe=True)
async def ask_compliance_agent(
    case_id: str,
    *,
    runner: Injected[AgentWorkflowRunner],
    board: Injected[StateRef[CaseBoard]],
    handoffs: Injected[dict],
) -> CaseResult:
    """Have the Compliance agent (in its own sandbox) check vendor, bank, PO and sanctions."""
    extraction = _extraction(handoffs, board, case_id)
    task = CaseTask(instructions=f"Case {case_id}: run vendor and invoice compliance checks.",
                    inputs=[extraction, *await _snapshot(runner, [PLAYBOOK])])
    return await _delegate(runner, board, handoffs, "compliance", case_id, task)


@agent.tool_defn(inherently_safe=True)
async def ask_contract_reviewer_agent(
    case_id: str,
    *,
    runner: Injected[AgentWorkflowRunner],
    board: Injected[StateRef[CaseBoard]],
    handoffs: Injected[dict],
) -> CaseResult:
    """Have the Contract reviewer agent (in its own sandbox) review the contract against the playbook."""
    extraction = _extraction(handoffs, board, case_id)
    # No PDF: the review works from the Extractor's extraction.json (and the playbook).
    task = CaseTask(instructions=f"Case {case_id}: review the supplier contract.",
                    inputs=[extraction, *await _snapshot(runner, [PLAYBOOK])])
    return await _delegate(runner, board, handoffs, "reviewer", case_id, task)


@agent.tool_defn(inherently_safe=True)
async def write_decision_to_box(
    case_id: str,
    ap_decision: str,
    ap_reasons: list[str],
    contract_route: str,
    redlines: list[str],
    *,
    board: Injected[StateRef[CaseBoard]],
    box: Injected[VFSMount],
) -> str:
    """Write the case decision to Reviewed/<case_id>/<run>/decision.md in Box. `ap_decision` is
    'approve', 'hold' or 'reject'; `contract_route` is 'legal_review' or 'signature'; each reason
    and redline is one concrete sentence with the real values."""
    case = next((c for c in board.current.cases if c.case_id == case_id), None)
    if case is None or not case.run:
        raise ValueError(f"No run for {case_id} yet: call ask_extractor_agent first.")
    lines = [f"# Decision: {case_id} ({case.run})", "",
             f"## Accounts payable: {ap_decision.upper()}", "", *[f"- {r}" for r in ap_reasons], "",
             f"## Contract: route to {contract_route}", "", *[f"- {r}" for r in redlines], "",
             "## Who did what", ""]
    lines += [f"- {s.agent}: {s.sandbox or '?'} ({s.status}); wrote {', '.join(s.outputs) or 'nothing'}" for s in case.steps]
    lines += ["- coordinator: sandbox-coordinator; wrote this decision", ""]
    path = f"Reviewed/{case_id}/{case.run}/decision.md"
    await box.backend.write(PurePosixPath(path), "\n".join(lines).encode())
    with board.mutate() as draft:
        _case(draft, case_id).decision = f"AP {ap_decision}; contract {contract_route}"
    return f"Wrote /box/{path}"


@agent.defn(name=WORKFLOW_TYPES["coordinator"])
class CoordinatorAgentWorkflow:
    """Vendor packet intake: coordinates three specialist agents through a shared Box folder."""

    box = agent.vfs_mount("/box", BoxMountFS, description=BOX_MOUNT_DESCRIPTION)
    case_board = agent.state(CaseBoard)

    @agent.init
    def __init__(self, config: AgentConfig) -> None:
        self._runner = AgentWorkflowRunner(
            config,
            stream=WorkflowStream(),
            # File operations, delegation and lookups run freely; submit_ap_decision and
            # route_contract (not inherently safe) wait for a person.
            approval_policy_default=ToolApprovalPolicy.allow_tools(
                [*FS_TOOLS, "run_python_on_box_folder"], also_inherently_safe=True
            ),
        )
        self._box = self.box.bind(BoxMountConfig(writable=["Reviewed/*/*/decision.md"]),
                                  timeout=timedelta(seconds=20))
        self._run_code = agent.code_mode_tool(SCRIPT_HELPERS, name="run_python_on_box_folder", mounts=[self._box])
        self._handoffs: dict[str, FileRef] = {}
        self._conversation: list[TResponseInputItem] = []
        self._turns = 0
        self._busy = False

    @agent.setup
    async def _watch_idle(self) -> None:
        self._idle = asyncio.create_task(
            close_when_idle("Vendor Packet Intake", lambda: self._turns, lambda: self._busy,
                            # the harness's own close-signal handler (private; see idle.py)
                            self._runner._handle_close)
        )

    @agent.accepts(mid_turn=MidTurn.ENQUEUE)
    async def ask(self, message: TextMessage) -> TextReply:
        """Ask the coordinator to process the vendor packet in Box (e.g. "Process the new packet
        in Incoming"), to re-review after a playbook change, or about a case."""
        self._turns += 1
        self._busy = True
        try:
            return await self._ask(message)
        finally:
            self._busy = False

    async def _ask(self, message: TextMessage) -> TextReply:
        injections = {"runner": self._runner, "board": self.case_board, "handoffs": self._handoffs}
        tools = [
            sandbox_info,
            self._run_code,
            (ask_extractor_agent, injections),
            (ask_compliance_agent, injections),
            (ask_contract_reviewer_agent, injections),
            (write_decision_to_box, {"board": self.case_board, "box": self._box}),
            submit_ap_decision,
            route_contract,
        ]
        self._conversation.append({"role": "user", "content": message.text})
        reply, self._conversation = await run_model_turn(
            self._runner, name="Coordinator", instructions=INSTRUCTIONS, tools=tools,
            conversation=self._conversation, max_turns=40,
        )
        return TextReply(text=reply)
