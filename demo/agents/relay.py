"""Box Relay: the short booth demo.

Two agents, each in its own sandbox, hand a contract review to each other through Box, and a
person decides whether the result is published:

    contract + playbook in Box ──► Extractor (sandbox-booth-extractor) writes extraction.json
        ──► Box ──► Reviewer (sandbox-booth-reviewer) writes review_memo.md ──► Box
        ──► approval card with a link to the memo in Box ──► approved: copied to Reviewed/ in Box

The relay itself is a plain workflow, not a model: it runs the two agents in order and passes
along only fingerprints of the files, so each agent waits until that exact version has synced
into its own sandbox. Type "go" to run it. Afterwards, edit Playbook/playbook.md in Box and type
"re-review": the relay waits for the edit to sync, and the Reviewer writes review_memo_v2.md.
"""

# No `from __future__ import annotations`: the harness reads tool and handler annotations
# concretely, and stringized ones fail to resolve.

import asyncio

from temporalio import workflow
from temporalio.exceptions import ActivityError
from temporalio.contrib.workflow_streams import WorkflowStream

with workflow.unsafe.imports_passed_through():
    from temporal_agent_harness.harness import agent
    from temporal_agent_harness.harness.agent_protocol import (
        AgentConfig,
        MidTurn,
        TextMessage,
        TextReply,
        ToolApprovalPolicy,
    )
    from temporal_agent_harness.harness.agent_protocol.events import ReplyDelta
    from temporal_agent_harness.harness.agent_workflow import AgentWorkflowRunner, ToolApprovalDenied

    from ..config import QUEUES, WORKFLOW_TYPES
    from ..handoff import await_change, fingerprint
    from ..tools import box_link, publish_memo
    from .idle import close_when_idle
    from ..models import Case, CaseAssignment, CaseBoard, CaseResult, CaseStep, CaseTask, FileRef

CASE_ID = "HALVORSEN-MSA"
CONTRACT = "Incoming/Halvorsen_Master_Supply_Agreement.pdf"
PLAYBOOK = "Playbook/playbook.md"
FOCUS = ("Booth review: cover only payment terms, renewal, limitation of liability, "
         "indemnification and termination, and keep the memo short. If a clause complies with the "
         "playbook as it stands now, mark it OK with no redline rather than listing it as a finding.")


def _call_id() -> str:
    return str(workflow.uuid4())


def _status(text: str) -> None:
    """The workflow's 'current details' line in the Temporal UI."""
    workflow.set_current_details(f"**Box Relay** · {text}")


@agent.defn(name=WORKFLOW_TYPES["relay"])
class BoxRelayAgentWorkflow:
    """Two agents in separate sandboxes relay a contract review through Box."""

    relay_board = agent.state(CaseBoard)

    @agent.init
    def __init__(self, config: AgentConfig) -> None:
        self._runner = AgentWorkflowRunner(
            config,
            stream=WorkflowStream(),
            # Lookups run freely; publish_memo (not inherently safe) waits for a person.
            approval_policy_default=ToolApprovalPolicy.allow_inherently_safe(),
        )
        self._work_dir = ""
        self._contract: FileRef | None = None
        self._playbook: FileRef | None = None
        self._extraction: FileRef | None = None
        self._reviews = 0
        self._turns = 0
        self._busy = False

    @agent.setup
    async def _watch_idle(self) -> None:
        self._idle = asyncio.create_task(
            close_when_idle("Box Relay", lambda: self._turns, lambda: self._busy,
                            # the harness's own close-signal handler (private; see idle.py)
                            self._runner._handle_close)
        )

    @agent.accepts(mid_turn=MidTurn.REJECT)
    async def ask(self, message: TextMessage) -> TextReply:
        """Type "go" to run the relay (Extractor, then Reviewer, handing files through Box). After a
        run, edit Playbook/playbook.md in Box and type "re-review"."""
        self._turns += 1
        self._busy = True
        try:
            text = message.text.strip().lower()
            if "review" in text and self._extraction is not None:
                return TextReply(text=await self._re_review())
            return TextReply(text=await self._relay())
        finally:
            self._busy = False

    # -- the relay -------------------------------------------------------------------------

    async def _relay(self) -> str:
        run = f"run-{workflow.now():%m%d-%H%M%S}"
        self._work_dir = f"Work/{CASE_ID}/{run}"
        self._reviews = 0
        with self.relay_board.mutate() as draft:
            draft.cases.append(Case(case_id=CASE_ID, run=run, documents=[CONTRACT, PLAYBOOK]))
        _status(f"{CASE_ID} {run} · fingerprinting the contract and playbook in Box")

        # The source files as this sandbox sees them; the agents wait for these exact versions.
        self._contract, self._playbook = [
            await self._runner.run_tool(_call_id(), fingerprint, path=p) for p in (CONTRACT, PLAYBOOK)
        ]
        extracted = await self._delegate(
            "extractor",
            CaseTask(instructions=f"Case {CASE_ID}: extract this supplier contract.",
                     documents=[CONTRACT], inputs=[self._contract]),
        )
        self._extraction = extracted.outputs[0]
        reviewed = await self._review("")
        return _report("Relay complete.", [extracted, reviewed]) + await self._publish(reviewed)

    async def _re_review(self) -> str:
        # Waits (durably, retrying) until a person's edit to the playbook has synced into this sandbox.
        _status(f"{CASE_ID} · waiting for someone to edit Playbook/playbook.md in Box")
        self._playbook = await self._runner.run_tool(
            _call_id(), await_change, path=PLAYBOOK, previous_sha256=self._playbook.sha256
        )
        self._reviews += 1
        reviewed = await self._review(f"_v{self._reviews + 1}")
        return _report(f"Re-review complete with the playbook edited in Box ({self._playbook.sha256[:8]}).",
                       [reviewed]) + await self._publish(reviewed)

    async def _publish(self, reviewed: CaseResult) -> str:
        """The human-in-the-loop step: link the memo in Box, and publish it only if a person approves."""
        memo = reviewed.outputs[0]
        link = await self._runner.run_tool(_call_id(), box_link, path=memo.path)
        summary = reviewed.summary.strip().splitlines()[0][:300] if reviewed.summary.strip() else ""
        # Shown in the chat now, while the approval waits, so the approver can open the memo first.
        self._runner.publish(ReplyDelta(text=(
            f"**Review ready for approval.** [Open {memo.path.rsplit('/', 1)[-1]} in Box]({link.url}), "
            f"then approve or reject publishing it to Reviewed/.\n\n"
        )))
        _status(f"{CASE_ID} · waiting for a person to approve publishing {memo.path} ({link.url})")
        try:
            published = await self._runner.run_tool(
                _call_id(), publish_memo,
                case_id=CASE_ID, memo_path=memo.path, memo_link=link.url, summary=summary,
            )
        except ActivityError as err:
            reason = getattr(err.cause, "message", None) or str(err.cause or err)
            _status(f"{CASE_ID} · could not publish: {reason[:120]}")
            return f"\n\n**Couldn't publish:** {reason}. Type `go` for a new run."
        except ToolApprovalDenied:
            _status(f"{CASE_ID} · publishing rejected; memo stays in Work/")
            return f"\n\n**Not published.** [Open the memo in Box]({link.url}); it stays in Work/ for revision."
        published_link = await self._runner.run_tool(_call_id(), box_link, path=published.published_path)
        _status(f"{CASE_ID} · published to {published.published_path} · type go for a new run")
        return (f"\n\n**Published to Legal.** [Open the published memo in Box]({published_link.url})"
                f" ({published.published_path}, copied in {published.sandbox}).")

    async def _review(self, suffix: str) -> CaseResult:
        return await self._delegate(
            "reviewer",
            # No PDF: the review works from the Extractor's extraction.json (and the playbook).
            CaseTask(instructions=f"Case {CASE_ID}: review the supplier contract against the playbook. {FOCUS}",
                     inputs=[self._extraction, self._playbook]),
            suffix=suffix,
        )

    async def _delegate(self, role: str, task: CaseTask, suffix: str = "") -> CaseResult:
        label = role if not suffix else f"{role}{suffix}"
        _status(f"{CASE_ID} · {label} working in its own sandbox (queue {QUEUES[role]})")
        self._set_step(CaseStep(agent=label, status="running", note=f"queue {QUEUES[role]}"))
        handle = await self._runner.start_subagent(
            role, WORKFLOW_TYPES[role], QUEUES[role],
            data=CaseAssignment(case_id=CASE_ID, work_dir=self._work_dir, output_suffix=suffix),
        )
        try:
            reply = await self._runner.run_subagent_turn(handle, "process", task.model_dump(mode="json"))
        except Exception as err:
            self._set_step(CaseStep(agent=label, status="failed", note=str(err)[:200]))
            raise
        finally:
            await self._runner.stop_subagent(handle)
        result = CaseResult.model_validate(reply)
        self._set_step(CaseStep(agent=label, status="done", sandbox=result.sandbox,
                                outputs=[f"{r.path} @ {r.sha256[:8]}" for r in result.outputs]))
        return result

    def _set_step(self, step: CaseStep) -> None:
        with self.relay_board.mutate() as draft:
            steps = draft.cases[-1].steps
            for i, existing in enumerate(steps):
                if existing.agent == step.agent:
                    steps[i] = step  # replace in place: a draft node can't move into a new list
                    return
            steps.append(step)


def _report(headline: str, results: list[CaseResult]) -> str:
    lines = [headline, ""]
    for result in results:
        for receipt in result.received:
            lines.append(f"- {receipt.path} reached {receipt.received_in} through Box "
                         f"(from {receipt.written_in}, {receipt.attempts} check(s))")
        for ref in result.outputs:
            lines.append(f"- {result.agent} in {ref.written_in} wrote {ref.path} ({ref.sha256[:8]})")
    last = results[-1]
    lines += ["", f"{last.agent} summary:", last.summary]
    return "\n".join(lines)
