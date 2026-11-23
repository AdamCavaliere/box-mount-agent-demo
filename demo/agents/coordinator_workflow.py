"""Box Relay: contract review across sandboxes, handed off through Box, with the coordinator
keeping a chain of custody for every file.

Each agent sees only its own Box folder, through its own Box Mount:

    Extractor (sandbox-extractor) mounts Extraction/: the contract PDF and its extraction.json
    Reviewer  (sandbox-reviewer)  mounts Review/:     the playbook, the handed-off extraction,
                                                            its memo and findings

The coordinator (this workflow, in sandbox-coordinator) has no mount. It works through the
Box API and sees versions, never content:

    pin the contract and playbook versions in Box
      ──► Extractor reads that exact contract version, writes extraction.json
      ──► check ──► supplier check (ERP + sanctions) and spend profile (Databricks), from facts the
          Extractor's sandbox reads out of the pinned extraction
      ──► copy that exact extraction version into Review/ (server-side, never through the coordinator)
      ──► check ──► Reviewer writes review_memo.md and review_findings.json
      ──► check ──► a person approves ──► the approved memo version is copied to Reviewed/
      ──► the findings go to Databricks

At each check the coordinator asks Box whether any pinned file has a new version. If one has
(someone edited the contract mid-extraction, say), it stops and asks a person: continue with the
new version, redoing the steps that depend on it, or stop. Temporal holds the run in place while
it waits, however long that takes.

Type "go" to run it. Afterwards, edit Playbook/playbook.md in Box and type "re-review". Anything
else is a question, answered by a model that reads the files in the sandbox that holds them.
"""

# No `from __future__ import annotations`: the Agent Kit reads tool and handler annotations
# concretely, and stringized ones fail to resolve.

import asyncio
import re
from pathlib import PurePosixPath

from temporalio import workflow
from temporalio.exceptions import ActivityError, ApplicationError
from temporalio.contrib.workflow_streams import WorkflowStream

with workflow.unsafe.imports_passed_through():
    from temporal_agent_kit import agent
    from temporal_agent_kit.agent_protocol import (
        AgentConfig,
        MidTurn,
        TextMessage,
        TextReply,
        ToolApprovalPolicy,
    )
    from temporal_agent_kit.agent_protocol.events import ReplyDelta
    from temporal_agent_kit.agent_workflow import AgentWorkflowRunner, ToolApprovalDenied

    from ..config import QUEUES, WORKFLOW_TYPES
    from ..activities import erp_find_vendor_by_name, screen_vendor_against_sanctions_lists
    from ..custody_activities import (
        wait_for_edit_in_box,
        wait_for_upload_to_box,
        get_box_file_version,
        continue_with_changed_file,
        copy_box_file_version,
        read_supplier_from_extraction,
        publish_memo_to_reviewed_folder,
        read_contract_in_extractor_sandbox,
        read_file_in_reviewer_sandbox,
        read_findings_from_review,
        read_payment_standard_from_playbook,
    )
    from ..databricks_activities import (
        SpendProfile, databricks_record_review_findings, databricks_supplier_spend_profile,
    )
    from .common import run_model_turn
    from .idle import close_when_idle
    from ..models import (
        BoxVersion, Case, CaseAssignment, CaseBoard, CaseResult, CaseStep, CaseTask, CounterpartyCheck,
        Custody, SpendContext,
    )

CASE_ID = "HALVORSEN-MSA"
EXTRACTION, REVIEW = "Extraction/", "Review/"  # the two agents' folders (each one's mount root)
CONTRACT = "Extraction/Incoming/Halvorsen_Master_Supply_Agreement.pdf"
PLAYBOOK = "Review/Playbook/playbook.md"
FOCUS = ("Focused review: cover only payment terms, renewal, limitation of liability, "
         "indemnification and termination, and keep the memo short. If a clause complies with the "
         "playbook as it stands now, mark it OK with no redline rather than listing it as a finding.")

GO = re.compile(r"^(go|run|start)\b")
RE_REVIEW = re.compile(r"\bre-?review\b|\breview (it )?again\b")

# The steps, in order, and which step to redo when a pinned file changes.
NEXT = {"extract": "handoff", "handoff": "review", "review": "publish"}
ORDER = ["extract", "handoff", "review", "publish"]
REDO = {"contract": "extract", "extraction": "handoff", "handed extraction": "review",
        "playbook": "review", "memo": "publish", "findings": "publish"}

ANSWER = """\
You answer questions about Box Relay. Two agents review a supplier contract, each in its own Linux \
sandbox with its own Box folder: the Extractor's folder holds the contract PDF and its \
extraction.json; the Reviewer's holds the playbook, a copy of the extraction, and the review memo. \
The coordinator (you) has no files of its own: it pins each file version in Box and checks between \
steps that none changed. Read files with the tools, which run in the sandbox that holds them. Be \
brief and concrete, quote the contract or playbook where it helps, and use Markdown. You can't \
change anything. To run the review the user types "go"; after editing Playbook/playbook.md in Box, \
"re-review"."""


def _call_id() -> str:
    return str(workflow.uuid4())


def _status(text: str) -> None:
    """The workflow's 'current details' line in the Temporal UI."""
    workflow.set_current_details(f"**Box Relay** · {text}")


def _url(version: BoxVersion) -> str:
    return f"https://app.box.com/file/{version.file_id}"


def _name(version: BoxVersion) -> str:
    return PurePosixPath(version.path).name


class _Stopped(Exception):
    """A person chose to stop the run (or a check failed); the message says why."""


@agent.defn(name=WORKFLOW_TYPES["coordinator"])
class BoxRelayAgentWorkflow:
    """Two agents in separate sandboxes relay a contract review through Box; the coordinator
    pins every file version and checks it between steps."""

    relay_board = agent.state(CaseBoard)

    @agent.init
    def __init__(self, config: AgentConfig) -> None:
        self._runner = AgentWorkflowRunner(
            config,
            stream=WorkflowStream(),
            # Checks and lookups run freely; continuing after a change, and publishing, wait for a person.
            approval_policy_default=ToolApprovalPolicy.allow_inherently_safe(),
        )
        self._pins: dict[str, BoxVersion] = {}
        self._work = ""
        self._run = ""
        self._extractions = 0
        self._reviews = 0
        self._supplier: CounterpartyCheck | None = None
        self._spend: SpendContext | None = None
        self._ap: SpendProfile | None = None  # the raw Databricks figures, for redoing the arithmetic
        self._contract_days: int | None = None
        self._playbook_terms = ""  # the pinned playbook's payment-terms standard, e.g. "Net 60"
        self._results: list[CaseResult] = []
        self._chat: list = []
        self._turns = 0
        self._busy = False

    @agent.setup
    async def _watch_idle(self) -> None:
        self._idle = asyncio.create_task(
            close_when_idle("Box Relay", lambda: self._turns, lambda: self._busy,
                            # the Agent Kit's own close-signal handler (private; see idle.py)
                            self._runner._handle_close)
        )

    @agent.accepts(mid_turn=MidTurn.REJECT)
    async def ask(self, message: TextMessage) -> TextReply:
        """Type "go" to run the review (Extractor, then Reviewer, handing files through Box). After a
        run, edit Playbook/playbook.md in Box and type "re-review". Ask anything else about the
        contract or the review."""
        self._turns += 1
        self._busy = True
        try:
            text = message.text.strip().lower()
            if GO.match(text):
                return TextReply(text=await self._relay())
            if RE_REVIEW.search(text) and "handed extraction" in self._pins:
                return TextReply(text=await self._re_review())
            return TextReply(text=await self._answer(message.text))
        finally:
            self._busy = False

    # -- a run -----------------------------------------------------------------------------

    async def _relay(self) -> str:
        self._run = f"run-{workflow.now():%m%d-%H%M%S}"
        self._work = f"Work/{CASE_ID}/{self._run}"
        self._pins, self._results = {}, []
        self._extractions = self._reviews = 0
        self._supplier = self._spend = self._ap = self._contract_days = None
        self._playbook_terms = ""
        with self.relay_board.mutate() as draft:
            draft.cases.append(Case(case_id=CASE_ID, run=self._run, documents=[CONTRACT, PLAYBOOK]))
        _status(f"{CASE_ID} {self._run} · pinning the contract and playbook versions in Box")
        for name, path in (("contract", CONTRACT), ("playbook", PLAYBOOK)):
            self._pin(name, await self._runner.run_tool(_call_id(), get_box_file_version, path=path), "start")
        return await self._run_from("extract")

    async def _re_review(self) -> str:
        """Wait (durably) for someone to save a new playbook version in Box, then review again."""
        _status(f"{CASE_ID} · waiting for someone to edit Playbook/playbook.md in Box")
        self._runner.publish(ReplyDelta(text="Waiting for a new version of Playbook/playbook.md in Box. "
                                             "The Reviewer starts as soon as it's saved.\n\n"))
        edited = await self._runner.run_tool(_call_id(), wait_for_edit_in_box, path=PLAYBOOK,
                                             version_id=self._pins["playbook"].version_id)
        self._pin("playbook", edited, "edited in Box")
        return await self._run_from("review")

    async def _run_from(self, step: str) -> str:
        """Run the steps from `step` on, with a version check after each one."""
        try:
            while True:
                if step == "extract":
                    await self._extract()
                elif step == "handoff":
                    await self._handoff()
                elif step == "review":
                    await self._review()
                else:
                    return self._report("Relay complete.") + await self._publish()
                redo = await self._checkpoint(after=step)
                # Go on with the next step, or go back to the earliest step a changed file affects.
                step = min(filter(None, (redo, NEXT[step])), key=ORDER.index)
        except _Stopped as stop:
            _status(f"{CASE_ID} · stopped: {str(stop)[:120]}")
            return self._report("Relay stopped.") + f"\n\n**Stopped:** {stop}"

    async def _extract(self) -> None:
        """The Extractor reads the pinned contract version (its sandbox waits until that exact
        version has synced) and writes extraction.json; the coordinator pins what reached Box."""
        self._extractions += 1
        contract = self._pins["contract"]
        result = await self._delegate(
            "extractor",
            CaseTask(instructions=f"Case {CASE_ID}: extract this supplier contract.",
                     documents=[contract.path.removeprefix(EXTRACTION)],
                     inputs=[contract.ref(EXTRACTION, written_in="Box")]),
            suffix="" if self._extractions == 1 else f"_v{self._extractions}",
        )
        out = result.outputs[0]
        self._pin("extraction", await self._runner.run_tool(
            _call_id(), wait_for_upload_to_box, path=EXTRACTION + out.path, sha1=out.sha1), "Extractor wrote it")

    async def _handoff(self) -> None:
        """Fixed steps in code, then the handoff: the supplier check and spend profile use facts
        read out of the pinned extraction in the Extractor's sandbox, and the coordinator copies
        that exact version into the Reviewer's folder on the Box side."""
        extraction = self._pins["extraction"]
        facts = await self._runner.run_tool(_call_id(), read_supplier_from_extraction,
                                            path=extraction.path.removeprefix(EXTRACTION), sha1=extraction.sha1)
        self._supplier = await self._check_supplier(facts.supplier, facts.country)
        if not self._supplier.cleared:
            raise _Stopped(f"{self._supplier.summary}, so the contract was not sent for review.")
        self._contract_days = facts.payment_days
        self._spend = await self._spend_profile(facts.payment_days)

        step = "handoff to Reviewer"
        self._set_step(CaseStep(agent=step, status="running", note=f"{_name(extraction)} v{extraction.version_id}"))
        _status(f"{CASE_ID} · copying extraction.json v{extraction.version_id} into the Reviewer's folder")
        handed = await self._runner.run_tool(_call_id(), copy_box_file_version, source=extraction,
                                             dest_path=f"{REVIEW}{self._work}/extraction.json")
        self._pin("handed extraction", handed, f"copied from v{extraction.version_id}")
        self._set_step(CaseStep(agent=step, status="done", outputs=[f"{handed.path} v{handed.version_id}"],
                                note="server-side copy; the Reviewer never sees the PDF"))

    async def _review(self) -> None:
        self._reviews += 1
        handed, playbook = self._pins["handed extraction"], self._pins["playbook"]
        # The playbook's payment standard, from the pinned version (read in the Reviewer's sandbox, the
        # only one that has it). Read here, not at the handoff, so a playbook change redoes it.
        terms = await self._runner.run_tool(_call_id(), read_payment_standard_from_playbook,
                                            path=playbook.path.removeprefix(REVIEW), sha1=playbook.sha1)
        self._playbook_terms = terms.payment_terms
        if self._ap is not None:
            self._spend = _spend_context(self._ap, self._contract_days, self._supplier.payment_terms_on_file,
                                         terms.payment_days)
        result = await self._delegate(
            "reviewer",
            # No PDF: the review works from the handed-off extraction.json and the playbook.
            CaseTask(instructions=f"Case {CASE_ID}: review the supplier contract against the playbook. {FOCUS}"
                     + _facts(self._supplier, self._spend, self._playbook_terms, playbook),
                     inputs=[handed.ref(REVIEW, written_in="Box (copied by the coordinator)"),
                             playbook.ref(REVIEW, written_in="Box")]),
            suffix="" if self._reviews == 1 else f"_v{self._reviews}",
        )
        memo, findings = result.outputs[0], result.outputs[1]
        self._pin("memo", await self._runner.run_tool(
            _call_id(), wait_for_upload_to_box, path=REVIEW + memo.path, sha1=memo.sha1), "Reviewer wrote it")
        self._pin("findings", await self._runner.run_tool(
            _call_id(), wait_for_upload_to_box, path=REVIEW + findings.path, sha1=findings.sha1), "Reviewer wrote it")

    # -- the checks between steps ---------------------------------------------------------------

    async def _checkpoint(self, after: str) -> str | None:
        """Ask Box for every pinned file's current version. If any changed since it was pinned,
        stop and ask a person: continue with the new version (redoing the steps that depend on it),
        or stop. Returns the step to redo, or None if nothing changed."""
        _status(f"{CASE_ID} · checking {len(self._pins)} pinned file version(s) in Box after {after}")
        changed: dict[str, BoxVersion] = {}
        for name, pinned in self._pins.items():
            now = await self._runner.run_tool(_call_id(), get_box_file_version, path=pinned.path)
            if now.version_id != pinned.version_id or now.sha1 != pinned.sha1:
                changed[name] = now
                self._mark(name, "changed", f"v{pinned.version_id} → v{now.version_id} after {after}")
            else:
                self._mark(name, "verified", f"unchanged after {after}")
        if not changed:
            return None

        redo = min((REDO[name] for name in changed), key=ORDER.index)
        already_ran = ORDER.index(redo) <= ORDER.index(after)
        lines = [f"**A file changed in Box after the {after} step.**", ""]
        for name, now in changed.items():
            pinned = self._pins[name]
            who = f" by {now.modified_by}" if now.modified_by else ""
            lines.append(f"- **{name}**: [{_name(now)}]({_url(now)}) went from version {pinned.version_id} "
                         f"({pinned.sha1[:8]}) to {now.version_id} ({now.sha1[:8]}){who}.")
        then = (f"the coordinator redoes the **{redo}** step and everything after it" if already_ran
                else f"the **{redo}** step, still to come, uses it")
        lines += ["", f"Approve to continue with the new version: {then}. Reject to stop this run."]
        _status(f"{CASE_ID} · paused: {', '.join(changed)} changed in Box; waiting for a person")
        try:
            await self._runner.run_tool(_call_id(), continue_with_changed_file, message="\n".join(lines))
        except ToolApprovalDenied:
            raise _Stopped(f"{', '.join(changed)} changed in Box after the {after} step, and you chose to stop.")
        for name, now in changed.items():
            self._pin(name, now, f"new version accepted after {after}")
        return redo

    def _pin(self, name: str, version: BoxVersion, why: str) -> None:
        self._pins[name] = version
        entry = Custody(file=f"{name}: {version.path}", version=version.version_id, sha1=version.sha1[:8],
                        pinned_at=why)
        with self.relay_board.mutate() as draft:
            custody = draft.cases[-1].custody
            for i, existing in enumerate(custody):
                if existing.file.startswith(f"{name}: "):
                    custody[i] = entry  # replace in place: a draft node can't move into a new list
                    return
            custody.append(entry)

    def _mark(self, name: str, status: str, note: str) -> None:
        with self.relay_board.mutate() as draft:
            for entry in draft.cases[-1].custody:
                if entry.file.startswith(f"{name}: "):
                    entry.status, entry.note = status, note

    # -- fixed steps in code ------------------------------------------------------------------------

    async def _check_supplier(self, name: str, country: str) -> CounterpartyCheck:
        """Look the supplier up in the ERP and screen it against sanctions lists. No model decides
        whether this runs, Temporal retries the lookups through outages, and a supplier that fails
        never reaches the Reviewer."""
        step = "supplier check"
        self._set_step(CaseStep(agent=step, status="running", note="ERP vendor master + sanctions lists"))
        _status(f"{CASE_ID} · checking the supplier in the ERP and against sanctions lists")
        if not name:
            check = CounterpartyCheck(legal_name="", cleared=False,
                                      summary="the Extractor did not name the supplier in extraction.json")
        else:
            try:
                vendor = await self._runner.run_tool(_call_id(), erp_find_vendor_by_name, legal_name=name)
                screen = await self._runner.run_tool(_call_id(), screen_vendor_against_sanctions_lists, name=name,
                                                     country=country or "not stated")
            except ActivityError as err:
                reason = getattr(err.cause, "message", None) or str(err.cause or err)
                check = CounterpartyCheck(legal_name=name, cleared=False,
                                          summary=f"the ERP or sanctions service was unavailable after retries ({reason})")
            else:
                lists = ", ".join(screen.lists_checked)
                if not vendor.found:
                    cleared, summary = False, f"{name} is not in the ERP vendor master"
                elif vendor.status != "active":
                    cleared, summary = False, f"{name} is {vendor.status} in the ERP vendor master"
                elif screen.hits:
                    cleared, summary = False, f"{name} has {screen.hits} sanctions hit(s) ({lists})"
                else:
                    cleared, summary = True, (f"{name} is vendor {vendor.vendor_id}, active in the ERP and paid today on "
                                              f"{vendor.payment_terms_on_file} (the terms on file for this supplier, not "
                                              f"a company standard); no sanctions hits ({lists})")
                check = CounterpartyCheck(legal_name=name, vendor_id=vendor.vendor_id,
                                          payment_terms_on_file=vendor.payment_terms_on_file,
                                          cleared=cleared, summary=summary)
        self._set_step(CaseStep(agent=step, status="done" if check.cleared else "failed", note=check.summary[:200]))
        return check

    async def _spend_profile(self, contract_days: int | None) -> SpendContext:
        """The supplier's last 12 months of accounts payable from Databricks. Context for the
        review, not a gate: if Databricks can't answer, the run goes on without it."""
        step = "spend profile (Databricks)"
        self._set_step(CaseStep(agent=step, status="running", note="ap_invoices, last 12 months"))
        _status(f"{CASE_ID} · reading the supplier's spend from Databricks")
        try:
            spend = await self._runner.run_tool(_call_id(), databricks_supplier_spend_profile, vendor_id=self._supplier.vendor_id)
        except ActivityError as err:
            reason = getattr(err.cause, "message", None) or str(err.cause or err)
            skipped = isinstance(err.cause, ApplicationError) and err.cause.type == "DatabricksNotConfigured"
            context = SpendContext(available=False, summary=f"no spend data ({reason})")
            self._set_step(CaseStep(agent=step, status="done" if skipped else "failed", note=context.summary[:200]))
            return context
        self._ap = spend
        context = _spend_context(spend, contract_days, self._supplier.payment_terms_on_file)
        self._set_step(CaseStep(agent=step, status="done", sandbox=spend.sandbox, note=context.summary[:200]))
        return context

    # -- a person approves, then the memo is published and the findings recorded --------------------

    async def _publish(self) -> str:
        memo = self._pins["memo"]
        self._runner.publish(ReplyDelta(text=(
            f"**Review ready for approval.** [Open {_name(memo)} in Box]({_url(memo)}) (version "
            f"{memo.version_id}), then approve or reject publishing it to Reviewed/.\n\n")))
        _status(f"{CASE_ID} · waiting for a person to approve publishing {_name(memo)} v{memo.version_id}")
        message = (f"**Publish the {CASE_ID} review to Legal?**\n\n"
                   f"[Open {_name(memo)} in Box]({_url(memo)}) to read it first. Approving copies this "
                   f"version ({memo.version_id}) to Reviewed/ in Box and records the findings in Databricks.")
        if self._supplier is not None:
            message += f"\n\n**Supplier check (ERP + sanctions):** {self._supplier.summary}."
        if self._spend is not None and self._spend.available:
            message += f"\n\n**Spend (Databricks):** {self._spend.summary}."
        message += "\n\n**Versions checked in Box:** " + "; ".join(
            f"{name} v{v.version_id}" for name, v in self._pins.items()) + "."
        dest = f"Reviewed/{self._work.removeprefix('Work/')}/{_name(memo)}"
        try:
            done = await self._runner.run_tool(_call_id(), publish_memo_to_reviewed_folder, message=message,
                                               injections={"memo": memo, "dest_path": dest})
        except ActivityError as err:
            reason = getattr(err.cause, "message", None) or str(err.cause or err)
            return f"\n\n**Couldn't publish:** {reason}. Type `go` for a new run."
        except ToolApprovalDenied:
            _status(f"{CASE_ID} · publishing rejected; the memo stays in the Reviewer's folder")
            return f"\n\n**Not published.** [Open the memo in Box]({_url(memo)}); it stays in the Reviewer's folder."
        _status(f"{CASE_ID} · published to {done.published.path} · type go for a new run")
        note = (" Someone has edited the memo in Box since; the published copy is the version you approved."
                if done.live_changed else "")
        return (f"\n\n**Published to Legal.** [Open the published memo in Box]({done.url}) "
                f"({done.published.path}, version {memo.version_id} as approved).{note}"
                + await self._record_findings(done.published))

    async def _record_findings(self, published: BoxVersion) -> str:
        """After a person approved: the findings, read in the Reviewer's sandbox, go to Databricks."""
        step = "findings to Databricks"
        findings, memo = self._pins["findings"], self._pins["memo"]
        self._set_step(CaseStep(agent=step, status="running", note=f"{_name(findings)} v{findings.version_id}"))
        _status(f"{CASE_ID} · recording the approved findings in Databricks")
        try:
            data = await self._runner.run_tool(_call_id(), read_findings_from_review,
                                               path=findings.path.removeprefix(REVIEW), sha1=findings.sha1)
            recorded = await self._runner.run_tool(
                _call_id(), databricks_record_review_findings, parsed=data, memo_path=memo.path, memo_url=_url(memo),
                published_path=published.path, run_id=self._run, case_id=CASE_ID,
                vendor_id=self._supplier.vendor_id if self._supplier else "",
                supplier=self._supplier.legal_name if self._supplier else "",
            )
        except ActivityError as err:
            reason = getattr(err.cause, "message", None) or str(err.cause or err)
            if isinstance(err.cause, ApplicationError) and err.cause.type == "DatabricksNotConfigured":
                self._set_step(CaseStep(agent=step, status="done", note="skipped: Databricks not configured"))
                return ""
            self._set_step(CaseStep(agent=step, status="failed", note=reason[:200]))
            return f"\n\n**Findings not recorded in Databricks:** {reason}"
        self._set_step(CaseStep(agent=step, status="done", sandbox=recorded.sandbox,
                                outputs=[f"{recorded.table}: {recorded.findings} finding(s)"]))
        return (f"\n\n**Recorded in Databricks:** {recorded.findings} finding(s) in `{recorded.table}` "
                f"({recorded.inserted} new, {recorded.updated} updated).")

    # -- questions ----------------------------------------------------------------------------------

    async def _answer(self, question: str) -> str:
        """A question, not a command: a model answers it, reading files in the sandbox that holds them."""
        _status(f"{CASE_ID} · answering a question")
        lines = [f"- contract, in the Extractor's folder: {CONTRACT.removeprefix(EXTRACTION)}",
                 f"- playbook, in the Reviewer's folder: {PLAYBOOK.removeprefix(REVIEW)}"]
        for name in ("handed extraction", "memo"):
            if name in self._pins:
                lines.append(f"- {name}, in the Reviewer's folder: {self._pins[name].path.removeprefix(REVIEW)}")
        if self._supplier is not None:
            lines.append(f"Supplier check (ERP + sanctions): {self._supplier.summary}")
        if self._spend is not None:
            lines.append(f"Spend profile (Databricks): {self._spend.summary}")
        if self._pins:
            lines.append("Pinned versions in Box: " + "; ".join(f"{n} v{v.version_id} ({v.sha1[:8]})"
                                                                for n, v in self._pins.items()))
        else:
            lines.append("No review has run yet in this session.")
        self._chat.append({"role": "user", "content": "Files:\n" + "\n".join(lines) + f"\n\n{question}"})
        reply, self._chat = await run_model_turn(
            self._runner, name="Box Relay", instructions=ANSWER,
            tools=[read_contract_in_extractor_sandbox, read_file_in_reviewer_sandbox], conversation=self._chat, max_turns=10,
        )
        _status(f"{CASE_ID} · answered a question · type go for a new run")
        return reply

    # -- running an agent ---------------------------------------------------------------------------

    async def _delegate(self, role: str, task: CaseTask, suffix: str = "") -> CaseResult:
        label = role if not suffix else f"{role}{suffix}"
        _status(f"{CASE_ID} · {label} working in its own sandbox (queue {QUEUES[role]})")
        self._set_step(CaseStep(agent=label, status="running", note=f"queue {QUEUES[role]}"))
        handle = await self._runner.start_subagent(
            role, WORKFLOW_TYPES[role], QUEUES[role],
            data=CaseAssignment(case_id=CASE_ID, work_dir=self._work, output_suffix=suffix),
        )
        try:
            reply = await self._runner.run_subagent_turn(handle, "process", task.model_dump(mode="json"))
        except Exception as err:
            self._set_step(CaseStep(agent=label, status="failed", note=str(err)[:200]))
            raise
        finally:
            await self._runner.stop_subagent(handle)
        result = CaseResult.model_validate(reply)
        self._results.append(result)
        self._set_step(CaseStep(agent=label, status="done", sandbox=result.sandbox,
                                outputs=[f"{r.path} @ {r.sha1[:8]}" for r in result.outputs]))
        return result

    def _set_step(self, step: CaseStep) -> None:
        with self.relay_board.mutate() as draft:
            steps = draft.cases[-1].steps
            for i, existing in enumerate(steps):
                if existing.agent == step.agent:
                    steps[i] = step  # replace in place: a draft node can't move into a new list
                    return
            steps.append(step)

    def _report(self, headline: str) -> str:
        lines = [headline, ""]
        if self._supplier is not None:
            lines.append(f"- Supplier check (ERP + sanctions): {self._supplier.summary}")
        if self._spend is not None:
            lines.append(f"- Spend profile (Databricks): {self._spend.summary}")
        for result in self._results:
            for receipt in result.received:
                lines.append(f"- {result.agent} in {receipt.received_in} read {receipt.path} "
                             f"@ {receipt.sha1[:8]} ({receipt.attempts} check(s) for it to sync)")
        if self._pins:
            lines.append("- Versions pinned and checked in Box: " + "; ".join(
                f"{name} v{v.version_id}" for name, v in self._pins.items()))
        if self._results:
            last = self._results[-1]
            lines += ["", f"{last.agent} summary:", last.summary]
        return "\n".join(lines)


def _spend_context(spend, contract_days: int | None, erp_terms: str,
                   playbook_days: int | None = None) -> SpendContext:
    """Plain arithmetic in the workflow: what the contract's payment terms mean against real spend,
    compared with the terms this supplier is paid on today (ERP) and with the playbook's standard."""
    match = re.search(r"(\d+)", erp_terms or "")
    erp_days = int(match.group(1)) if match else None
    paid = f"paid in {spend.avg_days_to_pay:.0f} days on average" if spend.avg_days_to_pay is not None else "no payments yet"
    summary = (f"${spend.spend_12m:,.0f} across {spend.invoices} invoices in the last 12 months, {paid}, "
               f"{spend.late_payments} late, {spend.disputed} disputed")

    def sooner(days: int | None, against: str) -> str:
        if contract_days is None or days is None or contract_days >= days or not spend.spend_12m:
            return ""
        tied_up = round(spend.spend_12m * (days - contract_days) / 365, -3)
        return (f"; against {against} (Net {days}), the contract's Net {contract_days} pays every invoice "
                f"{days - contract_days} days sooner, tying up about ${tied_up:,.0f} more working capital")

    summary += sooner(erp_days, "the terms this supplier is paid on today in the ERP")
    if playbook_days is not None and playbook_days != erp_days:
        summary += sooner(playbook_days, "the playbook's standard")
    return SpendContext(available=True, summary=summary, source=spend.source)


def _facts(check: CounterpartyCheck | None, spend: SpendContext | None, playbook_terms: str = "",
           playbook: BoxVersion | None = None) -> str:
    """The coordinator's own findings, as a labelled block the Reviewer's memo must restate."""
    facts = []
    if playbook_terms and playbook is not None:
        facts.append(f"- Playbook ({_name(playbook)} v{playbook.version_id}), the company standard: payment terms "
                     f"{playbook_terms}.")
    if check is not None and check.cleared:
        facts.append(f"- ERP vendor master: {check.summary}.")
    if spend is not None and spend.available:
        facts.append(f"- Databricks ({spend.source}), accounts payable: {spend.summary}.")
    if not facts:
        return ""
    return ("\n\nFacts from the coordinator (put them in the memo's Business context section; the company "
            "standard is always the playbook's, and the ERP's terms are only what this supplier is paid on today):\n"
            + "\n".join(facts))
