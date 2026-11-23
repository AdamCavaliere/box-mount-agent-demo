"""Box Relay's chain of custody: the coordinator's Box API checks, and the few facts it asks the
agents' sandboxes for.

Run in the coordinator's sandbox (Box API, no file content):
    get_box_file_version             what Box holds at a path now (ID, version, SHA-1, who changed it)
    wait_for_upload_to_box           wait until an agent's output has reached Box with the content it wrote
    copy_box_file_version            hand an exact version to the other agent's folder, server-side
    wait_for_edit_in_box             wait for a person to change a file in Box (re-review)
    continue_with_changed_file       ask a person whether to go on when a file changed between steps
    publish_memo_to_reviewed_folder  copy the memo version a person approved to Reviewed/

Run in an agent's sandbox, on its Task Queue, reading its own Box Mount (fixed code, no model):
    read_supplier_from_extraction        the supplier and payment terms from extraction.json (Extractor)
    read_findings_from_review            the findings from review_findings.json (Reviewer)
    read_payment_standard_from_playbook  the payment-terms standard from playbook.md (Reviewer)
    read_contract_in_extractor_sandbox,
    read_file_in_reviewer_sandbox        for answering questions

So the PDF stays in the Extractor's sandbox and extraction.json reaches only the Reviewer's;
what crosses to the coordinator is versions, hashes and a few checked facts, through Temporal.

No ``from __future__ import annotations`` (activity tool signatures are read concretely).
"""

import asyncio
import hashlib
import json
from datetime import datetime, timedelta

from pydantic import BaseModel
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError
from temporalio.workflow import ActivityConfig

from temporal_agent_kit import agent
from temporal_agent_kit.agent import Injected

from . import box_api, playbook
from .config import QUEUES
from .databricks_activities import ReviewFindings
from .handoff_activities import _relative
from .activities import _pdf_text
from .models import BoxVersion, FileText
from .sandbox import MOUNT_ROOT, sandbox_name

BOX_CALL = ActivityConfig(
    start_to_close_timeout=timedelta(seconds=30),
    retry_policy=RetryPolicy(initial_interval=timedelta(seconds=1), backoff_coefficient=2.0,
                             maximum_interval=timedelta(seconds=10), maximum_attempts=8),
)
WAIT_FOR_BOX = ActivityConfig(  # a retryable "not yet" every few seconds, for up to ~5 minutes
    start_to_close_timeout=timedelta(seconds=30),
    retry_policy=RetryPolicy(initial_interval=timedelta(seconds=2), backoff_coefficient=1.5,
                             maximum_interval=timedelta(seconds=5), maximum_attempts=72),
)
WAIT_FOR_PERSON = ActivityConfig(  # someone editing in Box: check every 5 s for up to an hour
    start_to_close_timeout=timedelta(seconds=30),
    retry_policy=RetryPolicy(initial_interval=timedelta(seconds=5), backoff_coefficient=1.0,
                             maximum_attempts=720),
)


def _log(action: str, text: str) -> None:
    """One line per Box API action, for `docker logs` and the ticker."""
    now = datetime.now().strftime("%H:%M:%S.%f")[:12]
    print(f"[{sandbox_name()} box-api] {now} {action} {text}", flush=True)


async def _call(fn, *args):
    return await asyncio.to_thread(fn, *args)


# -- the coordinator's Box API checks ------------------------------------------------------------

@agent.activity_tool_defn(inherently_safe=True, activity_config=BOX_CALL)
async def get_box_file_version(path: str) -> BoxVersion:
    """What Box holds at a path now: file ID, version, SHA-1, and who last changed it."""
    found = await _call(box_api.version, path)
    if found is None:
        raise ApplicationError(f"{path} is not in Box", type="NotInBox", non_retryable=True)
    return found


@agent.activity_tool_defn(inherently_safe=True, activity_config=WAIT_FOR_BOX)
async def wait_for_upload_to_box(path: str, sha1: str) -> BoxVersion:
    """Wait until an agent's output has reached Box with exactly the content it wrote (its Box Mount
    uploads it a few seconds after the write)."""
    found = await _call(box_api.version, path)
    if found is not None and found.sha1 == sha1:
        _log("UPLOADED", f"{path} v{found.version_id} ({sha1[:8]})")
        return found
    state = "not in Box yet" if found is None else f"Box has {found.sha1[:8]}"
    raise ApplicationError(f"{path} @ {sha1[:8]}: {state}", type="NotInBoxYet")


@agent.activity_tool_defn(inherently_safe=True, activity_config=BOX_CALL)
async def copy_box_file_version(source: BoxVersion, dest_path: str) -> BoxVersion:
    """Copy one exact version of a file to another folder, on the Box side."""
    copied = await _call(box_api.copy, source, dest_path)
    _log("COPY", f"{source.path} v{source.version_id} -> {copied.path} ({copied.sha1[:8]})")
    return copied


@agent.activity_tool_defn(inherently_safe=True, activity_config=WAIT_FOR_PERSON)
async def wait_for_edit_in_box(path: str, version_id: str) -> BoxVersion:
    """Wait until someone saves a new version of a file in Box."""
    found = await _call(box_api.version, path)
    if found is not None and found.version_id != version_id:
        _log("EDITED", f"{path} v{found.version_id} by {found.modified_by or 'someone'}")
        return found
    raise ApplicationError(f"{path} unchanged in Box (still v{version_id}): waiting for an edit", type="NoEditYet")


class Decision(BaseModel):
    """Always go_on=True when it returns: the approval card is the decision. Reject never reaches
    this activity; the workflow gets ToolApprovalDenied instead and stops the run."""
    go_on: bool


@agent.activity_tool_defn(inherently_safe=False, activity_config=BOX_CALL)
async def continue_with_changed_file(message: str) -> Decision:
    """Ask a person whether the coordinator should go on with a file that changed in Box between steps.
    Approve to continue with the new version (the coordinator redoes what depends on it); reject to stop."""
    return Decision(go_on=True)


class Published(BaseModel):
    published: BoxVersion
    url: str
    live_changed: bool


@agent.activity_tool_defn(inherently_safe=False, activity_config=BOX_CALL)
async def publish_memo_to_reviewed_folder(message: str, memo: Injected[BoxVersion], dest_path: Injected[str]) -> Published:
    """Publish a review memo to Legal: copy the exact version the approver saw to Reviewed/ in Box.
    Requires human approval.

    `message` is what the approver reads (markdown, with a link to the memo in Box); the memo
    version and destination are injected, so the card shows only the message."""
    published = await _call(box_api.copy, memo, dest_path)
    live = await _call(box_api.version, memo.path)
    _log("PUBLISH", f"{memo.path} v{memo.version_id} -> {published.path}")
    return Published(published=published, url=box_api.url(published.file_id),
                     live_changed=live is not None and live.version_id != memo.version_id)


# -- facts from the agents' sandboxes (fixed code on their own Task Queues) -----------------------

def _read_pinned(path: str, sha1: str) -> bytes:
    """The file in this sandbox's Box Mount, only if it is exactly the pinned version."""
    try:
        data = (MOUNT_ROOT / _relative(path)).read_bytes()
    except FileNotFoundError:
        raise ApplicationError(f"{path} has not synced into {sandbox_name()} yet", type="NotSyncedYet")
    have = hashlib.sha1(data).hexdigest()
    if have != sha1:
        raise ApplicationError(f"{path} in {sandbox_name()} is {have[:8]}, not the pinned {sha1[:8]}",
                               type="NotSyncedYet")
    return data


def _in(role: str, base: ActivityConfig = BOX_CALL) -> ActivityConfig:
    return ActivityConfig(**base, task_queue=QUEUES[role])


class ExtractionFacts(BaseModel):
    supplier: str
    country: str
    payment_days: int | None
    read_in: str


@agent.activity_tool_defn(inherently_safe=True,
                           activity_config=_in("extractor", WAIT_FOR_BOX))
async def read_supplier_from_extraction(path: str, sha1: str) -> ExtractionFacts:
    """In the Extractor's sandbox: the supplier and payment terms from the pinned extraction.json."""
    contract = json.loads(_read_pinned(path, sha1)).get("contract") or {}
    supplier = contract.get("supplier") if isinstance(contract.get("supplier"), dict) else {}
    name = str((supplier or {}).get("legal_name") or "").strip()
    if not name:  # fallback: the party the contract calls the Supplier
        for party in contract.get("parties") or []:
            text = json.dumps(party) if isinstance(party, dict) else str(party)
            if "supplier" in text.lower():
                name = str(party.get("name") if isinstance(party, dict) else text.split("(")[0]).strip()
                break
    days = (contract.get("key_terms") or {}).get("payment_days")
    try:
        days = int(days) if days is not None else None
    except (TypeError, ValueError):
        days = None
    return ExtractionFacts(supplier=name, country=str((supplier or {}).get("country") or ""),
                           payment_days=days, read_in=sandbox_name())


@agent.activity_tool_defn(inherently_safe=True,
                           activity_config=_in("reviewer", WAIT_FOR_BOX))
async def read_findings_from_review(path: str, sha1: str) -> ReviewFindings:
    """In the Reviewer's sandbox: the findings from the pinned review_findings.json."""
    try:
        return ReviewFindings.model_validate_json(_read_pinned(path, sha1))
    except ValueError as err:
        raise ApplicationError(f"{path} isn't valid review findings: {err}", type="BadFindings", non_retryable=True)


class PlaybookTerms(BaseModel):
    payment_terms: str  # the Standard column as written, e.g. "Net 60"; "" if the row is missing
    payment_days: int | None
    read_in: str


@agent.activity_tool_defn(inherently_safe=True,
                           activity_config=_in("reviewer", WAIT_FOR_BOX))
async def read_payment_standard_from_playbook(path: str, sha1: str) -> PlaybookTerms:
    """In the Reviewer's sandbox: the payment-terms standard from the pinned playbook version."""
    text = _read_pinned(path, sha1).decode(errors="replace")
    return PlaybookTerms(payment_terms=playbook.standard(text, "Payment terms") or "",
                         payment_days=playbook.payment_days(text), read_in=sandbox_name())


@agent.activity_tool_defn(inherently_safe=True,
                           activity_config=_in("extractor"))
async def read_contract_in_extractor_sandbox(path: str) -> FileText:
    """Text of the contract PDF, read in the Extractor's sandbox (the only one that has it).
    Path relative to the Extractor's folder, e.g. 'Incoming/<contract>.pdf'."""
    return FileText(path=path, text=_pdf_text(path), read_in=sandbox_name())


@agent.activity_tool_defn(inherently_safe=True,
                           activity_config=_in("reviewer"))
async def read_file_in_reviewer_sandbox(path: str) -> FileText:
    """Text of a file in the Reviewer's folder: the playbook ('Playbook/playbook.md'), the handed-off
    extraction.json, or the review memo ('Work/<case>/<run>/review_memo.md')."""
    target = MOUNT_ROOT / _relative(path)
    if not target.is_file():
        raise ApplicationError(f"No such file in the Reviewer's folder: {path}", type="NotFound", non_retryable=True)
    return FileText(path=path, text=target.read_text(errors="replace")[:60_000], read_in=sandbox_name())


CUSTODY_TOOLS = [get_box_file_version, wait_for_upload_to_box, copy_box_file_version, wait_for_edit_in_box,
                 continue_with_changed_file, publish_memo_to_reviewed_folder, read_supplier_from_extraction, read_findings_from_review,
                 read_payment_standard_from_playbook, read_contract_in_extractor_sandbox, read_file_in_reviewer_sandbox]
