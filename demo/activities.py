"""The agents' tools. Each is an activity, so it runs in the sandbox of the agent that calls it
and reports that sandbox's name: the document tools really open the PDF Box Mount synced into
that sandbox. The enterprise systems (ERP vendor master, sanctions lists, clause library) are
simulated from ``fixtures`` with realistic latency.

No ``from __future__ import annotations`` (activity tool signatures are read concretely).
"""

import asyncio
import getpass
import os
import random
import re
from datetime import timedelta
from pathlib import Path

from pydantic import BaseModel, Field
from pypdf import PdfReader
from temporalio import activity
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError
from temporalio.workflow import ActivityConfig

from temporal_agent_kit import agent


from . import fixtures, playbook
from .handoff_activities import _relative
from .models import FileText, SandboxInfo
from .sandbox import MOUNT_ROOT, machine, mount_status, sandbox_name

FLAKY_FLAG = Path("/run/demo/flaky")  # `just flaky on` creates it in the coordinator's sandbox

QUICK = ActivityConfig(start_to_close_timeout=timedelta(seconds=30))
SYSTEM_CALL = ActivityConfig(
    start_to_close_timeout=timedelta(seconds=20),
    retry_policy=RetryPolicy(initial_interval=timedelta(seconds=2), backoff_coefficient=2.0,
                             maximum_interval=timedelta(seconds=10), maximum_attempts=6),
)


async def _latency(lo: float = 0.6, hi: float = 1.8) -> None:
    await asyncio.sleep(random.uniform(lo, hi))


def _pdf_text(path: str) -> str:
    target = MOUNT_ROOT / _relative(path)
    if not target.is_file():
        raise ApplicationError(f"No such document in the Box folder: {path}", type="NotFound", non_retryable=True)
    return "\n".join(page.extract_text() or "" for page in PdfReader(target).pages)


# -- every agent ---------------------------------------------------------------------------

@agent.activity_tool_defn(inherently_safe=True, activity_config=QUICK)
async def check_sandbox_and_box_mount() -> SandboxInfo:
    """Report which sandbox this agent is running in and the state of its Box Mount."""
    healthy, status = mount_status()
    return SandboxInfo(
        sandbox=sandbox_name(),
        machine=machine(),
        user=getpass.getuser(),
        box_mount=status,
        box_mount_healthy=healthy,
        box_folder_entries=sorted(n for n in os.listdir(MOUNT_ROOT) if not n.startswith(("_", "."))),
        box_credentials_visible_to_agent=any(k.startswith("BOX_") for k in os.environ)
        or os.access("/home/boxsvc/.box-mount/box-jwt-config.json", os.R_OK),
    )


# -- the Extractor -------------------------------------------------------------------------

class DocumentText(BaseModel):
    path: str
    pages: int
    text: str
    sandbox: str


@agent.activity_tool_defn(inherently_safe=True, activity_config=QUICK)
async def read_pdf_text_from_box(path: str) -> DocumentText:
    """Extract the text of a PDF in the Box folder (path relative to the folder, e.g. 'Incoming/x.pdf')."""
    await _latency(1.0, 2.5)
    rel = _relative(path)
    text = _pdf_text(rel)
    return DocumentText(path=rel, pages=len(PdfReader(MOUNT_ROOT / rel).pages), text=text, sandbox=sandbox_name())


# -- the supplier check: ERP vendor master and sanctions lists (simulated) -------------------

class VendorRecord(BaseModel):
    found: bool
    vendor_id: str = ""
    name: str = ""
    status: str = ""
    payment_terms_on_file: str = ""
    system: str = "ERP vendor master (simulated)"
    sandbox: str


class SanctionsResult(BaseModel):
    name: str
    lists_checked: list[str]
    hits: int
    sandbox: str


async def _erp_call() -> None:
    """A call to the simulated ERP. With `just flaky on`, its first two attempts time out."""
    attempt = activity.info().attempt
    if FLAKY_FLAG.exists() and attempt <= 2:
        await asyncio.sleep(1.5)
        raise ApplicationError(f"ERP vendor-master API timed out (simulated outage, attempt {attempt})", type="ERPTimeout")
    await _latency(1.0, 2.0)


def _vendor_record(rec: dict | None) -> VendorRecord:
    if rec is None:
        return VendorRecord(found=False, sandbox=sandbox_name())
    return VendorRecord(found=True, vendor_id=rec["vendor_id"], name=rec["name"], status=rec["status"],
                        payment_terms_on_file=rec["payment_terms"], sandbox=sandbox_name())


def _legal_name_key(name: str) -> str:
    """'Halvorsen Industrial Supply Co.' and 'HALVORSEN INDUSTRIAL SUPPLY COMPANY' match."""
    words = re.sub(r"[^a-z0-9 ]", " ", name.lower()).split()
    return " ".join(w for w in words if w not in {"co", "company", "inc", "incorporated", "llc", "ltd", "corp", "corporation", "the"})


@agent.activity_tool_defn(inherently_safe=True, activity_config=SYSTEM_CALL)
async def erp_find_vendor_by_name(legal_name: str) -> VendorRecord:
    """Find a vendor in the ERP vendor master by legal name (e.g. 'Halvorsen Industrial Supply Co.')."""
    await _erp_call()
    key = _legal_name_key(legal_name)
    return _vendor_record(next((r for r in fixtures.VENDOR_MASTER.values() if _legal_name_key(r["name"]) == key), None))


@agent.activity_tool_defn(inherently_safe=True, activity_config=SYSTEM_CALL)
async def screen_vendor_against_sanctions_lists(name: str, country: str) -> SanctionsResult:
    """Screen a party against sanctions and denied-party lists (simulated)."""
    await _latency(0.8, 1.5)
    return SanctionsResult(name=name, lists_checked=["OFAC SDN", "BIS Entity List", "EU Consolidated"],
                           hits=0, sandbox=sandbox_name())


# -- contract review -----------------------------------------------------------------------

@agent.activity_tool_defn(inherently_safe=True, activity_config=QUICK)
async def read_handed_off_file_from_box(path: str) -> FileText:
    """Read a text file another agent handed off (e.g. extraction.json) from this sandbox's Box Mount."""
    rel = _relative(path)
    return FileText(path=rel, text=(MOUNT_ROOT / rel).read_text(), read_in=sandbox_name())


class ClauseStandard(BaseModel):
    clause_type: str
    approved_language: str
    sandbox: str


class RiskFinding(BaseModel):
    clause: str
    severity: str = Field(description="low, medium or high")
    issue: str


class RiskScore(BaseModel):
    score: int = Field(description="0 (clean) to 100")
    rating: str
    high: int
    medium: int
    low: int
    sandbox: str


@agent.activity_tool_defn(inherently_safe=True, activity_config=SYSTEM_CALL)
async def get_approved_clause_language(clause_type: str) -> ClauseStandard:
    """Fetch approved fallback language for a clause type. Types: renewal, payment_terms,
    price_adjustment, limitation_of_liability, indemnification, termination, insurance, governing_law."""
    await _latency(0.5, 1.2)
    key = clause_type.strip().lower().replace(" ", "_")
    if key not in fixtures.CLAUSE_LIBRARY:
        raise ApplicationError(f"Unknown clause type {clause_type!r}; use one of {sorted(fixtures.CLAUSE_LIBRARY)}",
                               type="UnknownClause", non_retryable=True)
    language = fixtures.CLAUSE_LIBRARY[key]
    if key == "payment_terms":
        # The approved terms are the playbook's, as Legal last saved it in Box (read from this
        # sandbox's Box Mount), so the fallback language never disagrees with the rule.
        try:
            days = playbook.payment_days((MOUNT_ROOT / playbook.PLAYBOOK_PATH).read_text())
        except FileNotFoundError:
            days = None
        if days is None:
            raise ApplicationError("The playbook has no payment-terms standard to draft language from",
                                   type="NoPlaybookStandard", non_retryable=True)
        language = language.format(days=days)
    return ClauseStandard(clause_type=key, approved_language=language, sandbox=sandbox_name())


@agent.activity_tool_defn(inherently_safe=True, activity_config=QUICK)
async def score_contract_risk(findings: list[RiskFinding]) -> RiskScore:
    """Score a contract's overall risk from its review findings."""
    await _latency(0.4, 0.9)
    counts = {s: sum(1 for f in findings if f.severity.lower() == s) for s in ("high", "medium", "low")}
    score = min(100, 25 * counts["high"] + 10 * counts["medium"] + 3 * counts["low"])
    rating = "high" if score >= 60 else "medium" if score >= 25 else "low"
    return RiskScore(score=score, rating=rating, sandbox=sandbox_name(), **counts)


EXTRACTOR_TOOLS = [read_pdf_text_from_box]
REVIEWER_TOOLS = [get_approved_clause_language, score_contract_risk, read_handed_off_file_from_box]
# The coordinator's supplier check (its other steps are in custody_activities and databricks_activities).
COORDINATOR_TOOLS = [erp_find_vendor_by_name, screen_vendor_against_sanctions_lists]
