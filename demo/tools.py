"""The agents' tools. Each is an activity, so it runs in the sandbox of the agent that calls it
and reports that sandbox's name: the document tools really open the PDF Box Mount synced into
that sandbox. The enterprise systems (ERP vendor master, PO system, sanctions list, clause
library) are simulated from ``fixtures`` with realistic latency.

No ``from __future__ import annotations`` (activity tool signatures are read concretely).
"""

import asyncio
import getpass
import hashlib
import json
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

from temporal_agent_harness.harness import agent
from temporal_agent_harness.harness.agent import Injected

from .quiet_tools import quiet_activity_tool

from . import fixtures
from .handoff import _relative
from .models import SandboxInfo
from .sandbox import MOUNT_ROOT, machine, mount_status, sandbox_name

ITEM_MAP = Path("/run/boxmount/item_map.json")  # local file -> Box ID, published by the supervisor
BOX_WEB = "https://app.box.com"

FLAKY_FLAG = Path("/run/demo/flaky")  # `just flaky on` creates it in the compliance sandboxes

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

@quiet_activity_tool(name="check_sandbox_and_box_mount", inherently_safe=True, activity_config=QUICK)
async def sandbox_info() -> SandboxInfo:
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


# -- extractor -----------------------------------------------------------------------------

class DocumentText(BaseModel):
    path: str
    pages: int
    text: str
    sandbox: str


class DocumentClass(BaseModel):
    path: str
    document_type: str
    confidence: float
    sandbox: str


class InvoiceLine(BaseModel):
    description: str
    quantity: int
    unit_price: float
    amount: float


class InvoiceFields(BaseModel):
    path: str
    invoice_number: str = ""
    invoice_date: str = ""
    po_number: str = ""
    vendor_name: str = ""
    vendor_tax_id: str = ""
    payment_terms: str = ""
    remit_to_bank: str = ""
    remit_to_account_last4: str = ""
    lines: list[InvoiceLine] = Field(default_factory=list)
    freight: float = 0.0
    sales_tax: float = 0.0
    total: float = 0.0
    sandbox: str


@quiet_activity_tool(name="read_pdf_text_from_box", inherently_safe=True, activity_config=QUICK)
async def ocr_document(path: str) -> DocumentText:
    """Extract the text of a PDF in the Box folder (path relative to the folder, e.g. 'Incoming/x.pdf')."""
    await _latency(1.0, 2.5)
    rel = _relative(path)
    text = _pdf_text(rel)
    return DocumentText(path=rel, pages=len(PdfReader(MOUNT_ROOT / rel).pages), text=text, sandbox=sandbox_name())


@quiet_activity_tool(name="classify_document_type", inherently_safe=True, activity_config=QUICK)
async def classify_document(path: str) -> DocumentClass:
    """Classify a document in the Box folder (invoice, contract, purchase order, other)."""
    await _latency()
    text = _pdf_text(path).lower()
    if "invoice number" in text:
        kind, conf = "invoice", 0.98
    elif "agreement" in text and "governing law" in text:
        kind, conf = "contract/msa", 0.96
    elif "purchase order" in text:
        kind, conf = "purchase_order", 0.9
    else:
        kind, conf = "other", 0.5
    return DocumentClass(path=_relative(path), document_type=kind, confidence=conf, sandbox=sandbox_name())


def _money(s: str) -> float:
    return float(s.replace("$", "").replace(",", ""))


@quiet_activity_tool(name="extract_invoice_fields_from_pdf", inherently_safe=True, activity_config=QUICK)
async def extract_invoice_fields(path: str) -> InvoiceFields:
    """Extract structured fields (header, remit-to, line items, totals) from an invoice PDF."""
    await _latency(1.0, 2.0)
    text = _pdf_text(path)

    def field(label: str) -> str:
        m = re.search(rf"{label}:\s*(.+)", text)
        return m.group(1).strip() if m else ""

    lines = [
        InvoiceLine(description=m.group(1).strip(), quantity=int(m.group(2).replace(",", "")),
                    unit_price=_money(m.group(3)), amount=_money(m.group(4)))
        for m in re.finditer(r"^(.+?)\s+([\d,]+)\s+(\$[\d,]+\.\d\d)\s+(\$[\d,]+\.\d\d)\s*$", text, re.M)
    ]
    account = field("Account")
    return InvoiceFields(
        path=_relative(path),
        invoice_number=field("Invoice Number"),
        invoice_date=field("Invoice Date"),
        po_number=field("PO Number"),
        vendor_name=field("Vendor"),
        vendor_tax_id=field("Tax ID"),
        payment_terms=field("Terms"),
        remit_to_bank=field("Bank"),
        remit_to_account_last4=account[-4:] if account else "",
        lines=lines,
        freight=_money(field("Freight") or "0"),
        sales_tax=_money(field("Sales Tax") or "0"),
        total=_money(field("Total Due") or "0"),
        sandbox=sandbox_name(),
    )


# -- vendor and compliance -----------------------------------------------------------------

class VendorRecord(BaseModel):
    found: bool
    vendor_id: str = ""
    name: str = ""
    status: str = ""
    payment_terms_on_file: str = ""
    bank_on_file: str = ""
    bank_account_last4_on_file: str = ""
    bank_verified: str = ""
    system: str = "ERP vendor master (simulated)"
    sandbox: str


class POMatch(BaseModel):
    po_number: str
    found: bool
    approved_total: float = 0.0
    invoice_total: float = 0.0
    variance: float = 0.0
    variance_pct: float = 0.0
    within_tolerance: bool = False
    tolerance_pct: float = fixtures.PO_TOLERANCE * 100
    approver: str = ""
    line_differences: list[str] = Field(default_factory=list)
    sandbox: str


class SanctionsResult(BaseModel):
    name: str
    lists_checked: list[str]
    hits: int
    sandbox: str


@quiet_activity_tool(name="erp_lookup_vendor_record", inherently_safe=True, activity_config=SYSTEM_CALL)
async def lookup_vendor_master(tax_id: str) -> VendorRecord:
    """Look up a vendor in the ERP vendor master by tax ID (e.g. '84-3920175')."""
    attempt = activity.info().attempt
    if FLAKY_FLAG.exists() and attempt <= 2:
        await asyncio.sleep(1.5)
        raise ApplicationError(f"ERP vendor-master API timed out (simulated outage, attempt {attempt})", type="ERPTimeout")
    await _latency(1.0, 2.0)
    rec = fixtures.VENDOR_MASTER.get(tax_id.strip())
    if rec is None:
        return VendorRecord(found=False, sandbox=sandbox_name())
    return VendorRecord(found=True, vendor_id=rec["vendor_id"], name=rec["name"], status=rec["status"],
                        payment_terms_on_file=rec["payment_terms"], bank_on_file=rec["bank"]["bank"],
                        bank_account_last4_on_file=rec["bank"]["account_last4"],
                        bank_verified=rec["bank"]["verified"], sandbox=sandbox_name())


@quiet_activity_tool(name="erp_match_invoice_to_purchase_order", inherently_safe=True, activity_config=SYSTEM_CALL)
async def match_purchase_order(po_number: str, invoice_total: float) -> POMatch:
    """Three-way match: compare an invoice total against its approved purchase order."""
    await _latency(1.0, 2.0)
    po = fixtures.PURCHASE_ORDERS.get(po_number.strip())
    if po is None:
        return POMatch(po_number=po_number, found=False, invoice_total=invoice_total, sandbox=sandbox_name())
    variance = round(invoice_total - po["approved_total"], 2)
    pct = round(100 * variance / po["approved_total"], 2)
    diffs = []
    for (desc, qty, price), (_, iqty, iprice) in zip(po["lines"], fixtures.INVOICE["lines"]):
        if (qty, price) != (iqty, iprice):
            diffs.append(f"{desc}: PO {qty} x ${price:,.2f}, invoiced {iqty} x ${iprice:,.2f}")
    if not po["freight_included"] and fixtures.INVOICE["freight"]:
        diffs.append(f"Freight ${fixtures.INVOICE['freight']:,.2f} invoiced but not on the PO")
    return POMatch(po_number=po_number, found=True, approved_total=po["approved_total"],
                   invoice_total=invoice_total, variance=variance, variance_pct=pct,
                   within_tolerance=abs(pct) <= fixtures.PO_TOLERANCE * 100,
                   approver=po["approver"], line_differences=diffs, sandbox=sandbox_name())


@quiet_activity_tool(name="screen_vendor_against_sanctions_lists", inherently_safe=True, activity_config=SYSTEM_CALL)
async def sanctions_screen(name: str, country: str) -> SanctionsResult:
    """Screen a party against sanctions and denied-party lists (simulated)."""
    await _latency(0.8, 1.5)
    return SanctionsResult(name=name, lists_checked=["OFAC SDN", "BIS Entity List", "EU Consolidated"],
                           hits=0, sandbox=sandbox_name())


# -- contract review -----------------------------------------------------------------------

class FileText(BaseModel):
    path: str
    text: str
    sandbox: str


@quiet_activity_tool(name="read_handed_off_file_from_box", inherently_safe=True, activity_config=QUICK)
async def read_box_text(path: str) -> FileText:
    """Read a text file another agent handed off (e.g. extraction.json) from this sandbox's Box Mount."""
    rel = _relative(path)
    return FileText(path=rel, text=(MOUNT_ROOT / rel).read_text(), sandbox=sandbox_name())


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


@quiet_activity_tool(name="get_approved_clause_language", inherently_safe=True, activity_config=SYSTEM_CALL)
async def clause_library_lookup(clause_type: str) -> ClauseStandard:
    """Fetch approved fallback language for a clause type. Types: renewal, payment_terms,
    price_adjustment, limitation_of_liability, indemnification, termination, insurance, governing_law."""
    await _latency(0.5, 1.2)
    key = clause_type.strip().lower().replace(" ", "_")
    if key not in fixtures.CLAUSE_LIBRARY:
        raise ApplicationError(f"Unknown clause type {clause_type!r}; use one of {sorted(fixtures.CLAUSE_LIBRARY)}",
                               type="UnknownClause", non_retryable=True)
    return ClauseStandard(clause_type=key, approved_language=fixtures.CLAUSE_LIBRARY[key], sandbox=sandbox_name())


@quiet_activity_tool(name="score_contract_risk", inherently_safe=True, activity_config=QUICK)
async def score_risk(findings: list[RiskFinding]) -> RiskScore:
    """Score a contract's overall risk from its review findings."""
    await _latency(0.4, 0.9)
    counts = {s: sum(1 for f in findings if f.severity.lower() == s) for s in ("high", "medium", "low")}
    score = min(100, 25 * counts["high"] + 10 * counts["medium"] + 3 * counts["low"])
    rating = "high" if score >= 60 else "medium" if score >= 25 else "low"
    return RiskScore(score=score, rating=rating, sandbox=sandbox_name(), **counts)


# -- coordinator: actions that need a human ------------------------------------------------

class ActionConfirmation(BaseModel):
    action: str
    case_id: str
    reference: str
    detail: str
    sandbox: str


def _reference(prefix: str, *parts: str) -> str:
    return f"{prefix}-{hashlib.sha256('|'.join(parts).encode()).hexdigest()[:8].upper()}"


@quiet_activity_tool(name="record_accounts_payable_decision", inherently_safe=False, activity_config=QUICK)
async def submit_ap_decision(case_id: str, decision: str, reason: str) -> ActionConfirmation:
    """Record the accounts-payable decision for an invoice: 'approve', 'hold' or 'reject'.
    Requires human approval."""
    await _latency()
    return ActionConfirmation(action=f"ap_{decision}", case_id=case_id, reference=_reference("AP", case_id, decision),
                              detail=reason, sandbox=sandbox_name())


@quiet_activity_tool(name="route_contract_to_legal_or_signature", inherently_safe=False, activity_config=QUICK)
async def route_contract(case_id: str, route: str, notes: str) -> ActionConfirmation:
    """Route the contract: 'legal_review' (with redlines) or 'signature'. Requires human approval."""
    await _latency()
    return ActionConfirmation(action=f"route_{route}", case_id=case_id, reference=_reference("CLM", case_id, route),
                              detail=notes, sandbox=sandbox_name())


EXTRACTOR_TOOLS = [classify_document, ocr_document, extract_invoice_fields]
COMPLIANCE_TOOLS = [lookup_vendor_master, match_purchase_order, sanctions_screen]
REVIEWER_TOOLS = [clause_library_lookup, score_risk, read_box_text]
COORDINATOR_TOOLS = [submit_ap_decision, route_contract]


# -- links into the Box web app, and the booth's publish step -------------------------------

class BoxLink(BaseModel):
    path: str
    box_id: str
    url: str
    sandbox: str


class PublishResult(BaseModel):
    published_path: str
    sha256: str
    sandbox: str


LINK_ACTIVITY = ActivityConfig(
    start_to_close_timeout=timedelta(seconds=10),
    retry_policy=RetryPolicy(initial_interval=timedelta(seconds=1), backoff_coefficient=1.5,
                             maximum_interval=timedelta(seconds=3), maximum_attempts=30),
)


@quiet_activity_tool(name="get_box_web_link", inherently_safe=True, activity_config=LINK_ACTIVITY)
async def box_link(path: str) -> BoxLink:
    """Link to a file in the Box web app. Waits until Box Mount has synced the file and knows its
    Box ID (looked up by the file's inode in this sandbox; no Box credentials involved)."""
    rel = _relative(path)
    st = (MOUNT_ROOT / rel).stat()
    inode = f"unix:{st.st_dev}:{st.st_ino}"
    try:
        entry = json.loads(ITEM_MAP.read_text()).get(inode)
    except (FileNotFoundError, ValueError):
        entry = None
    if not entry:
        raise ApplicationError(f"{rel} is not in Box yet (Box Mount hasn't recorded its Box ID)", type="NotSyncedYet")
    return BoxLink(path=rel, box_id=entry["box_id"], url=f"{BOX_WEB}/file/{entry['box_id']}", sandbox=sandbox_name())


@quiet_activity_tool(name="publish_memo_to_reviewed_folder", inherently_safe=False, activity_config=QUICK)
async def publish_memo(message: str, memo_path: Injected[str]) -> PublishResult:
    """Publish a review memo to Legal: copy it from Work/ to Reviewed/ in Box. Requires human approval.

    `message` is what the approver reads: markdown with a link to the memo in Box. It is the only
    argument the approval card shows (memo_path is injected), and the console renders a lone
    `message` as markdown, so the link is clickable there."""
    src = MOUNT_ROOT / _relative(memo_path)
    if not src.is_file():  # e.g. removed by a reset: retrying can't bring it back
        raise ApplicationError(f"{memo_path} is no longer in Box, so there is nothing to publish",
                               type="MemoMissing", non_retryable=True)
    rel_target = _relative(memo_path).replace("Work/", "Reviewed/", 1)
    target = MOUNT_ROOT / rel_target
    target.parent.mkdir(parents=True, exist_ok=True)
    data = src.read_bytes()
    if not (target.is_file() and target.read_bytes() == data):  # retried after it landed: nothing to do
        target.write_bytes(data)
    return PublishResult(published_path=rel_target, sha256=hashlib.sha256(data).hexdigest(), sandbox=sandbox_name())


BOOTH_TOOLS = [box_link, publish_memo]
