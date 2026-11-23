"""Box Relay's Databricks steps, as activities that run in the coordinator's sandbox.

- `databricks_supplier_spend_profile`: the supplier's last 12 months of accounts payable (spend, days to pay,
  disputes), read right after the supplier check.
- `databricks_record_review_findings`: after a person approves the review, the Reviewer's structured findings
  (read from review_findings.json in the Reviewer's sandbox) are merged into a Delta table.

Neither is up to a model: the coordinator's code calls them. Each statement is submitted, then polled
with a heartbeat that carries its statement ID. If the worker dies mid-query, Temporal retries the
activity on the restarted worker, which picks the same statement up from the last heartbeat
instead of running it again. A stopped SQL warehouse just means a longer wait in PENDING.

No ``from __future__ import annotations`` (activity tool signatures are read concretely).
"""

import asyncio
import json
from datetime import timedelta

from pydantic import BaseModel
from temporalio import activity
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError
from temporalio.workflow import ActivityConfig

from temporal_agent_kit import agent

from . import databricks_sql as dbsql
from .sandbox import sandbox_name

SQL_CALL = ActivityConfig(
    # Room for a stopped warehouse to start; the heartbeat says the query is still alive.
    start_to_close_timeout=timedelta(minutes=10),
    heartbeat_timeout=timedelta(seconds=30),
    retry_policy=RetryPolicy(initial_interval=timedelta(seconds=2), backoff_coefficient=2.0,
                             maximum_interval=timedelta(seconds=30), maximum_attempts=5),
)


def _settings() -> dbsql.Settings:
    cfg = dbsql.settings()
    if cfg is None:
        raise ApplicationError("Databricks is not configured in this sandbox (see .env.databricks.example)",
                               type="DatabricksNotConfigured", non_retryable=True)
    return cfg


async def _run(cfg: dbsql.Settings, statement: str, params: dict[str, str]) -> list[dict]:
    """Run one statement to completion, resuming the one from the last heartbeat after a retry."""
    details = activity.info().heartbeat_details
    resp = None
    if details:  # a previous attempt submitted it: keep waiting on that statement, don't resubmit
        resp = await asyncio.to_thread(dbsql.get, details[0])
        if resp.status.state in (dbsql.StatementState.CANCELED, dbsql.StatementState.CLOSED):
            resp = None
    if resp is None:
        resp = await asyncio.to_thread(dbsql.submit, cfg, statement, params)
    try:
        while resp.status.state not in dbsql.DONE:
            # Shows in the Temporal UI as this activity's heartbeat: the statement and its state
            # (PENDING while the warehouse starts, RUNNING once it executes).
            activity.heartbeat(resp.statement_id, resp.status.state.value)
            await asyncio.sleep(2)
            resp = await asyncio.to_thread(dbsql.get, resp.statement_id)
    except asyncio.CancelledError:  # the workflow gave up on it: stop the query too
        await asyncio.to_thread(dbsql.cancel, resp.statement_id)
        raise
    if resp.status.state == dbsql.StatementState.FAILED:
        # A SQL error (missing table, bad column) won't fix itself on retry.
        raise ApplicationError(f"Databricks SQL failed: {dbsql.error(resp)}", type="DatabricksSqlFailed",
                               non_retryable=True)
    if resp.status.state != dbsql.StatementState.SUCCEEDED:
        raise ApplicationError(f"Databricks statement {resp.status.state.value}", type="DatabricksStatementEnded")
    return dbsql.rows(resp)


def _num(value: str | None) -> float:
    return float(value) if value not in (None, "") else 0.0


# -- read: the supplier's spend --------------------------------------------------------------

class SpendProfile(BaseModel):
    vendor_id: str
    invoices: int
    spend_12m: float
    avg_days_to_pay: float | None
    late_payments: int
    disputed: int
    source: str
    sandbox: str


@agent.activity_tool_defn(inherently_safe=True, activity_config=SQL_CALL)
async def databricks_supplier_spend_profile(vendor_id: str) -> SpendProfile:
    """The supplier's accounts-payable history for the last 12 months, from Databricks: invoice
    count, spend, average days to pay, late payments and disputed invoices."""
    cfg = _settings()
    table = cfg.table("ap_invoices")
    [row] = await _run(cfg, dbsql.SPEND_PROFILE_SQL.format(table=table), {"vendor_id": vendor_id})
    return SpendProfile(
        vendor_id=vendor_id,
        invoices=int(_num(row["invoices"])),
        spend_12m=round(_num(row["spend_12m"]), 2),
        avg_days_to_pay=round(_num(row["avg_days_to_pay"]), 1) if row["avg_days_to_pay"] else None,
        late_payments=int(_num(row["late_payments"])),
        disputed=int(_num(row["disputed"])),
        source=table,
        sandbox=sandbox_name(),
    )


# -- write: the approved findings --------------------------------------------------------------

class Finding(BaseModel):
    clause: str
    contract_says: str = ""
    playbook_rule: str = ""
    severity: str = ""
    issue: str = ""


class ReviewFindings(BaseModel):
    """review_findings.json, as the Reviewer writes it."""
    risk_score: int | None = None
    rating: str = ""
    recommendation: str = ""
    findings: list[Finding]


class FindingsRecorded(BaseModel):
    table: str
    findings: int
    inserted: int
    updated: int
    sandbox: str


@agent.activity_tool_defn(inherently_safe=True, activity_config=SQL_CALL)
async def databricks_record_review_findings(parsed: ReviewFindings, memo_path: str, memo_url: str, published_path: str,
                          run_id: str, case_id: str, vendor_id: str, supplier: str) -> FindingsRecorded:
    """Merge an approved review's findings into the Databricks review_findings table, one row per
    finding. Safe to repeat: rows are keyed by memo and position."""
    cfg = _settings()
    table = cfg.table("review_findings")
    params = {
        "memo_path": memo_path, "memo_url": memo_url, "published_path": published_path,
        "run_id": run_id, "case_id": case_id, "vendor_id": vendor_id, "supplier": supplier,
        "workflow_id": activity.info().workflow_id,
        "risk_score": str(parsed.risk_score if parsed.risk_score is not None else ""),
        "rating": parsed.rating, "recommendation": parsed.recommendation,
        "findings": json.dumps([f.model_dump() for f in parsed.findings]),
    }
    result = await _run(cfg, dbsql.RECORD_FINDINGS_SQL.format(table=table), params)
    counts = result[0] if result else {}
    return FindingsRecorded(table=table, findings=len(parsed.findings),
                            inserted=int(_num(counts.get("num_inserted_rows"))),
                            updated=int(_num(counts.get("num_updated_rows"))), sandbox=sandbox_name())


DATABRICKS_TOOLS = [databricks_supplier_spend_profile, databricks_record_review_findings]
