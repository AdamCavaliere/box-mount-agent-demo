"""Databricks SQL for Box Relay: settings, the two tables, and a submit-then-poll statement runner.

Box holds the documents; Databricks holds the facts about the supplier. The coordinator reads the
supplier's accounts-payable history from `ap_invoices` and, after a person approves the review,
writes the findings to `review_findings`.

Settings come from `.env.databricks`, which only the coordinator's sandbox loads (the agents' sandboxes
never see them):
    DATABRICKS_HOST, DATABRICKS_TOKEN   (or DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET)
    DATABRICKS_WAREHOUSE_ID             a SQL warehouse
    DATABRICKS_CATALOG                  default "workspace"
    DATABRICKS_SCHEMA                   default "box_relay"

Statements go through the SQL Statement Execution API: submit, then poll until the warehouse
has run it. A stopped warehouse first has to start, so a statement can sit in PENDING for a while.
"""

import os
from dataclasses import dataclass

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import (
    ExecuteStatementRequestOnWaitTimeout,
    StatementParameterListItem,
    StatementResponse,
    StatementState,
)

DONE = {StatementState.SUCCEEDED, StatementState.FAILED, StatementState.CANCELED, StatementState.CLOSED}


@dataclass(frozen=True)
class Settings:
    warehouse_id: str
    catalog: str
    schema: str

    def table(self, name: str) -> str:
        return f"{self.catalog}.{self.schema}.{name}"


def settings() -> Settings | None:
    """None when Databricks isn't configured (the coordinator then skips its Databricks steps)."""
    has_auth = os.environ.get("DATABRICKS_TOKEN") or os.environ.get("DATABRICKS_CLIENT_SECRET")
    if not (os.environ.get("DATABRICKS_HOST") and os.environ.get("DATABRICKS_WAREHOUSE_ID") and has_auth):
        return None
    return Settings(
        warehouse_id=os.environ["DATABRICKS_WAREHOUSE_ID"],
        catalog=os.environ.get("DATABRICKS_CATALOG") or "workspace",
        schema=os.environ.get("DATABRICKS_SCHEMA") or "box_relay",
    )


_client: WorkspaceClient | None = None


def client() -> WorkspaceClient:
    """One client per process; the SDK reads the host and credentials from the environment."""
    global _client
    if _client is None:
        _client = WorkspaceClient()
    return _client


def submit(cfg: Settings, statement: str, params: dict[str, str] | None = None) -> StatementResponse:
    """Start a statement. Returns after at most 10 s, finished or not (then poll it)."""
    return client().statement_execution.execute_statement(
        statement=statement,
        warehouse_id=cfg.warehouse_id,
        catalog=cfg.catalog,
        schema=cfg.schema,
        parameters=[StatementParameterListItem(name=k, value=v) for k, v in (params or {}).items()],
        wait_timeout="10s",
        on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CONTINUE,
    )


def get(statement_id: str) -> StatementResponse:
    return client().statement_execution.get_statement(statement_id)


def cancel(statement_id: str) -> None:
    client().statement_execution.cancel_execution(statement_id)


def rows(resp: StatementResponse) -> list[dict[str, str | None]]:
    """The result as dicts of column name -> value (the API returns every value as a string)."""
    if resp.manifest is None or resp.manifest.schema is None:
        return []
    cols = [c.name for c in resp.manifest.schema.columns or []]
    return [dict(zip(cols, row)) for row in (resp.result.data_array if resp.result else None) or []]


def error(resp: StatementResponse) -> str:
    err = resp.status.error if resp.status else None
    return (err.message if err and err.message else str(resp.status.state if resp.status else "unknown"))


# -- the tables ----------------------------------------------------------------------------

AP_INVOICES_DDL = """
CREATE OR REPLACE TABLE {table} (
  vendor_id STRING, vendor_name STRING, invoice_id STRING, invoice_date DATE, due_date DATE,
  paid_date DATE, amount_usd DECIMAL(12, 2), disputed BOOLEAN
) COMMENT 'Accounts-payable invoice history (synthetic, for the Box Relay demo)'"""

REVIEW_FINDINGS_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
  memo_path STRING, finding_no INT, run_id STRING, case_id STRING, workflow_id STRING,
  vendor_id STRING, supplier STRING, risk_score INT, rating STRING, recommendation STRING,
  clause STRING, contract_says STRING, playbook_rule STRING, severity STRING, issue STRING,
  memo_url STRING, published_path STRING, recorded_at TIMESTAMP
) COMMENT 'Contract review findings approved through Box Relay; the memo itself stays in Box'"""

SPEND_PROFILE_SQL = """
SELECT count(*)                               AS invoices,
       coalesce(sum(amount_usd), 0)           AS spend_12m,
       avg(datediff(paid_date, invoice_date)) AS avg_days_to_pay,
       count_if(paid_date > due_date)         AS late_payments,
       count_if(disputed)                     AS disputed
FROM {table}
WHERE vendor_id = :vendor_id AND invoice_date >= add_months(current_date(), -12)"""

# One JSON parameter carries every finding; keyed by memo and position, so a retry (or a second
# run of the same approval) updates the same rows instead of adding duplicates.
RECORD_FINDINGS_SQL = """
MERGE INTO {table} AS t
USING (
  SELECT :memo_path AS memo_path, CAST(pos + 1 AS INT) AS finding_no, :run_id AS run_id,
         :case_id AS case_id, :workflow_id AS workflow_id, :vendor_id AS vendor_id,
         :supplier AS supplier, CAST(NULLIF(:risk_score, '') AS INT) AS risk_score, :rating AS rating,
         :recommendation AS recommendation, f.clause, f.contract_says, f.playbook_rule,
         f.severity, f.issue, :memo_url AS memo_url, :published_path AS published_path,
         current_timestamp() AS recorded_at
  FROM (SELECT posexplode(from_json(:findings,
          'array<struct<clause:string,contract_says:string,playbook_rule:string,severity:string,issue:string>>'))
        AS (pos, f))
) AS s
ON t.memo_path = s.memo_path AND t.finding_no = s.finding_no
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *"""
