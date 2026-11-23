"""Set up and inspect Box Relay's Databricks tables, from your Mac (reads .env.databricks).

    uv run python scripts/databricks_admin.py check           # can we reach the warehouse?
    uv run python scripts/databricks_admin.py seed            # create the schema and both tables
    uv run python scripts/databricks_admin.py findings        # the latest recorded findings
    uv run python scripts/databricks_admin.py stop-warehouse  # so the next run shows a cold start

`seed` replaces ap_invoices with synthetic history (two years, so the trailing 12 months stay full)
and creates review_findings if it doesn't exist; recorded findings are kept.
"""

import os
import random
import sys
import time
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def load_env() -> None:
    env = ROOT / ".env.databricks"
    if not env.is_file():
        sys.exit("error: no .env.databricks (copy .env.databricks.example and fill it in)")
    for line in env.read_text().splitlines():
        key, sep, value = line.partition("=")
        if sep and key.strip() and not key.lstrip().startswith("#") and value.strip():
            os.environ.setdefault(key.strip(), value.strip())


load_env()
from demo import databricks_sql as dbsql  # noqa: E402  (after the env is loaded)

CFG = dbsql.settings()
if CFG is None:
    sys.exit("error: .env.databricks needs DATABRICKS_HOST, DATABRICKS_WAREHOUSE_ID and a token or client secret")

# Halvorsen (V-10442) is the demo's supplier, on Net 45 in the ERP; the others make the table real.
VENDORS = [
    ("V-10442", "Halvorsen Industrial Supply Co.", 45, 200_000, 4),
    ("V-10118", "Brightline Packaging LLC", 30, 60_000, 3),
    ("V-10231", "Corvane Logistics Inc.", 30, 140_000, 6),
    ("V-10377", "Ostrander Electrical Components", 60, 90_000, 2),
    ("V-10509", "Tidewell Safety Products", 30, 25_000, 2),
]


def run(statement: str, params: dict[str, str] | None = None) -> list[dict]:
    resp = dbsql.submit(CFG, statement, params)
    started = time.time()
    while resp.status.state not in dbsql.DONE:
        print(f"  {resp.status.state.value} ({time.time() - started:.0f} s)", flush=True)
        time.sleep(2)
        resp = dbsql.get(resp.statement_id)
    if resp.status.state != dbsql.StatementState.SUCCEEDED:
        sys.exit(f"error: {dbsql.error(resp)}")
    return dbsql.rows(resp)


def invoices() -> list[str]:
    rng = random.Random(10442)
    today = date.today()
    rows = []
    for vendor_id, name, terms, monthly, per_month in VENDORS:
        for month in range(24):
            for i in range(per_month):
                inv_date = today - timedelta(days=30 * month + rng.randint(0, 29))
                amount = round(monthly / per_month * rng.uniform(0.7, 1.3), 2)
                paid = inv_date + timedelta(days=terms + rng.randint(-6, 5))
                paid_sql = "NULL" if paid > today else f"DATE'{paid}'"
                disputed = "true" if rng.random() < 0.03 else "false"
                rows.append(f"('{vendor_id}', '{name}', 'INV-{vendor_id[2:]}-{month:02d}{i}', DATE'{inv_date}', "
                            f"DATE'{inv_date + timedelta(days=terms)}', {paid_sql}, {amount}, {disputed})")
    return rows


def seed() -> None:
    print(f"schema {CFG.catalog}.{CFG.schema}")
    # Create the schema only if it's missing: an admin may have created it and granted just
    # USE SCHEMA, CREATE TABLE, SELECT and MODIFY on it (no CREATE SCHEMA on the catalog).
    if not run(f"SHOW SCHEMAS IN {CFG.catalog} LIKE '{CFG.schema}'"):
        run(f"CREATE SCHEMA {CFG.catalog}.{CFG.schema}")
    ap = CFG.table("ap_invoices")
    run(dbsql.AP_INVOICES_DDL.format(table=ap))
    rows = invoices()
    run(f"INSERT INTO {ap} VALUES " + ",\n".join(rows))
    print(f"{ap}: {len(rows)} invoices")
    run(dbsql.REVIEW_FINDINGS_DDL.format(table=CFG.table("review_findings")))
    print(f"{CFG.table('review_findings')}: ready")
    [profile] = run(dbsql.SPEND_PROFILE_SQL.format(table=ap), {"vendor_id": "V-10442"})
    print(f"Halvorsen, last 12 months: {profile}")


def check() -> None:
    print(run("SELECT current_user() AS user, current_catalog() AS catalog, current_schema() AS schema")[0])


def findings() -> None:
    for row in run(f"SELECT recorded_at, run_id, finding_no, clause, severity, issue "
                   f"FROM {CFG.table('review_findings')} ORDER BY recorded_at DESC, finding_no LIMIT 20"):
        print(f"{row['recorded_at'][:19]}  {row['run_id']}  #{row['finding_no']:>2}  {row['severity']:<6}  "
              f"{row['clause']}: {(row['issue'] or '')[:80]}")


def stop_warehouse() -> None:
    dbsql.client().warehouses.stop(CFG.warehouse_id)
    print(f"stopping warehouse {CFG.warehouse_id}; the next run's spend profile waits for it to start")


if __name__ == "__main__":
    commands = {"check": check, "seed": seed, "findings": findings, "stop-warehouse": stop_warehouse}
    if len(sys.argv) != 2 or sys.argv[1] not in commands:
        sys.exit(__doc__)
    commands[sys.argv[1]]()
