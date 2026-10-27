"""Generate the vendor packet (invoice + MSA PDFs) and the playbook into seed/.

    uv run --group seed python scripts/make_seed.py
"""

import sys
from pathlib import Path

from reportlab.lib.pagesizes import LETTER
from reportlab.lib.units import inch
from reportlab.pdfgen.canvas import Canvas

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from demo import fixtures as fx  # noqa: E402

OUT = ROOT / "seed"


def invoice(path: Path) -> None:
    inv, v = fx.INVOICE, fx.VENDOR
    c = Canvas(str(path), pagesize=LETTER, invariant=1)  # reproducible bytes: re-seeding leaves Box alone
    w, h = LETTER
    y = h - inch
    c.setFont("Helvetica-Bold", 18); c.drawString(inch, y, v["name"]); y -= 18
    c.setFont("Helvetica", 10); c.drawString(inch, y, v["address"]); y -= 30
    c.setFont("Helvetica-Bold", 14); c.drawString(inch, y, "INVOICE"); y -= 22
    c.setFont("Helvetica", 10)
    for label, value in [
        ("Invoice Number", inv["number"]), ("Invoice Date", inv["date"]), ("PO Number", inv["po_number"]),
        ("Vendor", v["name"]), ("Tax ID", v["tax_id"]), ("Bill To", fx.CUSTOMER), ("Terms", inv["terms"]),
    ]:
        c.drawString(inch, y, f"{label}: {value}"); y -= 14
    y -= 14
    c.setFont("Helvetica-Bold", 10)
    c.drawString(inch, y, "Description"); c.drawRightString(5.2 * inch, y, "Qty")
    c.drawRightString(6.3 * inch, y, "Unit price"); c.drawRightString(7.5 * inch, y, "Amount"); y -= 16
    c.setFont("Helvetica", 10)
    for desc, qty, price in inv["lines"]:
        c.drawString(inch, y, desc); c.drawRightString(5.2 * inch, y, f"{qty:,}")
        c.drawRightString(6.3 * inch, y, f"${price:,.2f}"); c.drawRightString(7.5 * inch, y, f"${qty * price:,.2f}")
        y -= 14
    y -= 10
    for label, value in [("Freight", inv["freight"]), ("Sales Tax", inv["sales_tax"]), ("Total Due", inv["total"])]:
        c.drawString(5.2 * inch, y, f"{label}: ${value:,.2f}"); y -= 14
    y -= 20
    c.setFont("Helvetica-Bold", 10); c.drawString(inch, y, "Remit payment to (UPDATED BANKING DETAILS)"); y -= 14
    c.setFont("Helvetica", 10)
    r = inv["remit_to"]
    for label, value in [("Bank", r["bank"]), ("Routing", r["routing"]), ("Account", f"XXXXXX{r['account_last4']}")]:
        c.drawString(inch, y, f"{label}: {value}"); y -= 14
    y -= 10
    c.setFont("Helvetica-Oblique", 9)
    c.drawString(inch, y, "Please note our new banking details effective immediately. Questions: ap@halvorsen-supply.example")
    c.save()


def msa(path: Path) -> None:
    c = Canvas(str(path), pagesize=LETTER, invariant=1)  # reproducible bytes: re-seeding leaves Box alone
    w, h = LETTER
    y = h - inch
    c.setFont("Helvetica-Bold", 16); c.drawCentredString(w / 2, y, fx.MSA_TITLE); y -= 22
    c.setFont("Helvetica", 10)
    intro = (f"This {fx.MSA_TITLE} is entered into as of October 1, 2026 between {fx.VENDOR['name']} "
             f"(\"Supplier\") and {fx.CUSTOMER} (\"Customer\").")
    for line in _wrap(intro, 95):
        c.drawString(inch, y, line); y -= 13
    y -= 8
    for heading, body in fx.MSA_CLAUSES:
        if y < 1.5 * inch:
            c.showPage(); y = h - inch
        c.setFont("Helvetica-Bold", 11); c.drawString(inch, y, heading); y -= 14
        c.setFont("Helvetica", 10)
        for line in _wrap(body, 95):
            c.drawString(inch, y, line); y -= 13
        y -= 8
    y -= 20
    c.drawString(inch, y, f"SUPPLIER: {fx.VENDOR['name']}"); c.drawString(4.3 * inch, y, f"CUSTOMER: {fx.CUSTOMER}")
    c.save()


def _wrap(text: str, width: int) -> list[str]:
    lines, line = [], ""
    for word in text.split():
        if len(line) + len(word) + 1 > width:
            lines.append(line); line = word
        else:
            line = f"{line} {word}".strip()
    return lines + [line]


if __name__ == "__main__":
    (OUT / "Incoming").mkdir(parents=True, exist_ok=True)
    (OUT / "Playbook").mkdir(parents=True, exist_ok=True)
    invoice(OUT / "Incoming" / f"Halvorsen_Invoice_{fx.INVOICE['number']}.pdf")
    msa(OUT / "Incoming" / "Halvorsen_Master_Supply_Agreement.pdf")
    (OUT / "Playbook" / "playbook.md").write_text(fx.PLAYBOOK_MD)
    for p in sorted(OUT.rglob("*.*")):
        print(p.relative_to(ROOT), p.stat().st_size)
