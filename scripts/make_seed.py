"""Generate the supplier contract (MSA PDF) and the playbook into seed/.

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
    msa(OUT / "Incoming" / "Halvorsen_Master_Supply_Agreement.pdf")
    (OUT / "Playbook" / "playbook.md").write_text(fx.PLAYBOOK_MD)
    for p in sorted(OUT.rglob("*.*")):
        print(p.relative_to(ROOT), p.stat().st_size)
