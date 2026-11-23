"""Reading the playbook's rules in code (the same Markdown table the Reviewer reads).

Legal edits Playbook/playbook.md in Box, so nothing here hardcodes a rule: the payment-terms
standard is whatever the table's "Payment terms" row says in the version being used.
"""

import re

PLAYBOOK_PATH = "Playbook/playbook.md"  # in the Reviewer's folder


def standard(text: str, topic: str) -> str | None:
    """The Standard column of the row whose Topic is `topic`, e.g. 'Net 60' for 'Payment terms'."""
    for line in text.splitlines():
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) >= 2 and cells[0].lower() == topic.lower():
            return cells[1]
    return None


def payment_days(text: str) -> int | None:
    """The playbook's standard payment terms in days ('Net 60' -> 60), or None if it doesn't say."""
    match = re.search(r"\d+", standard(text, "Payment terms") or "")
    return int(match.group()) if match else None
