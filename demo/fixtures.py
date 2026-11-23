"""The demo's ground truth: the supplier contract the seed PDF is generated from, the playbook,
and the records the simulated enterprise systems answer with. Fictional companies throughout.

The MSA breaks several playbook rules (payment terms, renewal, liability, indemnity, ...), and its
Net 15 payment terms differ from the Net 45 the ERP has on file for the supplier (the terms the
company pays this supplier on today, not a policy: the policy is the playbook).
"""

CUSTOMER = "Pinecrest Manufacturing, Inc."

VENDOR = {
    "name": "Halvorsen Industrial Supply Co.",
    "tax_id": "84-3920175",
    "country": "US",
    "address": "2200 Foundry Row, Reno, NV 89502",
}

# What the ERP knows about the supplier.
VENDOR_MASTER = {
    "84-3920175": {
        "vendor_id": "V-10442",
        "name": "Halvorsen Industrial Supply Co.",
        "status": "active",
        "onboarded": "2024-03-18",
        "payment_terms": "Net 45",
    }
}

MSA_TITLE = "Master Supply Agreement"
MSA_CLAUSES = [
    ("1. Term and Renewal",
     "This Agreement has an initial term of three (3) years and renews automatically for successive "
     "three (3) year terms unless either party gives written notice of non-renewal at least one "
     "hundred twenty (120) days before the end of the then-current term."),
    ("2. Payment Terms",
     "Customer shall pay each invoice within fifteen (15) days of the invoice date (Net 15). Late "
     "amounts accrue interest at 1.5% per month."),
    ("3. Price Adjustments",
     "Supplier may increase prices by up to twelve percent (12%) once per contract year on thirty "
     "(30) days' written notice."),
    ("4. Limitation of Liability",
     "Supplier's aggregate liability under this Agreement shall not exceed the fees paid by Customer "
     "in the three (3) months preceding the claim. Customer's liability is not limited."),
    ("5. Indemnification",
     "Customer shall defend, indemnify and hold harmless Supplier from any and all claims arising out "
     "of or relating to the products or services supplied under this Agreement."),
    ("6. Termination",
     "Either party may terminate for material breach uncured after sixty (60) days' notice. Customer "
     "may not terminate this Agreement for convenience."),
    ("7. Insurance",
     "Supplier shall maintain commercial general liability insurance of not less than one million "
     "dollars ($1,000,000) per occurrence."),
    ("8. Governing Law",
     "This Agreement is governed by the laws of the State of Nevada."),
]

# Approved fallback language, served by the simulated clause library.
CLAUSE_LIBRARY = {
    "renewal": "Renews automatically for successive one (1) year terms unless either party gives sixty (60) days' notice of non-renewal.",
    # Payment terms follow the playbook's standard (see activities.get_approved_clause_language).
    "payment_terms": "Customer shall pay undisputed invoices within {days} days of receipt (Net {days}).",
    "price_adjustment": "Prices are fixed for the first contract year; thereafter increases may not exceed the lesser of CPI-U or 5% per year, on sixty (60) days' notice.",
    "limitation_of_liability": "Each party's aggregate liability shall not exceed the fees paid or payable in the twelve (12) months preceding the claim, excluding indemnity and confidentiality obligations.",
    "indemnification": "Each party shall defend and indemnify the other against third-party claims to the extent caused by its negligence, wilful misconduct or breach.",
    "termination": "Customer may terminate for convenience on ninety (90) days' written notice.",
    "insurance": "Supplier shall maintain commercial general liability insurance of not less than two million dollars ($2,000,000) per occurrence.",
    "governing_law": "This Agreement is governed by the laws of the State of Delaware.",
}

PLAYBOOK_MD = """\
# Procurement contract playbook

Owned by Legal. Agents read this file from Box; edit it in Box to change
the rules.

## Contract terms

| Topic | Standard | Escalate when |
|---|---|---|
| Payment terms | Net 45 | Shorter than Net 30 |
| Auto-renewal | 1-year terms, notice of 60 days or less | Renewal term over 1 year, or notice over 60 days |
| Price increases | Capped at CPI or 5% a year, 60 days' notice | Any uncapped or >5% increase |
| Limitation of liability | Mutual, at least 12 months of fees | One-way or uncapped for Customer |
| Indemnification | Mutual, fault-based | One-way in Supplier's favour |
| Termination | Customer may terminate for convenience on 90 days' notice | No convenience termination |
| Insurance | General liability of at least $2M per occurrence | Below $2M |
| Governing law | Delaware or New York | Any other state: Legal review, low severity |
"""
