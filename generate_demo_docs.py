"""
generate_demo_docs.py
======================
Generates two small batches of banking PDFs specifically for pipeline demos.
Keeps them SEPARATE from the 1,000-doc knowledge base.

Tier 2 — Trial docs (raw-docs/trial/):   10 docs for rehearsals
Tier 3 — Real demo docs (raw-docs/demo/): 5 docs for the actual demo

Each "real demo" doc has a memorable scenario with a clear before/after:
  - Before pipeline: chat says "I don't have data on this"
  - After pipeline:  chat returns the exact case details

Usage:
    source .venv/bin/activate
    python generate_demo_docs.py

Uploads to s3://banking-docs-poc-qahftr/ under raw-docs/trial/ and raw-docs/demo/
Prints the exact curl commands to use for each demo.
"""

import io, json, os, boto3
from datetime import date, timedelta
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT

from dotenv import load_dotenv
load_dotenv(".env")

BUCKET   = "banking-docs-poc-qahftr"
REGION   = "us-east-1"
s3       = boto3.client("s3", region_name=REGION)

# ── Shared styling ────────────────────────────────────────────────────────────
def _styles():
    s = getSampleStyleSheet()
    s.add(ParagraphStyle("BankTitle", parent=s["Normal"],
        fontSize=14, fontName="Helvetica-Bold", alignment=TA_CENTER, spaceAfter=4))
    s.add(ParagraphStyle("SectionHdr", parent=s["Normal"],
        fontSize=9, fontName="Helvetica-Bold", spaceBefore=10, spaceAfter=4,
        textColor=colors.HexColor("#003366")))
    s.add(ParagraphStyle("Body", parent=s["Normal"], fontSize=8.5, spaceAfter=6))
    s.add(ParagraphStyle("Small", parent=s["Normal"], fontSize=7.5,
        textColor=colors.HexColor("#555555")))
    return s

def _table(data, col_widths=None):
    t = Table(data, colWidths=col_widths or [3.5*cm, 6*cm, 3.5*cm, 5*cm])
    t.setStyle(TableStyle([
        ("FONTNAME",  (0,0),(0,-1), "Helvetica-Bold"),
        ("FONTNAME",  (2,0),(2,-1), "Helvetica-Bold"),
        ("FONTSIZE",  (0,0),(-1,-1), 8),
        ("GRID",      (0,0),(-1,-1), 0.25, colors.HexColor("#dddddd")),
        ("ROWBACKGROUNDS", (0,0),(-1,-1), [colors.HexColor("#f0f4f9"), colors.white]),
        ("PADDING",   (0,0),(-1,-1), 5),
    ]))
    return t

def _header(story, styles, doc_type, doc_ref):
    story.append(Paragraph("SecureBank PLC", styles["BankTitle"]))
    story.append(Paragraph(f"{doc_type} — {doc_ref}", styles["Small"]))
    story.append(Spacer(1, 8))
    story.append(Paragraph("─" * 90, styles["Small"]))
    story.append(Spacer(1, 6))


# ══════════════════════════════════════════════════════════════════════════════
# REAL DEMO DOCS — 5 hand-crafted, memorable scenarios
# ══════════════════════════════════════════════════════════════════════════════

def make_demo_complaint_highvalue():
    """
    Demo doc 1: High-value product mis-selling complaint, $12,000 compensation
    Narration: 'Customer was sold a structured product that didn't match their risk profile'
    Query to use after ingestion: 'Show me product mis-selling complaints with compensation over $10,000'
    """
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, topMargin=1.5*cm, bottomMargin=1.5*cm,
                            leftMargin=2*cm, rightMargin=2*cm)
    styles = _styles()
    story  = []
    _header(story, styles, "COMPLAINT CASE", "CMP-DEMO-001")

    story.append(Paragraph("FORMAL COMPLAINT — PRODUCT MIS-SELLING", styles["SectionHdr"]))
    story.append(_table([
        ["Case Reference",  "CMP-DEMO-001",        "Status",        "Closed — Resolved Customer Favour"],
        ["Complaint Type",  "Product Mis-selling",  "Priority",      "Critical"],
        ["Date Filed",      "03 Jan 2026",           "Date Closed",   "28 Jan 2026"],
        ["Assigned Officer","Priya Sharma",          "Branch",        "Sydney CBD"],
        ["Compensation",    "$12,000.00",            "Regulatory",    "ASIC Case Ref ASIC-2026-00143"],
    ]))
    story.append(Spacer(1, 10))

    story.append(Paragraph("Customer Details", styles["SectionHdr"]))
    story.append(_table([
        ["Customer Name",   "DEMO — Robert Ashworth",  "Customer ID",  "CUST-DEMO-001"],
        ["Account Number",  "40271893",                 "BSB Number",    "062-000"],
        ["Account Type",    "Premier",                  "Branch",       "Sydney CBD BR001"],
    ]))
    story.append(Spacer(1, 10))

    story.append(Paragraph("Complaint Summary", styles["SectionHdr"]))
    story.append(Paragraph(
        "Customer Robert Ashworth (CUST-DEMO-001) filed a formal complaint on 03 January 2026 "
        "regarding the mis-selling of a 5-year structured capital-at-risk product in March 2024. "
        "The customer, classified as a cautious investor with a risk score of 2/10, was placed "
        "into a product requiring a minimum risk score of 6/10. The customer sustained a loss of "
        "$8,400 before the error was identified during an annual review. "
        "This complaint was classified as CRITICAL priority and escalated immediately to the "
        "Compliance & Regulatory team. ASIC was notified under RG 165.",
        styles["Body"]))
    story.append(Spacer(1, 6))

    story.append(Paragraph("Resolution", styles["SectionHdr"]))
    story.append(Paragraph(
        "SecureBank agreed to compensate the customer in full for the capital loss ($8,400) plus "
        "an additional goodwill payment of $3,600 to account for lost interest and distress, "
        "totalling $12,000.00. The responsible relationship manager has been subject to a formal "
        "performance review. Process improvements to the risk profiling system were implemented "
        "on 15 February 2026.",
        styles["Body"]))
    doc.build(story)
    return buf.getvalue()


def make_demo_dispute_fraud():
    """
    Demo doc 2: Card-not-present fraud, $3,200 disputed across 4 transactions
    Narration: 'Customer's card details were cloned and used for overseas purchases'
    Query: 'Show me card fraud disputes from 2026 with amounts over $3,000'
    """
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, topMargin=1.5*cm, bottomMargin=1.5*cm,
                            leftMargin=2*cm, rightMargin=2*cm)
    styles = _styles()
    story  = []
    _header(story, styles, "DISPUTE CASE", "DSP-DEMO-001")

    story.append(Paragraph("DISPUTE — CARD NOT PRESENT FRAUD", styles["SectionHdr"]))
    story.append(_table([
        ["Case Reference",  "DSP-DEMO-001",            "Status",       "Closed-Won (Customer)"],
        ["Dispute Type",    "Card Not Present Fraud",   "Total Amount", "$3,240.00"],
        ["Date Filed",      "07 Feb 2026",              "Date Closed",  "21 Feb 2026"],
        ["Assigned Officer","Mohammed Al-Rashid",        "Branch",       "Melbourne Central"],
    ]))
    story.append(Spacer(1, 10))

    story.append(Paragraph("Customer Details", styles["SectionHdr"]))
    story.append(_table([
        ["Customer Name",   "DEMO — Sarah Okonkwo",  "Customer ID", "CUST-DEMO-002"],
        ["Account Number",  "61094422",               "BSB Number",   "033-000"],
        ["Card Type",       "Visa Debit",             "Card Last 4", "7823"],
    ]))
    story.append(Spacer(1, 10))

    story.append(Paragraph("Disputed Transactions", styles["SectionHdr"]))
    story.append(Table(
        [["Date", "Merchant", "Country", "Amount"],
         ["01 Feb 2026", "ONLINE-STORE-XX",   "Singapore",     "$890.00"],
         ["01 Feb 2026", "DIGITAL-GOODS-YY",  "Singapore",     "$1,200.00"],
         ["02 Feb 2026", "MARKETPLACE-ZZ",    "Singapore",     "$750.00"],
         ["03 Feb 2026", "TECH-STORE-AA",     "Hong Kong", "$400.00"]],
        colWidths=[3*cm, 6*cm, 4*cm, 3*cm],
        style=TableStyle([
            ("FONTNAME",   (0,0),(-1,0), "Helvetica-Bold"),
            ("FONTSIZE",   (0,0),(-1,-1), 8),
            ("GRID",       (0,0),(-1,-1), 0.25, colors.lightgrey),
            ("BACKGROUND", (0,0),(-1,0), colors.HexColor("#003366")),
            ("TEXTCOLOR",  (0,0),(-1,0), colors.white),
            ("PADDING",    (0,0),(-1,-1), 5),
        ])
    ))
    story.append(Spacer(1, 10))

    story.append(Paragraph("Findings & Resolution", styles["SectionHdr"]))
    story.append(Paragraph(
        "Investigation confirmed that all four transactions were fraudulent. The customer was in "
        "Melbourne at the time all transactions were processed. Card details were likely obtained "
        "via a phishing attack on a third-party e-commerce site. Full refund of $3,240.00 was "
        "credited to account 61094422 on 21 February 2026. A replacement card (last 4: 3341) "
        "was issued. Fraud referral submitted to the Australian Federal Police (AFP).",
        styles["Body"]))
    doc.build(story)
    return buf.getvalue()


def make_demo_estatement_premier():
    """
    Demo doc 3: Premier account 6-month statement, high-value transactions
    Narration: 'New customer eStatement for live query demo'
    Query: 'What is the closing balance for customer CUST-DEMO-003 in April 2026?'
    """
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, topMargin=1.5*cm, bottomMargin=1.5*cm,
                            leftMargin=2*cm, rightMargin=2*cm)
    styles = _styles()
    story  = []
    _header(story, styles, "eCOUNT STATEMENT", "STMT-DEMO-001")

    story.append(Paragraph("ACCOUNT STATEMENT — APRIL 2026", styles["SectionHdr"]))
    story.append(_table([
        ["Customer Name",   "DEMO — Amelia Singh",  "Customer ID",   "CUST-DEMO-003"],
        ["Account Number",  "78823341",              "BSB Number",     "082-000"],
        ["Account Type",    "Premier Current",       "Branch",        "Brisbane BR004"],
        ["Statement From",  "01 Apr 2026",           "Statement To",  "30 Apr 2026"],
        ["Opening Balance", "$24,150.00",            "Closing Balance","$19,732.50"],
    ]))
    story.append(Spacer(1, 10))

    story.append(Paragraph("Transactions", styles["SectionHdr"]))
    story.append(Table(
        [["Date",         "Description",               "Debit",      "Credit",    "Balance"],
         ["01 Apr 2026",  "Opening Balance",            "",           "",          "$24,150.00"],
         ["03 Apr 2026",  "Home Loan Repayment",      "$2,100.00",  "",          "$22,050.00"],
         ["05 Apr 2026",  "Pay Salary — Accenture",    "",           "$6,800.00", "$28,850.00"],
         ["08 Apr 2026",  "Transfer — Super Fund",       "$500.00",    "",          "$28,350.00"],
         ["12 Apr 2026",  "Woolworths Melbourne",        "$186.50",    "",          "$28,163.50"],
         ["15 Apr 2026",  "ATO Tax Payment",       "$8,200.00",  "",          "$19,963.50"],
         ["20 Apr 2026",  "Premier Cashback Credit",    "",           "$89.00",    "$20,052.50"],
         ["25 Apr 2026",  "Telstra DD",            "$65.00",     "",          "$19,987.50"],
         ["28 Apr 2026",  "Kogan Purchase",               "$255.00",    "",          "$19,732.50"],
         ["30 Apr 2026",  "Closing Balance",            "",           "",          "$19,732.50"]],
        colWidths=[3*cm, 6.5*cm, 2.5*cm, 2.5*cm, 3*cm],
        style=TableStyle([
            ("FONTNAME",   (0,0),(-1,0), "Helvetica-Bold"),
            ("FONTNAME",   (0,-1),(-1,-1), "Helvetica-Bold"),
            ("FONTSIZE",   (0,0),(-1,-1), 7.5),
            ("GRID",       (0,0),(-1,-1), 0.25, colors.lightgrey),
            ("BACKGROUND", (0,0),(-1,0), colors.HexColor("#003366")),
            ("TEXTCOLOR",  (0,0),(-1,0), colors.white),
            ("ALIGN",      (2,0),(4,-1), "RIGHT"),
            ("PADDING",    (0,0),(-1,-1), 4),
        ])
    ))
    doc.build(story)
    return buf.getvalue()


def make_demo_maintenance_overdraft():
    """
    Demo doc 4: Overdraft limit increase for business account — $25k → $50k
    Narration: 'Business customer requesting increased overdraft for seasonal cash flow'
    Query: 'Which customers requested overdraft limit changes in 2026?'
    """
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, topMargin=1.5*cm, bottomMargin=1.5*cm,
                            leftMargin=2*cm, rightMargin=2*cm)
    styles = _styles()
    story  = []
    _header(story, styles, "ACCOUNT MAINTENANCE", "MNT-DEMO-001")

    story.append(Paragraph("OVERDRAFT LIMIT CHANGE REQUEST", styles["SectionHdr"]))
    story.append(_table([
        ["Request Reference", "MNT-DEMO-001",          "Status",    "Approved"],
        ["Request Type",      "Overdraft Limit Change", "Priority",  "Standard"],
        ["Date Submitted",    "10 Mar 2026",            "Approved",  "14 Mar 2026"],
        ["Authorised By",     "Thomas Müller — RM008",  "Branch",    "Perth BR003"],
    ]))
    story.append(Spacer(1, 10))

    story.append(Paragraph("Customer & Account Details", styles["SectionHdr"]))
    story.append(_table([
        ["Customer Name",   "DEMO — Blackwood & Foster Ltd",  "Customer ID",  "CUST-DEMO-004"],
        ["Account Number",  "92881150",                        "BSB Number",    "066-000"],
        ["Account Type",    "Business Current",                "Branch",       "Perth BR003"],
        ["Current Limit",   "$25,000.00",                     "Requested",    "$50,000.00"],
    ]))
    story.append(Spacer(1, 10))

    story.append(Paragraph("Business Justification", styles["SectionHdr"]))
    story.append(Paragraph(
        "Blackwood & Foster Ltd (CUST-DEMO-004) is a manufacturing company experiencing "
        "seasonal cash-flow variance of approximately $30,000 during Q1 and Q3. "
        "The current $25,000 facility has been fully utilised for 4 of the past 12 months. "
        "The company's turnover increased 34% year-on-year to $2.1M. "
        "Credit Risk assessment score: 72/100 (Low Risk). Application approved subject to "
        "annual review. New limit of $50,000 effective 15 March 2026.",
        styles["Body"]))
    doc.build(story)
    return buf.getvalue()


def make_demo_complaint_ombudsman():
    """
    Demo doc 5: Complaint escalated to AFCA — the 'escalation path' demo doc
    Narration: 'This one shows what happens when a complaint goes all the way to the Ombudsman'
    Query: 'Which complaints have been referred to the Ombudsman in 2026?'
    """
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, topMargin=1.5*cm, bottomMargin=1.5*cm,
                            leftMargin=2*cm, rightMargin=2*cm)
    styles = _styles()
    story  = []
    _header(story, styles, "COMPLAINT CASE", "CMP-DEMO-002")

    story.append(Paragraph("FORMAL COMPLAINT — REFERRED TO FINANCIAL OMBUDSMAN SERVICE", styles["SectionHdr"]))
    story.append(_table([
        ["Case Reference",  "CMP-DEMO-002",          "Status",    "Referred to Ombudsman"],
        ["Complaint Type",  "Mortgage Related",       "Priority",  "High"],
        ["Date Filed",      "15 Nov 2025",            "AFCA Ref",   "AFCA-2026-MB-008821"],
        ["Assigned Officer","James Hargreaves",        "Branch",    "Adelaide BR005"],
    ]))
    story.append(Spacer(1, 10))

    story.append(Paragraph("Customer Details", styles["SectionHdr"]))
    story.append(_table([
        ["Customer Name",   "DEMO — Patrick Devereux",  "Customer ID", "CUST-DEMO-005"],
        ["Account Number",  "33410087",                  "BSB Number",   "012-000"],
        ["Product",         "Residential Mortgage",      "Mortgage Ref","MTG-2022-007731"],
    ]))
    story.append(Spacer(1, 10))

    story.append(Paragraph("Complaint Summary", styles["SectionHdr"]))
    story.append(Paragraph(
        "Customer Patrick Devereux (CUST-DEMO-005) complained that SecureBank incorrectly applied "
        "an Early Repayment Charge (ERC) of $4,200 when the customer ported their mortgage to a "
        "new property in October 2025. The customer argues the porting was within the terms of "
        "the original mortgage offer. SecureBank's initial assessment upheld the charge. "
        "The customer rejected SecureBank's Final Response Letter dated 20 December 2025 "
        "and referred the case to the Australian Financial Complaints Authority on 15 January 2026.",
        styles["Body"]))
    story.append(Spacer(1, 6))

    story.append(Paragraph("Current Status", styles["SectionHdr"]))
    story.append(Paragraph(
        "AFCA reference AFCA-2026-MB-008821 assigned. SecureBank has 8 weeks from 15 January 2026 "
        "to provide its full case file to AFCA. Mortgage Operations team compiling documentation. "
        "Estimated AFCA decision: June 2026. ERC charge remains in dispute — not refunded pending "
        "AFCA adjudication.",
        styles["Body"]))
    doc.build(story)
    return buf.getvalue()


# ══════════════════════════════════════════════════════════════════════════════
# TRIAL DOCS — 10 simpler variants for rehearsals
# ══════════════════════════════════════════════════════════════════════════════

def make_trial_doc(n):
    """Simple complaint doc for trial ingestion practice."""
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, topMargin=1.5*cm, bottomMargin=1.5*cm,
                            leftMargin=2*cm, rightMargin=2*cm)
    styles = _styles()
    story  = []
    cust_id = f"CUST-TRIAL-{n:03d}"
    ref     = f"CMP-TRIAL-{n:03d}"
    _header(story, styles, "COMPLAINT CASE", ref)
    story.append(Paragraph(f"COMPLAINT — TRIAL DOCUMENT {n}", styles["SectionHdr"]))
    story.append(_table([
        ["Case Reference",  ref,                    "Status",   "Closed"],
        ["Complaint Type",  "Online Banking Issue", "Priority", "Low"],
        ["Customer ID",     cust_id,                "Branch",   "Glasgow BR008"],
        ["Filed",           "01 May 2026",          "Closed",   "10 May 2026"],
    ]))
    story.append(Spacer(1, 10))
    story.append(Paragraph("Complaint Details", styles["SectionHdr"]))
    story.append(Paragraph(
        f"Trial document {n} ({cust_id}). Customer reported intermittent login failures "
        f"to SecureBank Online Banking between 28 April and 30 April 2026. "
        f"Issue was traced to a scheduled maintenance window that overran by 2 hours. "
        f"Customer was issued a goodwill credit of $25.00. Case closed.",
        styles["Body"]))
    doc.build(story)
    return buf.getvalue()


# ══════════════════════════════════════════════════════════════════════════════
# Upload and report
# ══════════════════════════════════════════════════════════════════════════════

REAL_DEMO_DOCS = [
    ("raw-docs/demo/CMP-DEMO-001.pdf", make_demo_complaint_highvalue,  "Complaint",          "CUST-DEMO-001",
     "Product mis-selling $12k compensation",
     "'Show me product mis-selling complaints with compensation over $10,000'"),
    ("raw-docs/demo/DSP-DEMO-001.pdf", make_demo_dispute_fraud,        "Dispute",            "CUST-DEMO-002",
     "Card fraud $3,240 across 4 Singaporen transactions",
     "'Show me card fraud disputes from 2026 with amounts over $3,000'"),
    ("raw-docs/demo/STMT-DEMO-001.pdf",make_demo_estatement_premier,   "eStatement",         "CUST-DEMO-003",
     "Premier account statement April 2026",
     "'What is the closing balance for customer CUST-DEMO-003 in April 2026?'"),
    ("raw-docs/demo/MNT-DEMO-001.pdf", make_demo_maintenance_overdraft,"AccountMaintenance", "CUST-DEMO-004",
     "Overdraft limit $25k → $50k for Blackwood & Foster Ltd",
     "'Which customers requested overdraft limit changes in 2026?'"),
    ("raw-docs/demo/CMP-DEMO-002.pdf", make_demo_complaint_ombudsman,  "Complaint",          "CUST-DEMO-005",
     "Mortgage ERC complaint referred to AFCA",
     "'Which complaints have been referred to the Ombudsman in 2026?'"),
]

def main():
    print("=" * 65)
    print("  AskMyBank — Demo Document Generator")
    print("=" * 65)
    print()

    # ── Real demo docs ──────────────────────────────────────────────
    print("📋  Generating REAL DEMO docs (raw-docs/demo/) …")
    demo_curls = []
    for s3_key, fn, doc_type, cust_id, desc, query in REAL_DEMO_DOCS:
        pdf = fn()
        s3.put_object(Bucket=BUCKET, Key=s3_key, Body=pdf, ContentType="application/pdf")
        print(f"  ✅  {s3_key}  ({len(pdf):,} bytes)  — {desc}")
        demo_curls.append((s3_key, doc_type, cust_id, desc, query))

    # ── Trial docs ──────────────────────────────────────────────────
    print()
    print("🧪  Generating TRIAL docs (raw-docs/trial/) …")
    trial_curls = []
    for n in range(1, 11):
        s3_key = f"raw-docs/trial/CMP-TRIAL-{n:03d}.pdf"
        pdf    = make_trial_doc(n)
        s3.put_object(Bucket=BUCKET, Key=s3_key, Body=pdf, ContentType="application/pdf")
        print(f"  ✅  {s3_key}  ({len(pdf):,} bytes)")
        trial_curls.append((s3_key, "Complaint", f"CUST-TRIAL-{n:03d}"))

    # ── Print curl commands ─────────────────────────────────────────
    API = "https://r6v15i892m.execute-api.us-east-1.amazonaws.com"

    print()
    print("=" * 65)
    print("  REAL DEMO — curl commands (use one per live demo)")
    print("  Query chat BEFORE to show doc not in knowledge base,")
    print("  then trigger pipeline, then query AGAIN for the wow moment.")
    print("=" * 65)
    for s3_key, doc_type, cust_id, desc, query in demo_curls:
        print(f"\n# {desc}")
        print(f"# Chat query to use after: {query}")
        print(f"""curl -s -X POST {API}/pipeline/ingest \\
  -H 'Content-Type: application/json' \\
  -d '{{
    "s3_key":        "{s3_key}",
    "doc_type":      "{doc_type}",
    "customer_id":   "{cust_id}",
    "source_system": "manual"
  }}' | python3 -m json.tool""")

    print()
    print("=" * 65)
    print("  TRIAL — curl commands (rehearsal — use freely)")
    print("=" * 65)
    for s3_key, doc_type, cust_id in trial_curls:
        print(f"""
curl -s -X POST {API}/pipeline/ingest \\
  -H 'Content-Type: application/json' \\
  -d '{{"s3_key":"{s3_key}","doc_type":"{doc_type}","customer_id":"{cust_id}"}}'""")

    print()
    print("=" * 65)
    print("  S3 layout after this script:")
    print(f"  s3://{BUCKET}/")
    print("    ├── complaints/          ← 200 knowledge-base docs (already ingested)")
    print("    ├── disputes/            ← 250 knowledge-base docs (already ingested)")
    print("    ├── estatements/         ← 400 knowledge-base docs (already ingested)")
    print("    ├── maintenance/         ← 150 knowledge-base docs (already ingested)")
    print("    └── raw-docs/")
    print("          ├── demo/          ← 5 REAL demo docs (protect until showtime)")
    print("          └── trial/         ← 10 trial docs    (rehearse freely)")
    print("=" * 65)


if __name__ == "__main__":
    main()
