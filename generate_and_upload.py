"""
Banking POC - Sample Document Generator + S3 Uploader
Generates 1000 realistic banking PDFs and uploads to S3 with full metadata.

Usage:
    pip install boto3 faker reportlab
    aws configure
    python generate_and_upload.py

Output:
    - S3 bucket with 1000 PDFs organised by doc type
    - metadata.csv  (import to ClickHouse)
    - metadata.json (alternative format)
"""

import boto3
import csv
import json
import io
import random
import string
import sys
from datetime import date, timedelta
from pathlib import Path

from faker import Faker
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT

fake = Faker("en_US")
random.seed(42)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BUCKET_NAME   = f"banking-docs-poc-{''.join(random.choices(string.ascii_lowercase, k=6))}"
AWS_REGION    = "us-east-1"   # change if needed
DOC_COUNTS    = {
    "estatement":   400,
    "dispute":      250,
    "complaint":    200,
    "maintenance":  150,
}
OUTPUT_CSV    = "metadata.csv"
OUTPUT_JSON   = "metadata.json"

# ---------------------------------------------------------------------------
# Master Data
# ---------------------------------------------------------------------------
BRANCHES = [
    {"code": "BR001", "name": "London City",      "address": "1 Threadneedle St, London EC2R 8AH"},
    {"code": "BR002", "name": "Manchester Central","address": "100 Deansgate, Manchester M3 2GH"},
    {"code": "BR003", "name": "Birmingham",        "address": "5 Colmore Row, Birmingham B3 2BJ"},
    {"code": "BR004", "name": "Edinburgh",         "address": "12 George St, Edinburgh EH2 2PF"},
    {"code": "BR005", "name": "Leeds",             "address": "1 Park Row, Leeds LS1 5AB"},
    {"code": "BR006", "name": "Bristol",           "address": "15 Corn St, Bristol BS1 1HT"},
    {"code": "BR007", "name": "Cardiff",           "address": "3 St Mary St, Cardiff CF10 1AA"},
    {"code": "BR008", "name": "Glasgow",           "address": "100 Buchanan St, Glasgow G1 3BF"},
    {"code": "BR009", "name": "Liverpool",         "address": "1 Water St, Liverpool L2 0RG"},
    {"code": "BR010", "name": "Sheffield",         "address": "5 Pinstone St, Sheffield S1 2HN"},
]

RM_NAMES = [
    "James Hargreaves", "Sophie Chen", "Mohammed Al-Rashid", "Emily Watson",
    "David Okafor",     "Priya Sharma", "Thomas Müller",     "Rachel O'Brien",
    "Carlos Mendez",    "Aisha Patel",  "William Foster",    "Natalie Burke",
    "Liam Fitzgerald",  "Zoe Nakamura", "Patrick Devereux",  "Amelia Singh",
    "Robert Blackwood", "Fatima Hassan","George Whitfield",  "Isabella Rossi",
]

ACCOUNT_TYPES   = ["Current", "Savings", "Business Current", "Premier", "ISA"]
DISPUTE_TYPES   = [
    "Unauthorised Transaction", "Merchant Dispute", "ATM Withdrawal Error",
    "Duplicate Charge", "Card Not Present Fraud", "Direct Debit Dispute",
    "Wire Transfer Error", "Currency Conversion Dispute",
]
COMPLAINT_TYPES = [
    "Poor Customer Service", "Branch Service Complaint", "Online Banking Issue",
    "Mortgage Related", "Loan Processing Delay", "Fee Dispute",
    "Account Closure Complaint", "Staff Conduct", "Product Mis-selling",
]
MAINTENANCE_TYPES = [
    "Address Change", "Contact Number Update", "Signature Update",
    "Overdraft Limit Change", "Account Name Change", "Beneficiary Addition",
    "Standing Order Amendment", "Direct Debit Cancellation",
]
RESOLUTIONS = [
    "Resolved in customer favour", "Resolved in bank favour",
    "Partial refund issued", "Escalated to Ombudsman",
    "Withdrawn by customer", "No further action",
]
CASE_STATUS = ["Closed-Won", "Closed-Lost", "Withdrawn", "Referred to Ombudsman"]


def random_date(start_days_ago=730, end_days_ago=0):
    start = date.today() - timedelta(days=start_days_ago)
    end   = date.today() - timedelta(days=end_days_ago)
    return start + timedelta(days=random.randint(0, (end - start).days))


def make_customers(n=60):
    customers = []
    for i in range(n):
        branch = random.choice(BRANCHES)
        rm_name = random.choice(RM_NAMES)
        rm_id   = "RM" + str(RM_NAMES.index(rm_name) + 1).zfill(3)
        customers.append({
            "customer_id":   f"CUST{str(i+1).zfill(5)}",
            "customer_name":  fake.name(),
            "customer_email": fake.email(),
            "customer_phone": fake.phone_number(),
            "customer_dob":   fake.date_of_birth(minimum_age=21, maximum_age=80).isoformat(),
            "customer_address": fake.address().replace("\n", ", "),
            "account_number": "".join([str(random.randint(0,9)) for _ in range(8)]),
            "account_type":   random.choice(ACCOUNT_TYPES),
            "sort_code":      f"{random.randint(10,99)}-{random.randint(10,99)}-{random.randint(10,99)}",
            "branch_code":    branch["code"],
            "branch_name":    branch["name"],
            "branch_address": branch["address"],
            "rm_id":          rm_id,
            "rm_name":        rm_name,
            "rm_email":       rm_name.lower().replace(" ", ".") + "@securebankplc.com",
        })
    return customers


# ---------------------------------------------------------------------------
# PDF Generators
# ---------------------------------------------------------------------------
BANK_NAME   = "SecureBank PLC"
BANK_COLOUR = colors.HexColor("#003366")

def _base_doc(buffer, title):
    doc = SimpleDocTemplate(
        buffer,
        pagesize=A4,
        rightMargin=2*cm, leftMargin=2*cm, topMargin=2*cm, bottomMargin=2*cm,
        title=title,
    )
    return doc

def _styles():
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle("BankTitle",  fontName="Helvetica-Bold",   fontSize=16, textColor=BANK_COLOUR, spaceAfter=4))
    styles.add(ParagraphStyle("BankSub",    fontName="Helvetica",        fontSize=10, textColor=colors.grey))
    styles.add(ParagraphStyle("SectionHdr", fontName="Helvetica-Bold",   fontSize=11, textColor=BANK_COLOUR, spaceBefore=12, spaceAfter=4))
    styles.add(ParagraphStyle("Body",       fontName="Helvetica",        fontSize=9,  leading=14))
    styles.add(ParagraphStyle("BodyBold",   fontName="Helvetica-Bold",   fontSize=9))
    styles.add(ParagraphStyle("Small",      fontName="Helvetica",        fontSize=7,  textColor=colors.grey))
    return styles

def _header_footer(story, styles, doc_type, doc_id, cust):
    # Bank header
    story.append(Paragraph(BANK_NAME, styles["BankTitle"]))
    story.append(Paragraph(f"{cust['branch_address']}  |  {BANK_NAME.lower().replace(' ','')}.com", styles["BankSub"]))
    story.append(HRFlowable(width="100%", thickness=2, color=BANK_COLOUR, spaceAfter=8))

def _customer_table(styles, cust):
    data = [
        ["Customer Name",   cust["customer_name"],   "Customer ID",   cust["customer_id"]],
        ["Account Number",  cust["account_number"],  "Sort Code",     cust["sort_code"]],
        ["Account Type",    cust["account_type"],    "Branch",        cust["branch_name"]],
        ["Email",           cust["customer_email"],  "Phone",         cust["customer_phone"]],
        ["Address",         cust["customer_address"],"",              ""],
        ["Relationship Mgr",cust["rm_name"],         "RM Email",      cust["rm_email"]],
    ]
    t = Table(data, colWidths=[3.5*cm, 7*cm, 3.5*cm, 3.5*cm])
    t.setStyle(TableStyle([
        ("BACKGROUND",  (0,0),(-1,-1), colors.HexColor("#f5f8fc")),
        ("FONTNAME",    (0,0),(0,-1),  "Helvetica-Bold"),
        ("FONTNAME",    (2,0),(2,-1),  "Helvetica-Bold"),
        ("FONTSIZE",    (0,0),(-1,-1), 8),
        ("GRID",        (0,0),(-1,-1), 0.25, colors.lightgrey),
        ("ROWBACKGROUNDS",(0,0),(-1,-1),[colors.HexColor("#f5f8fc"), colors.white]),
        ("VALIGN",      (0,0),(-1,-1), "MIDDLE"),
        ("PADDING",     (0,0),(-1,-1), 4),
    ]))
    return t


def generate_estatement(cust, doc_id, stmt_date):
    buffer = io.BytesIO()
    doc    = _base_doc(buffer, f"eStatement {doc_id}")
    styles = _styles()
    story  = []

    period_start = stmt_date.replace(day=1)
    period_end   = (period_start + timedelta(days=32)).replace(day=1) - timedelta(days=1)
    opening_bal  = round(random.uniform(500, 50000), 2)

    _header_footer(story, styles, "eStatement", doc_id, cust)

    story.append(Paragraph("ACCOUNT STATEMENT", styles["SectionHdr"]))
    story.append(Paragraph(
        f"Statement Period: {period_start.strftime('%d %b %Y')} to {period_end.strftime('%d %b %Y')}  |  "
        f"Statement Ref: {doc_id}  |  Generated: {stmt_date.strftime('%d %b %Y')}",
        styles["Body"]
    ))
    story.append(Spacer(1, 8))
    story.append(_customer_table(styles, cust))
    story.append(Spacer(1, 12))

    # Balance summary
    story.append(Paragraph("Balance Summary", styles["SectionHdr"]))
    closing_bal = round(opening_bal + random.uniform(-2000, 5000), 2)
    total_cr    = round(random.uniform(1000, 8000), 2)
    total_dr    = round(abs(closing_bal - opening_bal - total_cr), 2)
    bal_data = [
        ["Opening Balance", f"${opening_bal:,.2f}"],
        ["Total Credits",   f"${total_cr:,.2f}"],
        ["Total Debits",    f"${total_dr:,.2f}"],
        ["Closing Balance", f"${closing_bal:,.2f}"],
    ]
    bt = Table(bal_data, colWidths=[8*cm, 4*cm])
    bt.setStyle(TableStyle([
        ("FONTNAME",    (0,0),(0,-1), "Helvetica-Bold"),
        ("FONTNAME",    (0,3),(1,3),  "Helvetica-Bold"),
        ("FONTSIZE",    (0,0),(-1,-1), 9),
        ("GRID",        (0,0),(-1,-1), 0.25, colors.lightgrey),
        ("ALIGN",       (1,0),(1,-1), "RIGHT"),
        ("BACKGROUND",  (0,3),(1,3),  colors.HexColor("#e8f0fe")),
        ("PADDING",     (0,0),(-1,-1), 5),
    ]))
    story.append(bt)
    story.append(Spacer(1, 12))

    # Transactions
    story.append(Paragraph("Transaction History", styles["SectionHdr"]))
    tx_data = [["Date", "Description", "Debit ($)", "Credit ($)", "Balance ($)"]]
    balance = opening_bal
    merchants = ["TESCO STORES", "AMAZON UK", "NETFLIX", "HMRC TAX", "BT GROUP",
                 "VIRGIN MEDIA", "SAINSBURY'S", "DIRECT LINE", "WATERSTONES", "COSTA COFFEE",
                 "SALARY - EMPLOYER", "BANK TRANSFER IN", "MORTGAGE PAYMENT", "COUNCIL TAX",
                 "WATER RATES", "GAS & ELECTRIC", "COUNCIL TAX DD", "NS&I SAVINGS"]
    cur_date = period_start
    while cur_date <= period_end:
        if random.random() < 0.6:
            is_credit = random.random() < 0.25
            amount    = round(random.uniform(5, 1500), 2)
            if is_credit:
                balance += amount
                tx_data.append([cur_date.strftime("%d/%m/%y"), random.choice(merchants), "", f"{amount:.2f}", f"{balance:,.2f}"])
            else:
                balance -= amount
                tx_data.append([cur_date.strftime("%d/%m/%y"), random.choice(merchants), f"{amount:.2f}", "", f"{balance:,.2f}"])
        cur_date += timedelta(days=1)

    tt = Table(tx_data, colWidths=[2*cm, 8*cm, 2.5*cm, 2.5*cm, 2.5*cm])
    tt.setStyle(TableStyle([
        ("BACKGROUND",  (0,0),(-1,0),  BANK_COLOUR),
        ("TEXTCOLOR",   (0,0),(-1,0),  colors.white),
        ("FONTNAME",    (0,0),(-1,0),  "Helvetica-Bold"),
        ("FONTSIZE",    (0,0),(-1,-1), 7.5),
        ("ROWBACKGROUNDS",(0,1),(-1,-1),[colors.white, colors.HexColor("#f9f9f9")]),
        ("GRID",        (0,0),(-1,-1), 0.2, colors.lightgrey),
        ("ALIGN",       (2,0),(4,-1),  "RIGHT"),
        ("PADDING",     (0,0),(-1,-1), 4),
    ]))
    story.append(tt)
    story.append(Spacer(1, 8))
    story.append(Paragraph(
        "This statement is auto-generated. Contact your Relationship Manager for queries.",
        styles["Small"]
    ))

    doc.build(story)
    buffer.seek(0)
    return buffer.read(), round(closing_bal, 2)


def generate_dispute(cust, doc_id, filed_date, status):
    buffer = io.BytesIO()
    doc    = _base_doc(buffer, f"Dispute {doc_id}")
    styles = _styles()
    story  = []

    dispute_type   = random.choice(DISPUTE_TYPES)
    amount         = round(random.uniform(20, 8500), 2)
    closed_date    = filed_date + timedelta(days=random.randint(5, 90))
    resolution     = random.choice(RESOLUTIONS)
    merchant       = fake.company()
    tx_date        = filed_date - timedelta(days=random.randint(1, 14))

    _header_footer(story, styles, "Dispute", doc_id, cust)

    story.append(Paragraph("DISPUTE CASE — CLOSED", styles["SectionHdr"]))
    story.append(_customer_table(styles, cust))
    story.append(Spacer(1, 10))

    story.append(Paragraph("Case Details", styles["SectionHdr"]))
    case_data = [
        ["Case Reference",   doc_id,            "Case Status",      status],
        ["Dispute Type",     dispute_type,      "Amount Disputed",  f"${amount:,.2f}"],
        ["Merchant / Payee", merchant,          "Transaction Date",  tx_date.strftime("%d %b %Y")],
        ["Date Filed",       filed_date.strftime("%d %b %Y"), "Date Closed", closed_date.strftime("%d %b %Y")],
        ["Handling Officer", cust["rm_name"],   "Branch",           cust["branch_name"]],
    ]
    ct = Table(case_data, colWidths=[3.5*cm, 6*cm, 3.5*cm, 4*cm])
    ct.setStyle(TableStyle([
        ("FONTNAME",    (0,0),(0,-1), "Helvetica-Bold"),
        ("FONTNAME",    (2,0),(2,-1), "Helvetica-Bold"),
        ("FONTSIZE",    (0,0),(-1,-1), 8.5),
        ("GRID",        (0,0),(-1,-1), 0.25, colors.lightgrey),
        ("ROWBACKGROUNDS",(0,0),(-1,-1),[colors.HexColor("#f5f8fc"), colors.white]),
        ("PADDING",     (0,0),(-1,-1), 5),
    ]))
    story.append(ct)
    story.append(Spacer(1, 10))

    story.append(Paragraph("Customer Statement", styles["SectionHdr"]))
    story.append(Paragraph(
        f"On {tx_date.strftime('%d %b %Y')}, the customer identified a {dispute_type.lower()} "
        f"of ${amount:,.2f} from {merchant} on their {cust['account_type']} account "
        f"(Sort Code: {cust['sort_code']}, Account: {cust['account_number']}). "
        f"The customer stated they did not authorise this transaction and requested an immediate investigation. "
        f"Supporting documentation was provided on {filed_date.strftime('%d %b %Y')}.",
        styles["Body"]
    ))
    story.append(Spacer(1, 8))

    story.append(Paragraph("Investigation Summary", styles["SectionHdr"]))
    story.append(Paragraph(
        f"Upon receipt of the dispute, {cust['rm_name']} at {cust['branch_name']} branch initiated a full "
        f"investigation in line with SecureBank PLC dispute resolution policy. Transaction logs, "
        f"merchant records and customer communication history were reviewed. "
        f"The investigation concluded on {closed_date.strftime('%d %b %Y')}.",
        styles["Body"]
    ))
    story.append(Spacer(1, 8))

    story.append(Paragraph("Resolution", styles["SectionHdr"]))
    bg = colors.HexColor("#e6f4ea") if "favour" in resolution.lower() and "customer" in resolution.lower() else colors.HexColor("#fce8e6")
    res_table = Table([[resolution]], colWidths=[17*cm])
    res_table.setStyle(TableStyle([
        ("BACKGROUND", (0,0),(0,0), bg),
        ("FONTNAME",   (0,0),(0,0), "Helvetica-Bold"),
        ("FONTSIZE",   (0,0),(0,0), 10),
        ("PADDING",    (0,0),(0,0), 10),
    ]))
    story.append(res_table)
    story.append(Spacer(1, 6))
    story.append(Paragraph(
        f"Case closed on {closed_date.strftime('%d %b %Y')}. "
        f"Customer notified via registered email ({cust['customer_email']}) and post.",
        styles["Body"]
    ))
    story.append(Spacer(1, 8))
    story.append(Paragraph(
        "This document is confidential and intended solely for SecureBank PLC internal use.",
        styles["Small"]
    ))

    doc.build(story)
    buffer.seek(0)
    return buffer.read(), amount, dispute_type, resolution, closed_date


def generate_complaint(cust, doc_id, filed_date, status):
    buffer = io.BytesIO()
    doc    = _base_doc(buffer, f"Complaint {doc_id}")
    styles = _styles()
    story  = []

    complaint_type = random.choice(COMPLAINT_TYPES)
    priority       = random.choice(["Low", "Medium", "High", "Critical"])
    closed_date    = filed_date + timedelta(days=random.randint(3, 56))
    resolution     = random.choice(RESOLUTIONS)
    compensation   = round(random.uniform(0, 500), 2) if random.random() < 0.4 else 0

    _header_footer(story, styles, "Complaint", doc_id, cust)

    story.append(Paragraph("COMPLAINT CASE — CLOSED", styles["SectionHdr"]))
    story.append(_customer_table(styles, cust))
    story.append(Spacer(1, 10))

    story.append(Paragraph("Complaint Details", styles["SectionHdr"]))
    case_data = [
        ["Case Reference",    doc_id,            "Status",           status],
        ["Complaint Type",    complaint_type,    "Priority",         priority],
        ["Date Filed",        filed_date.strftime("%d %b %Y"), "Date Closed", closed_date.strftime("%d %b %Y")],
        ["Assigned Officer",  cust["rm_name"],   "Branch",           cust["branch_name"]],
        ["Compensation Paid", f"${compensation:,.2f}" if compensation else "None", "", ""],
    ]
    ct = Table(case_data, colWidths=[3.5*cm, 6*cm, 3.5*cm, 4*cm])
    ct.setStyle(TableStyle([
        ("FONTNAME",    (0,0),(0,-1), "Helvetica-Bold"),
        ("FONTNAME",    (2,0),(2,-1), "Helvetica-Bold"),
        ("FONTSIZE",    (0,0),(-1,-1), 8.5),
        ("GRID",        (0,0),(-1,-1), 0.25, colors.lightgrey),
        ("ROWBACKGROUNDS",(0,0),(-1,-1),[colors.HexColor("#f5f8fc"), colors.white]),
        ("PADDING",     (0,0),(-1,-1), 5),
    ]))
    story.append(ct)
    story.append(Spacer(1, 10))

    story.append(Paragraph("Customer Complaint", styles["SectionHdr"]))
    story.append(Paragraph(
        f"The customer raised a formal complaint regarding {complaint_type.lower()} on "
        f"{filed_date.strftime('%d %b %Y')}. The complaint was classified as {priority.upper()} priority "
        f"and assigned to {cust['rm_name']} at {cust['branch_name']} branch for investigation. "
        f"The customer expressed dissatisfaction with the service received and requested formal acknowledgement "
        f"within the regulatory-prescribed 5-business-day window.",
        styles["Body"]
    ))
    story.append(Spacer(1, 8))

    story.append(Paragraph("Case Summary", styles["SectionHdr"]))
    story.append(Paragraph(
        f"Following a thorough review of the customer's account history, branch interaction logs, "
        f"and written correspondence, the complaints team determined that the case warranted a "
        f"{'full investigation' if priority in ['High','Critical'] else 'standard review'}. "
        f"All relevant stakeholders were consulted including {cust['rm_name']} (RM), "
        f"branch management, and the central complaints team. "
        f"A final decision was reached on {closed_date.strftime('%d %b %Y')}.",
        styles["Body"]
    ))
    story.append(Spacer(1, 8))

    story.append(Paragraph("Resolution & Outcome", styles["SectionHdr"]))
    story.append(Paragraph(f"Outcome: {resolution}", styles["BodyBold"]))
    if compensation > 0:
        story.append(Paragraph(f"Goodwill compensation of ${compensation:,.2f} credited to account {cust['account_number']}.", styles["Body"]))
    story.append(Spacer(1, 4))
    story.append(Paragraph(
        f"Customer informed of outcome on {closed_date.strftime('%d %b %Y')} "
        f"via email to {cust['customer_email']}. "
        f"Customer retains the right to refer this matter to the Consumer Financial Protection Bureau (CFPB) "
        f"within 6 months of this final response.",
        styles["Body"]
    ))
    story.append(Spacer(1, 8))
    story.append(Paragraph(
        "SecureBank PLC — Complaints Team  |  complaints@securebankplc.com  |  FDIC Member",
        styles["Small"]
    ))

    doc.build(story)
    buffer.seek(0)
    return buffer.read(), complaint_type, priority, resolution, compensation, closed_date


def generate_maintenance(cust, doc_id, req_date, status):
    buffer = io.BytesIO()
    doc    = _base_doc(buffer, f"Maintenance {doc_id}")
    styles = _styles()
    story  = []

    req_type       = random.choice(MAINTENANCE_TYPES)
    processed_date = req_date + timedelta(days=random.randint(1, 10))
    processor      = random.choice(RM_NAMES)

    _header_footer(story, styles, "Maintenance", doc_id, cust)

    story.append(Paragraph("ACCOUNT MAINTENANCE REQUEST", styles["SectionHdr"]))
    story.append(_customer_table(styles, cust))
    story.append(Spacer(1, 10))

    story.append(Paragraph("Request Details", styles["SectionHdr"]))
    req_data = [
        ["Request Reference", doc_id,            "Status",           status],
        ["Request Type",      req_type,          "Date Submitted",   req_date.strftime("%d %b %Y")],
        ["Date Processed",    processed_date.strftime("%d %b %Y"), "Processed By", processor],
        ["Branch",            cust["branch_name"],"RM",             cust["rm_name"]],
    ]
    rt = Table(req_data, colWidths=[3.5*cm, 6*cm, 3.5*cm, 4*cm])
    rt.setStyle(TableStyle([
        ("FONTNAME",    (0,0),(0,-1), "Helvetica-Bold"),
        ("FONTNAME",    (2,0),(2,-1), "Helvetica-Bold"),
        ("FONTSIZE",    (0,0),(-1,-1), 8.5),
        ("GRID",        (0,0),(-1,-1), 0.25, colors.lightgrey),
        ("ROWBACKGROUNDS",(0,0),(-1,-1),[colors.HexColor("#f5f8fc"), colors.white]),
        ("PADDING",     (0,0),(-1,-1), 5),
    ]))
    story.append(rt)
    story.append(Spacer(1, 10))

    story.append(Paragraph("Request Description", styles["SectionHdr"]))
    story.append(Paragraph(
        f"Customer {cust['customer_name']} (ID: {cust['customer_id']}) submitted an account maintenance "
        f"request for {req_type.lower()} on {req_date.strftime('%d %b %Y')}. "
        f"The request was received by {cust['branch_name']} branch and assigned to {processor} for processing. "
        f"Identity verification was completed in accordance with SecureBank PLC KYC policy before "
        f"any changes were applied to the account.",
        styles["Body"]
    ))
    story.append(Spacer(1, 8))

    story.append(Paragraph("Processing Notes", styles["SectionHdr"]))
    story.append(Paragraph(
        f"Request reviewed and validated by {processor}. "
        f"All supporting documentation checked and filed. "
        f"Change applied to account on {processed_date.strftime('%d %b %Y')}. "
        f"Confirmation letter dispatched to {cust['customer_address']} "
        f"and email notification sent to {cust['customer_email']}.",
        styles["Body"]
    ))
    story.append(Spacer(1, 8))
    story.append(Paragraph(
        "This record is retained for 7 years in accordance with federal banking regulations.",
        styles["Small"]
    ))

    doc.build(story)
    buffer.seek(0)
    return buffer.read(), req_type, processed_date


# ---------------------------------------------------------------------------
# S3 Setup
# ---------------------------------------------------------------------------
def create_bucket(s3, region):
    print(f"\nCreating S3 bucket: {BUCKET_NAME} in {region}...")
    try:
        if region == "us-east-1":
            s3.create_bucket(Bucket=BUCKET_NAME)
        else:
            s3.create_bucket(
                Bucket=BUCKET_NAME,
                CreateBucketConfiguration={"LocationConstraint": region}
            )
        # Block public access
        s3.put_public_access_block(
            Bucket=BUCKET_NAME,
            PublicAccessBlockConfiguration={
                "BlockPublicAcls": True, "IgnorePublicAcls": True,
                "BlockPublicPolicy": True, "RestrictPublicBuckets": True,
            }
        )
        print(f"Bucket created and locked down: s3://{BUCKET_NAME}")
    except s3.exceptions.BucketAlreadyOwnedByYou:
        print(f"Bucket already exists and is yours — continuing.")
    except Exception as e:
        print(f"Error creating bucket: {e}")
        sys.exit(1)


def upload_pdf(s3, key, pdf_bytes, metadata: dict):
    # S3 metadata must be ASCII-only strings — strip non-ASCII characters (e.g. $ symbol)
    str_meta = {k: str(v).encode("ascii", errors="ignore").decode("ascii") for k, v in metadata.items()}
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=pdf_bytes,
        ContentType="application/pdf",
        Metadata=str_meta,
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    s3 = boto3.client("s3", region_name=AWS_REGION)
    create_bucket(s3, AWS_REGION)

    customers   = make_customers(60)
    all_metadata = []
    total        = sum(DOC_COUNTS.values())
    count        = 0

    print(f"\nGenerating and uploading {total} documents...\n")

    # ---- eStatements -------------------------------------------------------
    for i in range(DOC_COUNTS["estatement"]):
        cust     = random.choice(customers)
        stmt_date = random_date(730, 30)
        doc_id   = f"STMT{str(i+1).zfill(5)}"
        key      = f"estatements/{stmt_date.year}/{stmt_date.strftime('%m')}/{doc_id}.pdf"

        pdf_bytes, closing_bal = generate_estatement(cust, doc_id, stmt_date)

        meta = {
            "doc_id":            doc_id,
            "doc_type":          "eStatement",
            "customer_id":       cust["customer_id"],
            "customer_name":     cust["customer_name"],
            "customer_email":    cust["customer_email"],
            "customer_phone":    cust["customer_phone"],
            "customer_address":  cust["customer_address"],
            "account_number":    cust["account_number"],
            "account_type":      cust["account_type"],
            "sort_code":         cust["sort_code"],
            "branch_code":       cust["branch_code"],
            "branch_name":       cust["branch_name"],
            "rm_id":             cust["rm_id"],
            "rm_name":           cust["rm_name"],
            "rm_email":          cust["rm_email"],
            "statement_date":    stmt_date.isoformat(),
            "closing_balance":   closing_bal,
            "s3_path":           f"s3://{BUCKET_NAME}/{key}",
            "case_summary":      f"Monthly {cust['account_type']} account statement. Closing balance ${closing_bal:,.2f}.",
        }
        upload_pdf(s3, key, pdf_bytes, meta)
        all_metadata.append(meta)
        count += 1
        print(f"[{count}/{total}] {key}")

    # ---- Disputes ----------------------------------------------------------
    for i in range(DOC_COUNTS["dispute"]):
        cust        = random.choice(customers)
        filed_date  = random_date(700, 30)
        status      = random.choice(CASE_STATUS)
        doc_id      = f"DSP{str(i+1).zfill(5)}"
        key         = f"disputes/{filed_date.year}/{filed_date.strftime('%m')}/{doc_id}.pdf"

        pdf_bytes, amount, dtype, resolution, closed_date = generate_dispute(cust, doc_id, filed_date, status)

        meta = {
            "doc_id":            doc_id,
            "doc_type":          "Dispute",
            "customer_id":       cust["customer_id"],
            "customer_name":     cust["customer_name"],
            "customer_email":    cust["customer_email"],
            "customer_phone":    cust["customer_phone"],
            "customer_address":  cust["customer_address"],
            "account_number":    cust["account_number"],
            "account_type":      cust["account_type"],
            "sort_code":         cust["sort_code"],
            "branch_code":       cust["branch_code"],
            "branch_name":       cust["branch_name"],
            "rm_id":             cust["rm_id"],
            "rm_name":           cust["rm_name"],
            "rm_email":          cust["rm_email"],
            "case_status":       status,
            "dispute_type":      dtype,
            "dispute_amount":    amount,
            "filed_date":        filed_date.isoformat(),
            "closed_date":       closed_date.isoformat(),
            "resolution":        resolution,
            "s3_path":           f"s3://{BUCKET_NAME}/{key}",
            "case_summary":      f"{dtype} dispute of ${amount:,.2f}. Status: {status}. Resolution: {resolution}.",
        }
        upload_pdf(s3, key, pdf_bytes, meta)
        all_metadata.append(meta)
        count += 1
        print(f"[{count}/{total}] {key}")

    # ---- Complaints --------------------------------------------------------
    for i in range(DOC_COUNTS["complaint"]):
        cust       = random.choice(customers)
        filed_date = random_date(700, 30)
        status     = random.choice(CASE_STATUS)
        doc_id     = f"CMP{str(i+1).zfill(5)}"
        key        = f"complaints/{filed_date.year}/{filed_date.strftime('%m')}/{doc_id}.pdf"

        pdf_bytes, ctype, priority, resolution, compensation, closed_date = generate_complaint(cust, doc_id, filed_date, status)

        meta = {
            "doc_id":            doc_id,
            "doc_type":          "Complaint",
            "customer_id":       cust["customer_id"],
            "customer_name":     cust["customer_name"],
            "customer_email":    cust["customer_email"],
            "customer_phone":    cust["customer_phone"],
            "customer_address":  cust["customer_address"],
            "account_number":    cust["account_number"],
            "account_type":      cust["account_type"],
            "sort_code":         cust["sort_code"],
            "branch_code":       cust["branch_code"],
            "branch_name":       cust["branch_name"],
            "rm_id":             cust["rm_id"],
            "rm_name":           cust["rm_name"],
            "rm_email":          cust["rm_email"],
            "case_status":       status,
            "complaint_type":    ctype,
            "priority":          priority,
            "filed_date":        filed_date.isoformat(),
            "closed_date":       closed_date.isoformat(),
            "resolution":        resolution,
            "compensation_paid": compensation,
            "s3_path":           f"s3://{BUCKET_NAME}/{key}",
            "case_summary":      f"{ctype} complaint, priority {priority}. Status: {status}. {('Compensation: $' + str(compensation)) if compensation else 'No compensation paid.'}",
        }
        upload_pdf(s3, key, pdf_bytes, meta)
        all_metadata.append(meta)
        count += 1
        print(f"[{count}/{total}] {key}")

    # ---- Account Maintenance -----------------------------------------------
    for i in range(DOC_COUNTS["maintenance"]):
        cust     = random.choice(customers)
        req_date = random_date(700, 7)
        status   = random.choice(["Completed", "Completed", "Completed", "Pending", "Rejected"])
        doc_id   = f"MNT{str(i+1).zfill(5)}"
        key      = f"maintenance/{req_date.year}/{req_date.strftime('%m')}/{doc_id}.pdf"

        pdf_bytes, req_type, processed_date = generate_maintenance(cust, doc_id, req_date, status)

        meta = {
            "doc_id":           doc_id,
            "doc_type":         "AccountMaintenance",
            "customer_id":      cust["customer_id"],
            "customer_name":    cust["customer_name"],
            "customer_email":   cust["customer_email"],
            "customer_phone":   cust["customer_phone"],
            "customer_address": cust["customer_address"],
            "account_number":   cust["account_number"],
            "account_type":     cust["account_type"],
            "sort_code":        cust["sort_code"],
            "branch_code":      cust["branch_code"],
            "branch_name":      cust["branch_name"],
            "rm_id":            cust["rm_id"],
            "rm_name":          cust["rm_name"],
            "rm_email":         cust["rm_email"],
            "request_type":     req_type,
            "request_status":   status,
            "request_date":     req_date.isoformat(),
            "processed_date":   processed_date.isoformat(),
            "s3_path":          f"s3://{BUCKET_NAME}/{key}",
            "case_summary":     f"{req_type} request. Status: {status}. Processed: {processed_date.strftime('%d %b %Y')}.",
        }
        upload_pdf(s3, key, pdf_bytes, meta)
        all_metadata.append(meta)
        count += 1
        print(f"[{count}/{total}] {key}")

    # ---- Write metadata files ----------------------------------------------
    print(f"\nWriting {OUTPUT_CSV}...")
    all_keys = sorted({k for m in all_metadata for k in m.keys()})
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=all_keys, extrasaction="ignore")
        writer.writeheader()
        for row in all_metadata:
            writer.writerow({k: row.get(k, "") for k in all_keys})

    print(f"Writing {OUTPUT_JSON}...")
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(all_metadata, f, indent=2, default=str)

    print(f"""
========================================================
  Done!
========================================================
  Bucket   : s3://{BUCKET_NAME}
  Documents: {total}
  Breakdown:
    eStatements  : {DOC_COUNTS['estatement']}
    Disputes     : {DOC_COUNTS['dispute']}
    Complaints   : {DOC_COUNTS['complaint']}
    Maintenance  : {DOC_COUNTS['maintenance']}

  Local files:
    {OUTPUT_CSV}   ← import this to ClickHouse
    {OUTPUT_JSON}  ← alternative format

  S3 folder structure:
    s3://{BUCKET_NAME}/estatements/YYYY/MM/
    s3://{BUCKET_NAME}/disputes/YYYY/MM/
    s3://{BUCKET_NAME}/complaints/YYYY/MM/
    s3://{BUCKET_NAME}/maintenance/YYYY/MM/

  Next step — run ingest.py to dual-write to ChromaDB + ClickHouse automatically:
    python rag/ingest.py
========================================================
""")


if __name__ == "__main__":
    main()
