-- Run this in ClickHouse Cloud SQL console
-- ReplacingMergeTree allows upserts — re-running ingest.py won't create duplicates
-- To recreate: DROP TABLE banking_docs.documents; then run this script.

CREATE DATABASE IF NOT EXISTS banking_docs;

CREATE TABLE IF NOT EXISTS banking_docs.documents (

    -- Document identity
    doc_id            String,
    doc_type          String,          -- eStatement | Dispute | Complaint | AccountMaintenance
    s3_path           String,

    -- Customer details
    customer_id       String,
    customer_name     String,
    customer_email    String,
    customer_phone    String,
    customer_address  String,

    -- Account details
    account_number    String,
    account_type      String,
    sort_code         String,

    -- Relationship Manager
    rm_id             String,
    rm_name           String,
    rm_email          String,

    -- Branch
    branch_code       String,
    branch_name       String,

    -- eStatement fields
    statement_date    Nullable(Date),
    closing_balance   Nullable(Float64),   -- USD

    -- Case fields (Disputes + Complaints)
    case_status       String,
    filed_date        Nullable(Date),
    closed_date       Nullable(Date),
    resolution        String,

    -- Dispute-specific
    dispute_type      String,
    dispute_amount    Nullable(Float64),   -- USD

    -- Complaint-specific
    complaint_type    String,
    priority          String,
    compensation_paid Nullable(Float64),   -- USD

    -- Maintenance-specific
    request_type      String,
    request_status    String,
    request_date      Nullable(Date),
    processed_date    Nullable(Date),

    -- Free-text summary
    case_summary      String,

    -- Pipeline timestamp (used as ReplacingMergeTree version — latest wins)
    ingested_at       DateTime DEFAULT now()

) ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (doc_type, customer_id, doc_id)
SETTINGS index_granularity = 8192;


-- Query with FINAL to get deduplicated rows (ReplacingMergeTree)
-- Example: SELECT * FROM banking_docs.documents FINAL WHERE doc_type = 'Dispute'


-- Useful views (always deduplicated)
CREATE VIEW IF NOT EXISTS banking_docs.disputes AS
SELECT * FROM banking_docs.documents FINAL WHERE doc_type = 'Dispute';

CREATE VIEW IF NOT EXISTS banking_docs.complaints AS
SELECT * FROM banking_docs.documents FINAL WHERE doc_type = 'Complaint';

CREATE VIEW IF NOT EXISTS banking_docs.estatements AS
SELECT * FROM banking_docs.documents FINAL WHERE doc_type = 'eStatement';

CREATE VIEW IF NOT EXISTS banking_docs.maintenance AS
SELECT * FROM banking_docs.documents FINAL WHERE doc_type = 'AccountMaintenance';
