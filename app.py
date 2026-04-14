"""
SecureBank AI Assistant — Streamlit Chat UI
Run: streamlit run app.py
"""

import os
import streamlit as st

# ---------------------------------------------------------------------------
# Page config  (must be first Streamlit call)
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="SecureBank AI Assistant",
    page_icon="🏦",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Password gate — shown before anything else loads
# ---------------------------------------------------------------------------
_DEMO_PASSWORD = os.getenv("APP_PASSWORD", "securebank2025")
try:
    import streamlit as _st
    _DEMO_PASSWORD = _st.secrets.get("APP_PASSWORD", _DEMO_PASSWORD)
except Exception:
    pass


def _check_password() -> bool:
    """Return True once the correct password has been entered."""

    def _on_submit():
        if st.session_state.get("_pw_input") == _DEMO_PASSWORD:
            st.session_state["_authenticated"] = True
        else:
            st.session_state["_auth_failed"] = True

    if st.session_state.get("_authenticated"):
        return True

    # ── Login screen ────────────────────────────────────────────────────────
    st.markdown("""
    <div style="max-width:440px;margin:80px auto 0;text-align:center">
      <div style="background:linear-gradient(135deg,#003366,#0055a5);
                  border-radius:16px;padding:36px 40px;color:white;
                  box-shadow:0 8px 32px rgba(0,51,102,0.2)">
        <div style="font-size:48px;margin-bottom:12px">🏦</div>
        <h2 style="color:white;margin:0 0 6px">SecureBank AI Assistant</h2>
        <p style="color:#aaccee;margin:0 0 24px;font-size:14px">
          Powered by AWS Bedrock · ClickHouse · ChromaDB RAG
        </p>
      </div>
    </div>
    """, unsafe_allow_html=True)

    col_l, col_c, col_r = st.columns([1, 2, 1])
    with col_c:
        st.markdown("<div style='height:20px'/>", unsafe_allow_html=True)
        st.text_input(
            "🔐 Demo Password",
            type="password",
            key="_pw_input",
            on_change=_on_submit,
            placeholder="Enter password to access demo…",
        )
        st.button("Log in →", on_click=_on_submit, use_container_width=True, type="primary")

        if st.session_state.get("_auth_failed"):
            st.error("Incorrect password. Contact the author on LinkedIn for access.")

        st.markdown("""
        <div style="text-align:center;margin-top:20px;color:#8899aa;font-size:12px">
          This is a banking AI POC demo.<br>All customer data is synthetic / anonymised.
        </div>
        """, unsafe_allow_html=True)

    return False


if not _check_password():
    st.stop()

# ---------------------------------------------------------------------------
# Authenticated — load the rest of the app
# ---------------------------------------------------------------------------
from rag.chain import BankingRAG  # noqa: E402 — imported after auth check

# ---------------------------------------------------------------------------
# Custom CSS
# ---------------------------------------------------------------------------
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(135deg, #003366 0%, #0055a5 100%);
        padding: 1.5rem 2rem;
        border-radius: 10px;
        margin-bottom: 1.5rem;
        color: white;
    }
    .main-header h1 { color: white; margin: 0; font-size: 1.8rem; }
    .main-header p  { color: #cce0ff; margin: 0.3rem 0 0 0; font-size: 0.95rem; }

    .source-card {
        background: #f0f4ff;
        border-left: 4px solid #003366;
        border-radius: 6px;
        padding: 0.7rem 1rem;
        margin: 0.4rem 0;
        font-size: 0.85rem;
    }
    .source-card .doc-id  { font-weight: bold; color: #003366; font-size: 0.95rem; }
    .source-card .summary { color: #555; margin-top: 0.2rem; }

    .filter-badge {
        background: #e8f0fe;
        border: 1px solid #4a90d9;
        border-radius: 12px;
        padding: 2px 10px;
        font-size: 0.78rem;
        color: #003366;
        display: inline-block;
        margin: 2px;
    }

    .stChatMessage [data-testid="stMarkdownContainer"] p { font-size: 0.95rem; }

    /* Sidebar */
    .sample-query {
        background: #f5f8ff;
        border: 1px solid #d0dff7;
        border-radius: 6px;
        padding: 0.5rem 0.7rem;
        margin: 0.3rem 0;
        font-size: 0.82rem;
        cursor: pointer;
        color: #003366;
    }
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
st.markdown("""
<div class="main-header">
    <h1>🏦 SecureBank AI Assistant</h1>
    <p>Ask questions about customer eStatements, Disputes, Complaints and Account Maintenance in plain English</p>
</div>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
with st.sidebar:
    st.markdown("### 🔍 Sample Questions")

    sample_questions = [
        "How many complaints were raised each year?",
        "How many disputes per branch?",
        "Breakdown of complaints by type",
        "How many cases were referred to Ombudsman by year?",
        "Which relationship manager handled the most disputes?",
        "Summarise the complaint from Mathew Little",
        "What was the dispute about in case DSP00012?",
        "Show all high priority complaints from Leeds branch",
        "What complaints were referred to the Ombudsman?",
        "What is the resolution of case CMP00047?",
        "Tell me about the mortgage complaints this year",
        "Which customers had disputes over £5000?",
        "What account maintenance did customer CUST00020 request?",
        "Summarise all product mis-selling complaints",
        "What was the outcome of the unauthorised transaction disputes?",
    ]

    for q in sample_questions:
        if st.button(q, key=q, use_container_width=True):
            st.session_state["pending_query"] = q

    st.markdown("---")
    st.markdown("### ⚙️ Settings")
    show_filters  = st.toggle("Show extracted filters", value=True)
    show_sources  = st.toggle("Show source documents", value=True)

    st.markdown("---")
    st.markdown("### 📊 Document Coverage")
    st.markdown("""
    | Type | Count |
    |---|---|
    | eStatements | 400 |
    | Disputes | 250 |
    | Complaints | 200 |
    | Maintenance | 150 |
    """)

    st.markdown("---")
    if st.button("🗑️ Clear conversation", use_container_width=True):
        st.session_state["messages"] = []
        st.rerun()

# ---------------------------------------------------------------------------
# Initialise session state
# ---------------------------------------------------------------------------
if "messages" not in st.session_state:
    st.session_state["messages"] = []

if "rag" not in st.session_state:
    with st.spinner("Loading AI assistant..."):
        st.session_state["rag"] = BankingRAG()

rag: BankingRAG = st.session_state["rag"]

# ---------------------------------------------------------------------------
# Helper — render a result (works for both aggregation and content queries)
# ---------------------------------------------------------------------------
def render_result(result: dict):
    """Render answer + table or source cards depending on query type."""
    import pandas as pd

    st.markdown(result["answer"])

    qtype = result.get("query_type", "content")

    # ── ClickHouse NL→SQL: show query + full results table ────────────────
    if qtype == "clickhouse_nl_sql":
        table_data = result.get("table_data", [])
        sql        = result.get("sql", "")
        row_count  = result.get("row_count", 0)
        if table_data:
            with st.expander(f"📊 ClickHouse results — {row_count} rows", expanded=True):
                st.dataframe(pd.DataFrame(table_data), use_container_width=True, hide_index=True)
        if sql and show_filters:
            with st.expander("🔎 Generated SQL"):
                st.code(sql, language="sql")
        st.caption("⚡ ClickHouse — NL → SQL across full dataset")

    # ── Content RAG: show source cards ───────────────────────────────────
    elif result.get("sources") and show_sources:
        with st.expander(f"📄 {len(result['sources'])} source document(s)"):
            for src in result["sources"]:
                s3_url = src["s3_path"].replace("s3://", "https://s3.amazonaws.com/")
                st.markdown(f"""
<div class="source-card">
    <div class="doc-id">{src['doc_id']} — {src['doc_type']}</div>
    <div>👤 {src['customer_name']}  &nbsp;|&nbsp;  🏢 {src['branch_name']}</div>
    <div class="summary">{src['case_summary'][:120]}...</div>
    <div><a href="{s3_url}" target="_blank">📎 View PDF in S3</a></div>
</div>
""", unsafe_allow_html=True)
        st.caption("🔍 ChromaDB — semantic vector search")

    # ── Filters applied (content queries) ────────────────────────────────
    if show_filters and result.get("filters_applied"):
        badges = " ".join([
            f'<span class="filter-badge">🔎 {k}: {v}</span>'
            for k, v in result["filters_applied"].items()
        ])
        st.markdown(f"**Filters applied:** {badges}", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Chat history (replay stored messages)
# ---------------------------------------------------------------------------
for msg in st.session_state["messages"]:
    with st.chat_message(msg["role"], avatar="🏦" if msg["role"] == "assistant" else "👤"):
        st.markdown(msg["content"])
        # Replay aggregation table if stored
        if msg["role"] == "assistant" and msg.get("agg_data"):
            import pandas as pd
            agg = msg["agg_data"]
            counts = agg.get("counts", {})
            total  = agg.get("total", 0)
            if counts:
                with st.expander(f"📊 Full data — {total} documents", expanded=False):
                    rows = [{agg["group_by"].replace("_"," ").title(): k, "Count": v, "% of Total": f"{round(v/total*100,1)}%"} for k,v in counts.items()]
                    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
        # Replay source cards
        elif msg["role"] == "assistant" and show_sources and msg.get("sources"):
            with st.expander(f"📄 {len(msg['sources'])} source document(s)"):
                for src in msg["sources"]:
                    s3_url = src["s3_path"].replace("s3://", "https://s3.amazonaws.com/")
                    st.markdown(f"""<div class="source-card">
    <div class="doc-id">{src['doc_id']} — {src['doc_type']}</div>
    <div>👤 {src['customer_name']}  &nbsp;|&nbsp;  🏢 {src['branch_name']}</div>
    <div class="summary">{src['case_summary'][:120]}...</div>
    <div><a href="{s3_url}" target="_blank">📎 View PDF in S3</a></div>
</div>""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Handle sidebar sample question click
# ---------------------------------------------------------------------------
if "pending_query" in st.session_state:
    pending = st.session_state.pop("pending_query")
    st.session_state["messages"].append({"role": "user", "content": pending})
    with st.chat_message("user", avatar="👤"):
        st.markdown(pending)
    with st.chat_message("assistant", avatar="🏦"):
        with st.spinner("Searching documents..."):
            result = rag.ask(pending)
        render_result(result)
    st.session_state["messages"].append({
        "role":     "assistant",
        "content":  result["answer"],
        "sources":  result.get("sources", []),
        "filters":  result.get("filters_applied", {}),
        "agg_data": result.get("agg_data"),
    })
    st.rerun()

# ---------------------------------------------------------------------------
# Chat input
# ---------------------------------------------------------------------------
if prompt := st.chat_input("Ask about any customer document, case or statement..."):
    st.session_state["messages"].append({"role": "user", "content": prompt})
    with st.chat_message("user", avatar="👤"):
        st.markdown(prompt)
    with st.chat_message("assistant", avatar="🏦"):
        with st.spinner("Searching documents..."):
            result = rag.ask(prompt)
        render_result(result)
    st.session_state["messages"].append({
        "role":     "assistant",
        "content":  result["answer"],
        "sources":  result.get("sources", []),
        "filters":  result.get("filters_applied", {}),
        "agg_data": result.get("agg_data"),
    })
