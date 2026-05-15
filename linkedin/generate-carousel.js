"use strict";
const pptxgen = require("pptxgenjs");

const pres = new pptxgen();
pres.layout = "LAYOUT_16x9";
pres.author = "SecureBank AI";
pres.title = "SecureBank AI LinkedIn Carousel";

// ─── Color constants (no # prefix per pptxgenjs rules) ───────────────────────
const NAVY   = "003366";
const BLUE   = "0055a5";
const GREEN  = "00aa66";
const WHITE  = "ffffff";
const LBLUE  = "aaccee";   // light blue for subtitles on dark bg
const DARK1  = "1a2540";
const GRAY1  = "556677";
const GRAY2  = "8899aa";
const GRAY3  = "334455";
const ACCENT = "cc3333";

// ─── Helper: stat box on slide 1 ─────────────────────────────────────────────
function addStatBox(slide, x, big, small) {
  // big number
  slide.addText(big, { x, y: 3.4, w: 2.8, h: 0.55, fontSize: 36, bold: true, color: WHITE, align: "center", valign: "bottom" });
  // small label
  slide.addText(small, { x, y: 3.95, w: 2.8, h: 0.35, fontSize: 12, color: LBLUE, align: "center", valign: "top" });
}

// ─── Helper: pill label on slide 5 ───────────────────────────────────────────
function addPill(slide, x, y, label, bgColor, borderColor) {
  slide.addShape(pres.shapes.ROUNDED_RECTANGLE, {
    x, y, w: 2.0, h: 0.4,
    fill: { color: bgColor },
    line: { color: borderColor, width: 1 },
    rectRadius: 0.08
  });
  slide.addText(label, { x, y, w: 2.0, h: 0.4, fontSize: 13, color: WHITE, align: "center", valign: "middle" });
}

// ══════════════════════════════════════════════════════════════════════════════
// SLIDE 1 — Dark navy
// ══════════════════════════════════════════════════════════════════════════════
{
  const s = pres.addSlide();
  s.background = { color: NAVY };

  // Weekend project tag
  s.addText("WEEKEND PROJECT", { x: 0.5, y: 0.3, w: 3, h: 0.3, fontSize: 11, color: WHITE, bold: false });

  // Main title
  s.addText("🏦 I Built a Banking AI Document Assistant in a Weekend", {
    x: 0.5, y: 0.9, w: 9, h: 1.1, fontSize: 40, bold: true, color: WHITE,
    fontFace: "Arial Black", wrap: true
  });

  // Subtitle
  s.addText("1,000 banking documents. Natural language search. AWS Bedrock + ClickHouse + ChromaDB RAG.", {
    x: 0.5, y: 2.2, w: 9, h: 0.5, fontSize: 18, color: LBLUE
  });

  // Divider line
  s.addShape(pres.shapes.RECTANGLE, { x: 0.5, y: 3.1, w: 9, h: 0.02, fill: { color: WHITE }, line: { color: WHITE, width: 0 } });

  // Stat boxes — background panels
  s.addShape(pres.shapes.RECTANGLE, { x: 0.5, y: 3.3, w: 2.8, h: 0.9, fill: { color: "0a2244" }, line: { color: "1a4488", width: 1 } });
  s.addShape(pres.shapes.RECTANGLE, { x: 3.6, y: 3.3, w: 2.8, h: 0.9, fill: { color: "0a2244" }, line: { color: "1a4488", width: 1 } });
  s.addShape(pres.shapes.RECTANGLE, { x: 6.7, y: 3.3, w: 2.8, h: 0.9, fill: { color: "0a2244" }, line: { color: "1a4488", width: 1 } });

  addStatBox(s, 0.5, "1,000",    "Documents");
  addStatBox(s, 3.6, "3,948",    "Vector Chunks");
  addStatBox(s, 6.7, "2 Engines","ClickHouse + ChromaDB");

  // Hashtags
  s.addText("#AI #Fintech #AWS #RAG #Banking", { x: 0.5, y: 5.1, w: 9, h: 0.3, fontSize: 11, color: LBLUE });
}

// ══════════════════════════════════════════════════════════════════════════════
// SLIDE 2 — White bg, "The Problem"
// ══════════════════════════════════════════════════════════════════════════════
{
  const s = pres.addSlide();
  s.background = { color: WHITE };

  // Top accent bar
  s.addShape(pres.shapes.RECTANGLE, { x: 0, y: 0, w: 10, h: 0.55, fill: { color: NAVY }, line: { color: NAVY, width: 0 } });
  s.addText("The Problem Bankers Face Every Day", { x: 0.5, y: 0.12, w: 9, h: 0.35, fontSize: 22, bold: true, color: WHITE, margin: 0 });

  const cards = [
    { y: 0.8,  borderColor: "cc3333", icon: "📁", title: "Thousands of PDFs — impossible to search",         sub: "Disputes, complaints, statements all siloed in different folders" },
    { y: 2.15, borderColor: "ff8800", icon: "⏱️", title: "Finding one case = opening 20 documents",          sub: "No keyword search, no metadata filter, no way to query across docs" },
    { y: 3.5,  borderColor: BLUE,    icon: "📊", title: "Analytics questions need a data team",              sub: "How many complaints per branch this year? No one knows without running SQL" },
  ];

  for (const c of cards) {
    // Card bg
    s.addShape(pres.shapes.RECTANGLE, { x: 0.5, y: c.y, w: 9, h: 1.15, fill: { color: "f8fafd" }, line: { color: "dde8f0", width: 1 } });
    // Left color border
    s.addShape(pres.shapes.RECTANGLE, { x: 0.5, y: c.y, w: 0.1, h: 1.15, fill: { color: c.borderColor }, line: { color: c.borderColor, width: 0 } });
    // Icon
    s.addText(c.icon, { x: 0.7, y: c.y + 0.18, w: 0.7, h: 0.6, fontSize: 24, align: "center" });
    // Title
    s.addText(c.title, { x: 1.55, y: c.y + 0.08, w: 7.8, h: 0.45, fontSize: 18, bold: true, color: DARK1 });
    // Sub
    s.addText(c.sub, { x: 1.55, y: c.y + 0.55, w: 7.8, h: 0.5, fontSize: 13, color: GRAY1 });
  }
}

// ══════════════════════════════════════════════════════════════════════════════
// SLIDE 3 — White bg, "The Solution"
// ══════════════════════════════════════════════════════════════════════════════
{
  const s = pres.addSlide();
  s.background = { color: WHITE };

  // Top accent bar
  s.addShape(pres.shapes.RECTANGLE, { x: 0, y: 0, w: 10, h: 0.55, fill: { color: NAVY }, line: { color: NAVY, width: 0 } });
  s.addText("What I Built", { x: 0.5, y: 0.12, w: 9, h: 0.35, fontSize: 22, bold: true, color: WHITE, margin: 0 });

  // Big center callout
  s.addShape(pres.shapes.RECTANGLE, { x: 2.5, y: 0.75, w: 5, h: 1.1, fill: { color: NAVY }, line: { color: NAVY, width: 0 } });
  s.addText("1 question", { x: 2.5, y: 0.82, w: 5, h: 0.42, fontSize: 32, bold: true, color: WHITE, align: "center" });
  s.addText("→ instant answer", { x: 2.5, y: 1.27, w: 5, h: 0.42, fontSize: 16, color: LBLUE, align: "center" });

  // Feature cards 2x2
  const featureCards = [
    { x: 0.5, y: 2.05, fill: "edf5ff", border: BLUE,  icon: "💬", title: "Ask in plain English",       sub: "No SQL. No training required.",    green: false },
    { x: 5.2, y: 2.05, fill: "edf5ff", border: BLUE,  icon: "📚", title: "Searches 1,000 Documents",  sub: "Every PDF. Every chunk.",          green: false },
    { x: 0.5, y: 3.3,  fill: "edfaf2", border: GREEN, icon: "📎", title: "Cites Source Documents",     sub: "With links back to PDFs",          green: true  },
    { x: 5.2, y: 3.3,  fill: "edfaf2", border: GREEN, icon: "📊", title: "Aggregates All Data",        sub: "Counts, trends, breakdowns",       green: true  },
  ];

  for (const c of featureCards) {
    s.addShape(pres.shapes.RECTANGLE, { x: c.x, y: c.y, w: 4.3, h: 1.1, fill: { color: c.fill }, line: { color: c.border, width: 1 } });
    s.addText(c.icon + " " + c.title, { x: c.x + 0.15, y: c.y + 0.12, w: 4.0, h: 0.45, fontSize: 16, bold: true, color: NAVY });
    s.addText(c.sub, { x: c.x + 0.15, y: c.y + 0.6, w: 4.0, h: 0.38, fontSize: 13, color: GRAY1 });
  }

  // Footer
  s.addText("Powered by AWS Bedrock · ClickHouse Cloud · ChromaDB · LangChain · Streamlit", {
    x: 0, y: 4.9, w: 10, h: 0.3, fontSize: 11, color: GRAY2, align: "center"
  });
}

// ══════════════════════════════════════════════════════════════════════════════
// SLIDE 4 — White bg, "Architecture"
// ══════════════════════════════════════════════════════════════════════════════
{
  const s = pres.addSlide();
  s.background = { color: WHITE };

  // Top accent bar
  s.addShape(pres.shapes.RECTANGLE, { x: 0, y: 0, w: 10, h: 0.55, fill: { color: NAVY }, line: { color: NAVY, width: 0 } });
  s.addText("Smart Dual-Engine Architecture", { x: 0.5, y: 0.12, w: 9, h: 0.35, fontSize: 22, bold: true, color: WHITE, margin: 0 });

  // Router box
  s.addShape(pres.shapes.RECTANGLE, { x: 3.5, y: 0.7, w: 3, h: 0.7, fill: { color: "fff8ee" }, line: { color: "ff9900", width: 2 } });
  s.addText("🔀 Query Router", { x: 3.5, y: 0.74, w: 3, h: 0.32, fontSize: 14, bold: true, color: "884400", align: "center" });
  s.addText("automatic intent detection", { x: 3.5, y: 1.04, w: 3, h: 0.28, fontSize: 11, color: "aa6600", align: "center" });

  // Arrow left (router → left engine)
  s.addShape(pres.shapes.LINE, { x: 3.6, y: 1.4, w: 2.5, h: 0.5, line: { color: GREEN, width: 2 } });
  // Arrow right (router → right engine)
  s.addShape(pres.shapes.LINE, { x: 6.4, y: 1.4, w: 2.5, h: 0.5, line: { color: BLUE, width: 2 } });

  // LEFT engine box
  s.addShape(pres.shapes.RECTANGLE, { x: 0.3, y: 1.6, w: 4.3, h: 3.5, fill: { color: "f0fdf6" }, line: { color: GREEN, width: 2 } });
  s.addShape(pres.shapes.RECTANGLE, { x: 0.3, y: 1.6, w: 4.3, h: 0.48, fill: { color: GREEN }, line: { color: GREEN, width: 0 } });
  s.addText("📊 Aggregation", { x: 0.3, y: 1.63, w: 4.3, h: 0.42, fontSize: 16, bold: true, color: WHITE, align: "center", margin: 0 });

  const leftItems = [
    "How many disputes per branch?",
    "Which RM handled most cases?",
    "Total compensation by branch?",
  ];
  leftItems.forEach((t, i) => {
    s.addText(t, { x: 0.5, y: 2.18 + i * 0.38, w: 3.9, h: 0.35, fontSize: 13, color: "116633" });
  });

  s.addShape(pres.shapes.LINE, { x: 0.5, y: 3.35, w: 3.9, h: 0, line: { color: "c0ddc8", width: 1 } });
  s.addText("→ ClickHouse NL→SQL", { x: 0.5, y: 3.45, w: 3.9, h: 0.35, fontSize: 14, bold: true, color: NAVY });
  s.addText("Scans ALL 1,000 docs. No limits.", { x: 0.5, y: 3.82, w: 3.9, h: 0.32, fontSize: 12, color: GRAY1 });

  // RIGHT engine box
  s.addShape(pres.shapes.RECTANGLE, { x: 5.4, y: 1.6, w: 4.3, h: 3.5, fill: { color: "f0f6ff" }, line: { color: BLUE, width: 2 } });
  s.addShape(pres.shapes.RECTANGLE, { x: 5.4, y: 1.6, w: 4.3, h: 0.48, fill: { color: BLUE }, line: { color: BLUE, width: 0 } });
  s.addText("🔍 Content Search", { x: 5.4, y: 1.63, w: 4.3, h: 0.42, fontSize: 16, bold: true, color: WHITE, align: "center", margin: 0 });

  const rightItems = [
    "Summarise this complaint",
    "Why was DSP00047 lost?",
    "What did the customer say?",
  ];
  rightItems.forEach((t, i) => {
    s.addText(t, { x: 5.6, y: 2.18 + i * 0.38, w: 3.9, h: 0.35, fontSize: 13, color: "0044aa" });
  });

  s.addShape(pres.shapes.LINE, { x: 5.6, y: 3.35, w: 3.9, h: 0, line: { color: "c0d4f8", width: 1 } });
  s.addText("→ ChromaDB Vector RAG", { x: 5.6, y: 3.45, w: 3.9, h: 0.35, fontSize: 14, bold: true, color: NAVY });
  s.addText("3,948 chunks · 256-dim embeddings", { x: 5.6, y: 3.82, w: 3.9, h: 0.32, fontSize: 12, color: GRAY1 });
}

// ══════════════════════════════════════════════════════════════════════════════
// SLIDE 5 — Dark navy, "Tech Stack"
// ══════════════════════════════════════════════════════════════════════════════
{
  const s = pres.addSlide();
  s.background = { color: NAVY };

  s.addText("Full Tech Stack", { x: 0.5, y: 0.25, w: 9, h: 0.45, fontSize: 28, bold: true, color: WHITE });
  s.addText("Everything used to build this POC", { x: 0.5, y: 0.75, w: 9, h: 0.3, fontSize: 14, color: LBLUE });

  const rows = [
    { y: 1.25, label: "☁️ AWS",   bg: "0d2a4a", border: "2266aa", pills: ["AWS S3", "AWS Bedrock", "Amazon Nova Lite", "Titan Embeddings v2"] },
    { y: 2.3,  label: "🗄️ DATA",  bg: "0a2a1a", border: GREEN,    pills: ["ClickHouse Cloud", "ReplacingMergeTree", "NL→SQL Pipeline", "ChromaDB"] },
    { y: 3.35, label: "🐍 DEV",   bg: "1a0a3a", border: "7744cc", pills: ["LangChain", "Python 3.9", "Streamlit Cloud", "pdfplumber"] },
    { y: 4.4,  label: "🏭 PROD",  bg: "2a1a0a", border: "cc8800", pills: ["IBM FileNet P8", "pgvector on RDS", "Claude Haiku"] },
  ];

  for (const row of rows) {
    s.addText(row.label, { x: 0.5, y: row.y + 0.04, w: 1.1, h: 0.36, fontSize: 11, bold: true, color: LBLUE });
    row.pills.forEach((pill, i) => {
      const px = 1.7 + i * 2.05;
      s.addShape(pres.shapes.ROUNDED_RECTANGLE, {
        x: px, y: row.y, w: 1.95, h: 0.38,
        fill: { color: row.bg },
        line: { color: row.border, width: 1 },
        rectRadius: 0.08
      });
      s.addText(pill, { x: px, y: row.y, w: 1.95, h: 0.38, fontSize: 11, color: WHITE, align: "center", valign: "middle" });
    });
  }
}

// ══════════════════════════════════════════════════════════════════════════════
// SLIDE 6 — White bg, "Pipeline"
// ══════════════════════════════════════════════════════════════════════════════
{
  const s = pres.addSlide();
  s.background = { color: WHITE };

  // Top accent bar
  s.addShape(pres.shapes.RECTANGLE, { x: 0, y: 0, w: 10, h: 0.55, fill: { color: NAVY }, line: { color: NAVY, width: 0 } });
  s.addText("Real-Time Dual-Write Pipeline", { x: 0.5, y: 0.12, w: 9, h: 0.35, fontSize: 22, bold: true, color: WHITE, margin: 0 });

  // S3 box
  s.addShape(pres.shapes.RECTANGLE, { x: 0.3, y: 0.8, w: 1.8, h: 0.8, fill: { color: "edf5ff" }, line: { color: BLUE, width: 1 } });
  s.addText("☁️ S3 PDF", { x: 0.3, y: 0.8, w: 1.8, h: 0.8, fontSize: 13, bold: true, color: NAVY, align: "center", valign: "middle" });

  // Arrow →
  s.addShape(pres.shapes.LINE, { x: 2.1, y: 1.2, w: 0.35, h: 0, line: { color: GRAY1, width: 2 } });

  // ingest.py box
  s.addShape(pres.shapes.RECTANGLE, { x: 2.5, y: 0.8, w: 2.0, h: 0.8, fill: { color: "fff8ee" }, line: { color: "ff9900", width: 1 } });
  s.addText("🐍 ingest.py", { x: 2.5, y: 0.8, w: 2.0, h: 0.8, fontSize: 13, bold: true, color: "884400", align: "center", valign: "middle" });

  // Split lines
  s.addShape(pres.shapes.LINE, { x: 1.5, y: 1.6, w: 2.2, h: 0.5, line: { color: "7744cc", width: 2 } });
  s.addShape(pres.shapes.LINE, { x: 3.6, y: 1.6, w: 3.6, h: 0.5, line: { color: GREEN, width: 2 } });

  // ChromaDB box
  s.addShape(pres.shapes.RECTANGLE, { x: 0.5, y: 2.0, w: 3.5, h: 0.9, fill: { color: "f6eeff" }, line: { color: "7744cc", width: 1 } });
  s.addText("📦 ChromaDB", { x: 0.5, y: 2.05, w: 3.5, h: 0.38, fontSize: 13, bold: true, color: "552288", align: "center" });
  s.addText("Content search · 3,948 chunks", { x: 0.5, y: 2.45, w: 3.5, h: 0.35, fontSize: 11, color: "776699", align: "center" });

  // ClickHouse box
  s.addShape(pres.shapes.RECTANGLE, { x: 5.5, y: 2.0, w: 3.5, h: 0.9, fill: { color: "edfaf2" }, line: { color: GREEN, width: 1 } });
  s.addText("🗄️ ClickHouse", { x: 5.5, y: 2.05, w: 3.5, h: 0.38, fontSize: 13, bold: true, color: "116633", align: "center" });
  s.addText("Aggregation · 1,000 docs · FINAL", { x: 5.5, y: 2.45, w: 3.5, h: 0.35, fontSize: 11, color: "338855", align: "center" });

  // Stat boxes
  const stats = [
    { val: "3,948", color: BLUE,      label: "chunks indexed" },
    { val: "1,000", color: GREEN,     label: "docs in ClickHouse" },
    { val: "Safe",  color: "ff9900",  label: "to re-run anytime" },
    { val: "Auto",  color: "7744cc",  label: "deduplicates" },
  ];
  stats.forEach((st, i) => {
    const sx = 0.3 + i * 2.35;
    s.addShape(pres.shapes.RECTANGLE, { x: sx, y: 3.15, w: 2.1, h: 0.85, fill: { color: "f8faff" }, line: { color: "dde8f0", width: 1 } });
    s.addText(st.val, { x: sx, y: 3.2, w: 2.1, h: 0.42, fontSize: 28, bold: true, color: st.color, align: "center" });
    s.addText(st.label, { x: sx, y: 3.62, w: 2.1, h: 0.3, fontSize: 11, color: GRAY1, align: "center" });
  });

  // Before/after note
  s.addText("BEFORE: manual CSV → import (goes stale)", { x: 0.3, y: 4.25, w: 4.7, h: 0.3, fontSize: 12, color: "cc3333" });
  s.addText("NOW: ./run.sh ingest → both stores sync", { x: 5.2, y: 4.25, w: 4.5, h: 0.3, fontSize: 12, bold: true, color: "116633" });
}

// ══════════════════════════════════════════════════════════════════════════════
// SLIDE 7 — White bg, "Example Queries"
// ══════════════════════════════════════════════════════════════════════════════
{
  const s = pres.addSlide();
  s.background = { color: WHITE };

  // Top accent bar
  s.addShape(pres.shapes.RECTANGLE, { x: 0, y: 0, w: 10, h: 0.55, fill: { color: NAVY }, line: { color: NAVY, width: 0 } });
  s.addText("Questions It Can Answer Right Now", { x: 0.5, y: 0.12, w: 9, h: 0.35, fontSize: 22, bold: true, color: WHITE, margin: 0 });

  // Left column header
  s.addShape(pres.shapes.RECTANGLE, { x: 0.4, y: 0.7, w: 4.4, h: 0.5, fill: { color: "edfaf2" }, line: { color: GREEN, width: 1 } });
  s.addText("📊 Aggregation → ClickHouse", { x: 0.4, y: 0.7, w: 4.4, h: 0.5, fontSize: 15, bold: true, color: "116633", align: "center", valign: "middle" });

  const leftQ = [
    "How many complaints raised each year?",
    "Which branch had the most disputes?",
    "Total compensation paid per RM?",
    "Cases referred to CFPB by year?",
  ];
  leftQ.forEach((q, i) => {
    s.addShape(pres.shapes.RECTANGLE, { x: 0.4, y: 1.3 + i * 0.82, w: 4.4, h: 0.72, fill: { color: WHITE }, line: { color: "c0ddc8", width: 1 } });
    s.addText(q, { x: 0.55, y: 1.3 + i * 0.82, w: 4.1, h: 0.72, fontSize: 13, color: GRAY3, valign: "middle" });
  });

  // Right column header
  s.addShape(pres.shapes.RECTANGLE, { x: 5.2, y: 0.7, w: 4.4, h: 0.5, fill: { color: "edf5ff" }, line: { color: BLUE, width: 1 } });
  s.addText("🔍 Content → ChromaDB RAG", { x: 5.2, y: 0.7, w: 4.4, h: 0.5, fontSize: 15, bold: true, color: "0044aa", align: "center", valign: "middle" });

  const rightQ = [
    "Summarise Mathew Little's complaint",
    "Why was dispute DSP00047 lost?",
    "Show high priority complaints — Leeds",
    "What did the customer claim in this case?",
  ];
  rightQ.forEach((q, i) => {
    s.addShape(pres.shapes.RECTANGLE, { x: 5.2, y: 1.3 + i * 0.82, w: 4.4, h: 0.72, fill: { color: WHITE }, line: { color: "c0d4f8", width: 1 } });
    s.addText(q, { x: 5.35, y: 1.3 + i * 0.82, w: 4.1, h: 0.72, fontSize: 13, color: GRAY3, valign: "middle" });
  });
}

// ══════════════════════════════════════════════════════════════════════════════
// SLIDE 8 — White bg, "Cost"
// ══════════════════════════════════════════════════════════════════════════════
{
  const s = pres.addSlide();
  s.background = { color: WHITE };

  // Top accent bar
  s.addShape(pres.shapes.RECTANGLE, { x: 0, y: 0, w: 10, h: 0.55, fill: { color: NAVY }, line: { color: NAVY, width: 0 } });
  s.addText("Total Cost to Run This POC", { x: 0.5, y: 0.12, w: 9, h: 0.35, fontSize: 22, bold: true, color: WHITE, margin: 0 });

  // Big highlight box
  s.addShape(pres.shapes.RECTANGLE, { x: 2, y: 0.75, w: 6, h: 1.1, fill: { color: NAVY }, line: { color: NAVY, width: 0 } });
  s.addText("~$1 / month", { x: 2, y: 0.82, w: 6, h: 0.52, fontSize: 40, bold: true, color: WHITE, align: "center" });
  s.addText("running cost at 100 questions/day", { x: 2, y: 1.35, w: 6, h: 0.35, fontSize: 14, color: LBLUE, align: "center" });

  // Cost rows
  const costRows = [
    { icon: "☁️",  service: "AWS Bedrock — Nova Lite LLM",  cost: "~$0.001 per question",  fill: "f8faff" },
    { icon: "🗄️",  service: "S3 Storage — 1,000 PDFs",      cost: "$0.00 — free tier",      fill: WHITE },
    { icon: "📊",  service: "ClickHouse Cloud",              cost: "$0.00 — free tier",      fill: "f8faff" },
    { icon: "🌐",  service: "Streamlit Community Cloud",     cost: "$0.00 — free tier",      fill: WHITE },
    { icon: "📦",  service: "ChromaDB (local index)",        cost: "$0.00 — open source",    fill: "f8faff" },
  ];
  costRows.forEach((r, i) => {
    const ry = 2.1 + i * 0.52;
    s.addShape(pres.shapes.RECTANGLE, { x: 0.5, y: ry, w: 9, h: 0.5, fill: { color: r.fill }, line: { color: "dde8f0", width: 1 } });
    s.addText(r.icon + "  " + r.service, { x: 0.7, y: ry + 0.06, w: 6, h: 0.38, fontSize: 14, bold: true, color: NAVY });
    s.addText(r.cost, { x: 6.5, y: ry + 0.06, w: 2.8, h: 0.38, fontSize: 14, bold: true, color: "116633", align: "right" });
  });

  // Bottom box
  s.addShape(pres.shapes.RECTANGLE, { x: 1, y: 4.7, w: 8, h: 0.4, fill: { color: "edfaf2" }, line: { color: GREEN, width: 1 } });
  s.addText("POC total budget spent: $0", { x: 1, y: 4.7, w: 8, h: 0.4, fontSize: 15, bold: true, color: "116633", align: "center", valign: "middle" });
}

// ══════════════════════════════════════════════════════════════════════════════
// SLIDE 9 — White bg, "Production Path"
// ══════════════════════════════════════════════════════════════════════════════
{
  const s = pres.addSlide();
  s.background = { color: WHITE };

  // Top accent bar
  s.addShape(pres.shapes.RECTANGLE, { x: 0, y: 0, w: 10, h: 0.55, fill: { color: NAVY }, line: { color: NAVY, width: 0 } });
  s.addText("Production Ready — One Swap Per Component", { x: 0.5, y: 0.12, w: 9, h: 0.35, fontSize: 20, bold: true, color: WHITE, margin: 0 });

  const swapCards = [
    { x: 0.25, y: 0.75, tag: "📁 Storage",   title: "AWS S3  →  IBM FileNet P8",         sub: "FileNet REST/CMIS replaces boto3. Zero chunking changes." },
    { x: 5.25, y: 0.75, tag: "🤖 LLM",        title: "Nova Lite  →  Claude Haiku",        sub: "One line config change. Better reasoning." },
    { x: 0.25, y: 2.45, tag: "📦 Vector DB",  title: "ChromaDB  →  pgvector on RDS",      sub: "Same LangChain interface. No chain changes." },
    { x: 5.25, y: 2.45, tag: "🌐 Frontend",   title: "Streamlit  →  FastAPI + React",     sub: "Same RAG chain backend. Just swap the UI layer." },
  ];

  for (const c of swapCards) {
    s.addShape(pres.shapes.RECTANGLE, { x: c.x, y: c.y, w: 4.5, h: 1.5, fill: { color: "fff8ee" }, line: { color: "f0d8a8", width: 1 } });
    s.addText(c.tag, { x: c.x + 0.15, y: c.y + 0.1, w: 2, h: 0.28, fontSize: 11, bold: true, color: "884400" });
    s.addText(c.title, { x: c.x + 0.15, y: c.y + 0.42, w: 4.2, h: 0.48, fontSize: 16, bold: true, color: NAVY, wrap: true });
    s.addText(c.sub, { x: c.x + 0.15, y: c.y + 0.95, w: 4.2, h: 0.45, fontSize: 12, color: GRAY1, wrap: true });
  }

  // Bottom bar
  s.addShape(pres.shapes.RECTANGLE, { x: 0.25, y: 4.2, w: 9.5, h: 0.75, fill: { color: "f0f4f9" }, line: { color: "d0dce8", width: 1 } });
  s.addText("✅ Stays identical: chunking · Titan embeddings · query router · prompts · citations · audit logging", {
    x: 0.25, y: 4.2, w: 9.5, h: 0.75, fontSize: 13, color: "334466", align: "center", valign: "middle"
  });
}

// ══════════════════════════════════════════════════════════════════════════════
// SLIDE 10 — Dark navy, CTA
// ══════════════════════════════════════════════════════════════════════════════
{
  const s = pres.addSlide();
  s.background = { color: NAVY };

  s.addText("🚀 Live Demo Available", { x: 0, y: 0.4, w: 10, h: 0.6, fontSize: 36, bold: true, color: WHITE, align: "center" });
  s.addText("Password protected · DM me to access", { x: 0, y: 1.1, w: 10, h: 0.38, fontSize: 16, color: LBLUE, align: "center" });

  // CTA boxes
  const ctaBoxes = [
    { x: 0.35,  fill: "0044aa", border: "2266cc", icon: "💬", title: "DM for password",   sub: "Get instant access" },
    { x: 3.6,   fill: "116633", border: "33aa66", icon: "⭐",  title: "GitHub Repo",       sub: "dpanwar-vigyan/securebank-ai-poc" },
    { x: 6.85,  fill: "884400", border: "cc7700", icon: "🏗️",  title: "Architecture",      sub: "Diagrams in comments" },
  ];

  for (const b of ctaBoxes) {
    s.addShape(pres.shapes.RECTANGLE, { x: b.x, y: 1.7, w: 2.8, h: 1.4, fill: { color: b.fill }, line: { color: b.border, width: 1 } });
    s.addText(b.icon, { x: b.x, y: 1.78, w: 2.8, h: 0.5, fontSize: 30, align: "center" });
    s.addText(b.title, { x: b.x + 0.1, y: 2.3, w: 2.6, h: 0.35, fontSize: 14, bold: true, color: WHITE, align: "center" });
    s.addText(b.sub, { x: b.x + 0.05, y: 2.65, w: 2.7, h: 0.35, fontSize: 11, color: LBLUE, align: "center", wrap: true });
  }

  // Divider
  s.addShape(pres.shapes.RECTANGLE, { x: 0.5, y: 3.35, w: 9, h: 0.02, fill: { color: WHITE }, line: { color: WHITE, width: 0 } });

  // Stat pills
  const pills = ["1,000 docs", "3,948 chunks", "256-dim embeddings", "~$1/month"];
  pills.forEach((p, i) => {
    const px = 0.4 + i * 2.3;
    s.addShape(pres.shapes.ROUNDED_RECTANGLE, {
      x: px, y: 3.55, w: 2.1, h: 0.38,
      fill: { color: "1a3a66" },
      line: { color: "4477aa", width: 1 },
      rectRadius: 0.08
    });
    s.addText(p, { x: px, y: 3.55, w: 2.1, h: 0.38, fontSize: 13, color: WHITE, align: "center", valign: "middle" });
  });

  // Hashtags
  s.addText("#BankingAI #RAG #AWSBedrock #ClickHouse #LangChain #Fintech #AIDemo #GenAI", {
    x: 0, y: 4.85, w: 10, h: 0.3, fontSize: 11, color: LBLUE, align: "center"
  });
}

// ─── Write file ───────────────────────────────────────────────────────────────
const OUTPUT = "/Users/dineshsinghpanwar/banking-poc/linkedin/securebank-ai-linkedin.pptx";
pres.writeFile({ fileName: OUTPUT })
  .then(() => console.log("✅  Saved:", OUTPUT))
  .catch(e => { console.error("❌  Error:", e); process.exit(1); });
