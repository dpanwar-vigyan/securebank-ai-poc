"use strict";
/**
 * QA preview generator — renders each slide as 1920x1080 PNG using node-canvas.
 * Coordinates are in inches (matching pptxgenjs).
 * Font sizes are in POINTS (matching pptxgenjs).
 */
const { createCanvas } = require("canvas");
const fs = require("fs");
const path = require("path");

const OUT = "/Users/dineshsinghpanwar/banking-poc/linkedin/qa_images";
fs.mkdirSync(OUT, { recursive: true });

const SLIDE_W_IN = 10;
const SLIDE_H_IN = 5.63;
const CANVAS_W = 1920;
const CANVAS_H = 1080;
const PPI = CANVAS_W / SLIDE_W_IN;  // 192 px/inch

// Convert inches → canvas pixels
function ix(inches) { return Math.round(inches * PPI); }
// Convert points → canvas pixels (72pt = 1 inch)
function pt(points) { return Math.round(points * PPI / 72); }

// Colors
const NAVY   = "#003366";
const BLUE   = "#0055a5";
const GREEN  = "#00aa66";
const WHITE  = "#ffffff";
const LBLUE  = "#aaccee";
const DARK1  = "#1a2540";
const GRAY1  = "#556677";
const GRAY2  = "#8899aa";
const GRAY3  = "#334455";
const ACCENT = "#cc3333";

function makeCanvas() {
  const c = createCanvas(CANVAS_W, CANVAS_H);
  const ctx = c.getContext("2d");
  return { c, ctx };
}

function fillBg(ctx, color) {
  ctx.fillStyle = color;
  ctx.fillRect(0, 0, CANVAS_W, CANVAS_H);
}

/** Draw a filled/stroked rectangle. Coords in inches. */
function rect(ctx, x, y, w, h, fillColor, strokeColor, lineW = 1) {
  ctx.save();
  if (fillColor) {
    ctx.fillStyle = fillColor;
    ctx.fillRect(ix(x), ix(y), ix(w), ix(h));
  }
  if (strokeColor) {
    ctx.strokeStyle = strokeColor;
    ctx.lineWidth = lineW;
    ctx.strokeRect(ix(x) + 0.5, ix(y) + 0.5, ix(w) - 1, ix(h) - 1);
  }
  ctx.restore();
}

/**
 * Draw text. x, y, w, h in inches. fontSize in POINTS.
 * opts: { fontSize, color, bold, align, valign, wrap, fontFace }
 */
function drawText(ctx, str, x, y, w, h, opts = {}) {
  const {
    fontSize = 14, color = WHITE, bold = false,
    align = "left", valign = "top",
    wrap = false, fontFace = "Arial"
  } = opts;

  ctx.save();
  const weight = bold ? "bold" : "normal";
  const pxSize = pt(fontSize);
  ctx.font = `${weight} ${pxSize}px "${fontFace}", sans-serif`;
  ctx.fillStyle = color;

  const bx = ix(x), by = ix(y), bw = ix(w), bh = h ? ix(h) : CANVAS_H;
  const lineH = pxSize * 1.25;

  // Build lines
  let lines = [];
  const rawLines = String(str).split("\n");
  for (const rawLine of rawLines) {
    if (wrap && bw > 0) {
      const words = rawLine.split(" ");
      let line = "";
      for (const word of words) {
        const test = line ? line + " " + word : word;
        if (ctx.measureText(test).width > bw - 4 && line) {
          lines.push(line);
          line = word;
        } else {
          line = test;
        }
      }
      if (line) lines.push(line);
    } else {
      lines.push(rawLine);
    }
  }

  const totalH = lines.length * lineH;
  let startY = by;
  if (valign === "middle" && bh) startY = by + (bh - totalH) / 2;
  if (valign === "bottom" && bh) startY = by + bh - totalH;

  for (let i = 0; i < lines.length; i++) {
    let drawX = bx;
    if (align === "center") drawX = bx + bw / 2;
    if (align === "right")  drawX = bx + bw;

    ctx.textAlign = align;
    ctx.textBaseline = "top";
    ctx.fillText(lines[i], drawX, startY + i * lineH);
  }
  ctx.restore();
}

function hline(ctx, x, y, w, color = WHITE, lw = 2) {
  ctx.save();
  ctx.strokeStyle = color;
  ctx.lineWidth = lw;
  ctx.beginPath();
  ctx.moveTo(ix(x), ix(y));
  ctx.lineTo(ix(x + w), ix(y));
  ctx.stroke();
  ctx.restore();
}

function diagLine(ctx, x1, y1, x2, y2, color, lw = 2) {
  ctx.save();
  ctx.strokeStyle = color;
  ctx.lineWidth = lw;
  ctx.beginPath();
  ctx.moveTo(ix(x1), ix(y1));
  ctx.lineTo(ix(x2), ix(y2));
  ctx.stroke();
  ctx.restore();
}

function roundRect(ctx, x, y, w, h, r, fillColor, strokeColor, lw = 1) {
  ctx.save();
  ctx.beginPath();
  const rx = ix(x), ry = ix(y), rw = ix(w), rh = ix(h), rr = ix(r);
  ctx.roundRect(rx, ry, rw, rh, rr);
  if (fillColor) { ctx.fillStyle = fillColor; ctx.fill(); }
  if (strokeColor) { ctx.strokeStyle = strokeColor; ctx.lineWidth = lw; ctx.stroke(); }
  ctx.restore();
}

function topBar(ctx, title) {
  rect(ctx, 0, 0, 10, 0.55, NAVY, null);
  drawText(ctx, title, 0.5, 0.09, 9, 0.38, { fontSize: 22, bold: true, color: WHITE, valign: "middle" });
}

function saveSlide(canvas, n) {
  const outPath = path.join(OUT, `slide${String(n).padStart(2, "0")}.png`);
  fs.writeFileSync(outPath, canvas.toBuffer("image/png"));
  console.log(`  Saved: slide${String(n).padStart(2,"0")}.png`);
}

// ══════════════════════════════════════════════════════════════════════════════
// SLIDE 1 — Dark navy
// ══════════════════════════════════════════════════════════════════════════════
{
  const { c, ctx } = makeCanvas();
  fillBg(ctx, NAVY);

  drawText(ctx, "WEEKEND PROJECT", 0.5, 0.3, 4, 0.28, { fontSize: 11, color: WHITE });
  drawText(ctx, "I Built a Banking AI Document Assistant in a Weekend",
    0.5, 0.85, 9, 1.1, { fontSize: 38, bold: true, color: WHITE, wrap: true });
  drawText(ctx, "1,000 banking documents. Natural language search. AWS Bedrock + ClickHouse + ChromaDB RAG.",
    0.5, 2.15, 9, 0.5, { fontSize: 17, color: LBLUE, wrap: true });

  hline(ctx, 0.5, 3.1, 9, WHITE, 2);

  const stats = [
    { x: 0.5,  big: "1,000",     small: "Documents" },
    { x: 3.6,  big: "3,948",     small: "Vector Chunks" },
    { x: 6.7,  big: "2 Engines", small: "ClickHouse + ChromaDB" },
  ];
  for (const s of stats) {
    rect(ctx, s.x, 3.3, 2.8, 0.9, "#0a2244", "#1a4488", 1);
    drawText(ctx, s.big,   s.x, 3.35, 2.8, 0.48, { fontSize: 32, bold: true, color: WHITE, align: "center", valign: "middle" });
    drawText(ctx, s.small, s.x, 3.82, 2.8, 0.28, { fontSize: 12, color: LBLUE, align: "center", valign: "middle" });
  }

  drawText(ctx, "#AI  #Fintech  #AWS  #RAG  #Banking", 0.5, 5.08, 9, 0.28, { fontSize: 11, color: LBLUE });
  saveSlide(c, 1);
}

// ══════════════════════════════════════════════════════════════════════════════
// SLIDE 2 — White bg, "The Problem"
// ══════════════════════════════════════════════════════════════════════════════
{
  const { c, ctx } = makeCanvas();
  fillBg(ctx, WHITE);
  topBar(ctx, "The Problem Bankers Face Every Day");

  const cards = [
    { y: 0.72, bc: ACCENT,    icon: "📁", title: "Thousands of PDFs — impossible to search",  sub: "Disputes, complaints, statements all siloed in different folders" },
    { y: 2.07, bc: "#ff8800", icon: "⏱", title: "Finding one case = opening 20 documents",   sub: "No keyword search, no metadata filter, no way to query across docs" },
    { y: 3.42, bc: BLUE,      icon: "📊", title: "Analytics questions need a data team",      sub: "How many complaints per branch this year? No one knows without SQL" },
  ];
  for (const card of cards) {
    rect(ctx, 0.5, card.y, 9, 1.15, "#f8fafd", "#dde8f0", 1);
    rect(ctx, 0.5, card.y, 0.1, 1.15, card.bc, null);
    drawText(ctx, card.icon, 0.72, card.y + 0.1, 0.7, 0.7, { fontSize: 26, valign: "middle" });
    drawText(ctx, card.title, 1.55, card.y + 0.09, 7.8, 0.44, { fontSize: 17, bold: true, color: DARK1, wrap: true });
    drawText(ctx, card.sub,   1.55, card.y + 0.58, 7.8, 0.48, { fontSize: 13, color: GRAY1, wrap: true });
  }
  saveSlide(c, 2);
}

// ══════════════════════════════════════════════════════════════════════════════
// SLIDE 3 — White bg, "The Solution"
// ══════════════════════════════════════════════════════════════════════════════
{
  const { c, ctx } = makeCanvas();
  fillBg(ctx, WHITE);
  topBar(ctx, "What I Built");

  rect(ctx, 2.5, 0.72, 5, 1.1, NAVY, null);
  drawText(ctx, "1 question",       2.5, 0.78, 5, 0.5, { fontSize: 30, bold: true, color: WHITE, align: "center", valign: "middle" });
  drawText(ctx, "→ instant answer", 2.5, 1.3,  5, 0.4, { fontSize: 16, color: LBLUE, align: "center", valign: "middle" });

  const fCards = [
    { x: 0.5, y: 2.0,  fill: "#edf5ff", bdr: BLUE,  icon: "💬 ", title: "Ask in plain English",      sub: "No SQL. No training required." },
    { x: 5.2, y: 2.0,  fill: "#edf5ff", bdr: BLUE,  icon: "📚 ", title: "Searches 1,000 Documents",  sub: "Every PDF. Every chunk." },
    { x: 0.5, y: 3.25, fill: "#edfaf2", bdr: GREEN, icon: "📎 ", title: "Cites Source Documents",     sub: "With links back to PDFs" },
    { x: 5.2, y: 3.25, fill: "#edfaf2", bdr: GREEN, icon: "📊 ", title: "Aggregates All Data",        sub: "Counts, trends, breakdowns" },
  ];
  for (const fc of fCards) {
    rect(ctx, fc.x, fc.y, 4.3, 1.1, fc.fill, fc.bdr, 1);
    drawText(ctx, fc.icon + fc.title, fc.x + 0.18, fc.y + 0.1, 3.9, 0.5, { fontSize: 15, bold: true, color: NAVY, wrap: true });
    drawText(ctx, fc.sub, fc.x + 0.18, fc.y + 0.65, 3.9, 0.36, { fontSize: 13, color: GRAY1 });
  }

  drawText(ctx, "Powered by AWS Bedrock · ClickHouse Cloud · ChromaDB · LangChain · Streamlit",
    0.5, 4.88, 9, 0.3, { fontSize: 11, color: GRAY2, align: "center" });
  saveSlide(c, 3);
}

// ══════════════════════════════════════════════════════════════════════════════
// SLIDE 4 — White bg, "Architecture"
// ══════════════════════════════════════════════════════════════════════════════
{
  const { c, ctx } = makeCanvas();
  fillBg(ctx, WHITE);
  topBar(ctx, "Smart Dual-Engine Architecture");

  rect(ctx, 3.5, 0.68, 3, 0.72, "#fff8ee", "#ff9900", 2);
  drawText(ctx, "Query Router",         3.5, 0.72, 3, 0.32, { fontSize: 14, bold: true, color: "#884400", align: "center", valign: "middle" });
  drawText(ctx, "automatic intent detection", 3.5, 1.05, 3, 0.28, { fontSize: 11, color: "#aa6600", align: "center" });

  // Connector lines router → engines
  diagLine(ctx, 4.2, 1.4, 2.5, 1.85, GREEN, 2);
  diagLine(ctx, 5.8, 1.4, 7.5, 1.85, BLUE,  2);

  // LEFT engine
  rect(ctx, 0.3, 1.65, 4.3, 3.5, "#f0fdf6", GREEN, 2);
  rect(ctx, 0.3, 1.65, 4.3, 0.48, GREEN, null);
  drawText(ctx, "Aggregation", 0.3, 1.68, 4.3, 0.42, { fontSize: 16, bold: true, color: WHITE, align: "center", valign: "middle" });

  const leftItems = ["How many disputes per branch?", "Which RM handled most cases?", "Total compensation by branch?"];
  leftItems.forEach((t, i) => {
    drawText(ctx, t, 0.5, 2.22 + i * 0.42, 3.9, 0.38, { fontSize: 13, color: "#116633" });
  });
  hline(ctx, 0.5, 3.52, 3.9, "#c0ddc8", 1);
  drawText(ctx, "→ ClickHouse NL→SQL",         0.5, 3.62, 3.9, 0.36, { fontSize: 14, bold: true, color: NAVY });
  drawText(ctx, "Scans ALL 1,000 docs. No limits.", 0.5, 3.98, 3.9, 0.34, { fontSize: 12, color: GRAY1 });

  // RIGHT engine
  rect(ctx, 5.4, 1.65, 4.3, 3.5, "#f0f6ff", BLUE, 2);
  rect(ctx, 5.4, 1.65, 4.3, 0.48, BLUE, null);
  drawText(ctx, "Content Search", 5.4, 1.68, 4.3, 0.42, { fontSize: 16, bold: true, color: WHITE, align: "center", valign: "middle" });

  const rightItems = ["Summarise this complaint", "Why was DSP00047 lost?", "What did the customer say?"];
  rightItems.forEach((t, i) => {
    drawText(ctx, t, 5.6, 2.22 + i * 0.42, 3.9, 0.38, { fontSize: 13, color: "#0044aa" });
  });
  hline(ctx, 5.6, 3.52, 3.9, "#c0d4f8", 1);
  drawText(ctx, "→ ChromaDB Vector RAG",        5.6, 3.62, 3.9, 0.36, { fontSize: 14, bold: true, color: NAVY });
  drawText(ctx, "3,948 chunks · 256-dim embeddings", 5.6, 3.98, 3.9, 0.34, { fontSize: 12, color: GRAY1 });

  saveSlide(c, 4);
}

// ══════════════════════════════════════════════════════════════════════════════
// SLIDE 5 — Dark navy, "Tech Stack"
// ══════════════════════════════════════════════════════════════════════════════
{
  const { c, ctx } = makeCanvas();
  fillBg(ctx, NAVY);

  drawText(ctx, "Full Tech Stack", 0.5, 0.25, 9, 0.5, { fontSize: 28, bold: true, color: WHITE });
  drawText(ctx, "Everything used to build this POC", 0.5, 0.82, 9, 0.32, { fontSize: 14, color: LBLUE });

  const rows = [
    { y: 1.25, label: "AWS",  bg: "#0d2a4a", bc: "#2266aa", pills: ["AWS S3", "AWS Bedrock", "Amazon Nova Lite", "Titan Embeddings v2"] },
    { y: 2.3,  label: "DATA", bg: "#0a2a1a", bc: GREEN,     pills: ["ClickHouse Cloud", "ReplacingMergeTree", "NL→SQL Pipeline", "ChromaDB"] },
    { y: 3.35, label: "DEV",  bg: "#1a0a3a", bc: "#7744cc", pills: ["LangChain", "Python 3.9", "Streamlit Cloud", "pdfplumber"] },
    { y: 4.4,  label: "PROD", bg: "#2a1a0a", bc: "#cc8800", pills: ["IBM FileNet P8", "pgvector on RDS", "Claude Haiku"] },
  ];
  for (const row of rows) {
    drawText(ctx, row.label, 0.4, row.y + 0.04, 1.1, 0.36, { fontSize: 11, bold: true, color: LBLUE });
    row.pills.forEach((pill, i) => {
      const px2 = 1.65 + i * 2.1;
      roundRect(ctx, px2, row.y, 2.0, 0.38, 0.07, row.bg, row.bc, 1);
      drawText(ctx, pill, px2, row.y, 2.0, 0.38, { fontSize: 11, color: WHITE, align: "center", valign: "middle" });
    });
  }
  saveSlide(c, 5);
}

// ══════════════════════════════════════════════════════════════════════════════
// SLIDE 6 — White bg, "Pipeline"
// ══════════════════════════════════════════════════════════════════════════════
{
  const { c, ctx } = makeCanvas();
  fillBg(ctx, WHITE);
  topBar(ctx, "Real-Time Dual-Write Pipeline");

  // S3 box
  rect(ctx, 0.3, 0.75, 1.8, 0.8, "#edf5ff", BLUE, 1);
  drawText(ctx, "S3 PDF", 0.3, 0.75, 1.8, 0.8, { fontSize: 13, bold: true, color: NAVY, align: "center", valign: "middle" });

  // Arrow
  hline(ctx, 2.1, 1.15, 0.35, GRAY1, 2);

  // ingest.py box
  rect(ctx, 2.5, 0.75, 2.0, 0.8, "#fff8ee", "#ff9900", 1);
  drawText(ctx, "ingest.py", 2.5, 0.75, 2.0, 0.8, { fontSize: 13, bold: true, color: "#884400", align: "center", valign: "middle" });

  // Split lines
  diagLine(ctx, 2.5, 1.55, 1.6, 2.0, "#7744cc", 2);
  diagLine(ctx, 4.0, 1.55, 7.3, 2.0, GREEN, 2);

  // ChromaDB box
  rect(ctx, 0.5, 2.0, 3.5, 0.9, "#f6eeff", "#7744cc", 1);
  drawText(ctx, "ChromaDB",                  0.5, 2.05, 3.5, 0.38, { fontSize: 13, bold: true, color: "#552288", align: "center" });
  drawText(ctx, "Content search  3,948 chunks", 0.5, 2.48, 3.5, 0.36, { fontSize: 11, color: "#776699", align: "center" });

  // ClickHouse box
  rect(ctx, 5.5, 2.0, 3.5, 0.9, "#edfaf2", GREEN, 1);
  drawText(ctx, "ClickHouse",                     5.5, 2.05, 3.5, 0.38, { fontSize: 13, bold: true, color: "#116633", align: "center" });
  drawText(ctx, "Aggregation  1,000 docs  FINAL", 5.5, 2.48, 3.5, 0.36, { fontSize: 11, color: "#338855", align: "center" });

  // Stat boxes
  const s6 = [
    { val: "3,948", color: BLUE,      label: "chunks indexed" },
    { val: "1,000", color: GREEN,     label: "docs in ClickHouse" },
    { val: "Safe",  color: "#ff9900", label: "to re-run anytime" },
    { val: "Auto",  color: "#7744cc", label: "deduplicates" },
  ];
  s6.forEach((s, i) => {
    const sx = 0.3 + i * 2.35;
    rect(ctx, sx, 3.1, 2.1, 0.88, "#f8faff", "#dde8f0", 1);
    drawText(ctx, s.val,   sx, 3.14, 2.1, 0.46, { fontSize: 26, bold: true, color: s.color, align: "center", valign: "middle" });
    drawText(ctx, s.label, sx, 3.6,  2.1, 0.3,  { fontSize: 11, color: GRAY1, align: "center" });
  });

  drawText(ctx, "BEFORE: manual CSV → import (goes stale)", 0.3, 4.2, 4.7, 0.32, { fontSize: 12, color: "#cc3333" });
  drawText(ctx, "NOW: ./run.sh ingest → both stores sync",  5.1, 4.2, 4.6, 0.32, { fontSize: 12, bold: true, color: "#116633" });
  saveSlide(c, 6);
}

// ══════════════════════════════════════════════════════════════════════════════
// SLIDE 7 — White bg, "Example Queries"
// ══════════════════════════════════════════════════════════════════════════════
{
  const { c, ctx } = makeCanvas();
  fillBg(ctx, WHITE);
  topBar(ctx, "Questions It Can Answer Right Now");

  // Left header
  rect(ctx, 0.4, 0.68, 4.4, 0.52, "#edfaf2", GREEN, 1);
  drawText(ctx, "Aggregation  →  ClickHouse", 0.4, 0.68, 4.4, 0.52, { fontSize: 15, bold: true, color: "#116633", align: "center", valign: "middle" });

  const lq = [
    "How many complaints raised each year?",
    "Which branch had the most disputes?",
    "Total compensation paid per RM?",
    "Cases referred to CFPB by year?",
  ];
  lq.forEach((q, i) => {
    rect(ctx, 0.4, 1.3 + i * 0.84, 4.4, 0.74, WHITE, "#c0ddc8", 1);
    drawText(ctx, q, 0.55, 1.3 + i * 0.84, 4.1, 0.74, { fontSize: 13, color: GRAY3, valign: "middle", wrap: true });
  });

  // Right header
  rect(ctx, 5.2, 0.68, 4.4, 0.52, "#edf5ff", BLUE, 1);
  drawText(ctx, "Content  →  ChromaDB RAG", 5.2, 0.68, 4.4, 0.52, { fontSize: 15, bold: true, color: "#0044aa", align: "center", valign: "middle" });

  const rq = [
    "Summarise Mathew Little's complaint",
    "Why was dispute DSP00047 lost?",
    "Show high priority complaints — Leeds",
    "What did the customer claim in this case?",
  ];
  rq.forEach((q, i) => {
    rect(ctx, 5.2, 1.3 + i * 0.84, 4.4, 0.74, WHITE, "#c0d4f8", 1);
    drawText(ctx, q, 5.35, 1.3 + i * 0.84, 4.1, 0.74, { fontSize: 13, color: GRAY3, valign: "middle", wrap: true });
  });
  saveSlide(c, 7);
}

// ══════════════════════════════════════════════════════════════════════════════
// SLIDE 8 — White bg, "Cost"
// ══════════════════════════════════════════════════════════════════════════════
{
  const { c, ctx } = makeCanvas();
  fillBg(ctx, WHITE);
  topBar(ctx, "Total Cost to Run This POC");

  rect(ctx, 2, 0.72, 6, 1.12, NAVY, null);
  drawText(ctx, "~$1 / month",                       2, 0.76, 6, 0.58, { fontSize: 38, bold: true, color: WHITE, align: "center", valign: "middle" });
  drawText(ctx, "running cost at 100 questions/day", 2, 1.35, 6, 0.36, { fontSize: 14, color: LBLUE, align: "center" });

  const r8 = [
    { icon: "AWS Bedrock", service: "AWS Bedrock — Nova Lite LLM", cost: "~$0.001 per question", fill: "#f8faff" },
    { icon: "S3 Storage",  service: "S3 Storage — 1,000 PDFs",    cost: "$0.00 — free tier",     fill: WHITE },
    { icon: "ClickHouse",  service: "ClickHouse Cloud",            cost: "$0.00 — free tier",     fill: "#f8faff" },
    { icon: "Streamlit",   service: "Streamlit Community Cloud",   cost: "$0.00 — free tier",     fill: WHITE },
    { icon: "ChromaDB",    service: "ChromaDB (local index)",      cost: "$0.00 — open source",   fill: "#f8faff" },
  ];
  r8.forEach((r, i) => {
    const ry = 2.08 + i * 0.51;
    rect(ctx, 0.5, ry, 9, 0.49, r.fill, "#dde8f0", 1);
    drawText(ctx, r.service, 0.7, ry, 6.5, 0.49, { fontSize: 14, bold: true, color: NAVY, valign: "middle" });
    drawText(ctx, r.cost, 6.8, ry, 2.5, 0.49, { fontSize: 14, bold: true, color: "#116633", align: "right", valign: "middle" });
  });

  rect(ctx, 1, 4.68, 8, 0.42, "#edfaf2", GREEN, 1);
  drawText(ctx, "POC total budget spent: $0", 1, 4.68, 8, 0.42, { fontSize: 15, bold: true, color: "#116633", align: "center", valign: "middle" });
  saveSlide(c, 8);
}

// ══════════════════════════════════════════════════════════════════════════════
// SLIDE 9 — White bg, "Production Path"
// ══════════════════════════════════════════════════════════════════════════════
{
  const { c, ctx } = makeCanvas();
  fillBg(ctx, WHITE);
  topBar(ctx, "Production Ready — One Swap Per Component");

  const sw = [
    { x: 0.25, y: 0.72, tag: "Storage",  title: "AWS S3  →  IBM FileNet P8",      sub: "FileNet REST/CMIS replaces boto3. Zero chunking changes." },
    { x: 5.25, y: 0.72, tag: "LLM",      title: "Nova Lite  →  Claude Haiku",     sub: "One line config change. Better reasoning." },
    { x: 0.25, y: 2.42, tag: "Vector DB", title: "ChromaDB  →  pgvector on RDS",  sub: "Same LangChain interface. No chain changes." },
    { x: 5.25, y: 2.42, tag: "Frontend", title: "Streamlit  →  FastAPI + React",  sub: "Same RAG chain backend. Just swap the UI layer." },
  ];
  for (const sc of sw) {
    rect(ctx, sc.x, sc.y, 4.5, 1.5, "#fff8ee", "#f0d8a8", 1);
    drawText(ctx, sc.tag,   sc.x + 0.15, sc.y + 0.1,  4.2, 0.28, { fontSize: 11, bold: true, color: "#884400" });
    drawText(ctx, sc.title, sc.x + 0.15, sc.y + 0.42, 4.2, 0.5,  { fontSize: 15, bold: true, color: NAVY, wrap: true });
    drawText(ctx, sc.sub,   sc.x + 0.15, sc.y + 0.95, 4.2, 0.46, { fontSize: 12, color: GRAY1, wrap: true });
  }

  rect(ctx, 0.25, 4.18, 9.5, 0.75, "#f0f4f9", "#d0dce8", 1);
  drawText(ctx, "Stays identical: chunking  ·  Titan embeddings  ·  query router  ·  prompts  ·  citations  ·  audit logging",
    0.25, 4.18, 9.5, 0.75, { fontSize: 13, color: "#334466", align: "center", valign: "middle", wrap: true });
  saveSlide(c, 9);
}

// ══════════════════════════════════════════════════════════════════════════════
// SLIDE 10 — Dark navy, CTA
// ══════════════════════════════════════════════════════════════════════════════
{
  const { c, ctx } = makeCanvas();
  fillBg(ctx, NAVY);

  drawText(ctx, "Live Demo Available",           0, 0.4,  10, 0.58, { fontSize: 34, bold: true, color: WHITE, align: "center", valign: "middle" });
  drawText(ctx, "Password protected  ·  DM me to access", 0, 1.08, 10, 0.38, { fontSize: 15, color: LBLUE, align: "center" });

  const ctas = [
    { x: 0.35,  fill: "#0044aa", bc: "#2266cc", title: "DM for password",  sub: "Get instant access" },
    { x: 3.6,   fill: "#116633", bc: "#33aa66", title: "GitHub Repo",      sub: "dpanwar-vigyan/securebank-ai-poc" },
    { x: 6.85,  fill: "#884400", bc: "#cc7700", title: "Architecture",     sub: "Diagrams in comments" },
  ];
  for (const b of ctas) {
    rect(ctx, b.x, 1.68, 2.8, 1.42, b.fill, b.bc, 1);
    drawText(ctx, b.title, b.x + 0.1, 1.9,  2.6, 0.38, { fontSize: 14, bold: true, color: WHITE, align: "center" });
    drawText(ctx, b.sub,   b.x + 0.1, 2.35, 2.6, 0.66, { fontSize: 11, color: LBLUE, align: "center", wrap: true });
  }

  hline(ctx, 0.5, 3.35, 9, WHITE, 2);

  const pills = ["1,000 docs", "3,948 chunks", "256-dim embeddings", "~$1/month"];
  pills.forEach((p, i) => {
    roundRect(ctx, 0.4 + i * 2.3, 3.52, 2.1, 0.4, 0.07, "#1a3a66", "#4477aa", 1);
    drawText(ctx, p, 0.4 + i * 2.3, 3.52, 2.1, 0.4, { fontSize: 13, color: WHITE, align: "center", valign: "middle" });
  });

  drawText(ctx, "#BankingAI  #RAG  #AWSBedrock  #ClickHouse  #LangChain  #Fintech  #AIDemo  #GenAI",
    0.3, 4.83, 9.4, 0.3, { fontSize: 11, color: LBLUE, align: "center" });
  saveSlide(c, 10);
}

console.log("\nAll 10 QA images saved to:", OUT);
