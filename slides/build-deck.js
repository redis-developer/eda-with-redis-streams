// Redis 2026-themed intro deck: Redis Streams, Kafka & Korvet.
// Uses the redis-slides brand system (Hyper Red / Midnight / Lime, Space Grotesk).
// Footer/title use the official Redis wordmark PNGs in assets/ (white on dark/red, red on light).
const pptxgen = require("pptxgenjs");
const path = require("path");

const OUT = process.argv[2] ||
  "/Users/jeremy.plichta/git/eda-with-redis-streams/slides/redis-streams-korvet-intro.pptx";
const ASSETS = path.join(__dirname, "assets");
const LOGO_WHITE = path.join(ASSETS, "redis-logo-white.png"); // for dark/red slides
const LOGO_RED = path.join(ASSETS, "redis-logo-red.png");     // for light slides
const LOGO_AR = 2048 / 641;                                   // official Redis wordmark aspect

const C = {
  RED: "FF4438", MIDNIGHT: "091A23", DUSK: "163341", DUSK30: "B9C2C6",
  LIME: "DCFF1E", SKY: "80DBFF", PURPLE: "C795E3", WHITE: "FFFFFF",
  OFFWHITE: "F2F2F2", MIDGRAY: "5C707A",
};
const F = { HEAD: "Space Grotesk", BODY: "Space Grotesk", CODE: "Space Mono" };

// Official Redis wordmark image: white on dark/red slides, red on light slides.
function logo(s, x, y, h, onDark) {
  s.addImage({ path: onDark ? LOGO_WHITE : LOGO_RED, x, y, w: h * LOGO_AR, h });
}

function footer(s, pageNum, onDark) {
  logo(s, 0.42, 5.16, 0.22, onDark);
  s.addText("© 2026 Redis Ltd. All rights reserved.", {
    x: 1.22, y: 5.20, w: 4.5, h: 0.20, fontSize: 6.5,
    color: onDark ? C.DUSK30 : C.MIDGRAY, fontFace: F.BODY, margin: 0,
  });
  if (pageNum) {
    s.addText(String(pageNum), {
      x: 9.55, y: 5.20, w: 0.35, h: 0.20, fontSize: 7,
      color: onDark ? C.DUSK30 : C.MIDGRAY, align: "right", fontFace: F.BODY, margin: 0,
    });
  }
}

function titleBlock(s, title, subtitle, onDark) {
  s.addText(title, {
    x: 0.42, y: 0.30, w: 9.2, h: 0.60, fontSize: 34,
    color: onDark ? C.WHITE : C.MIDNIGHT, fontFace: F.HEAD, margin: 0,
  });
  if (subtitle) {
    s.addText(subtitle, {
      x: 0.42, y: 0.92, w: 9.2, h: 0.30, fontSize: 14,
      color: onDark ? C.DUSK30 : C.MIDGRAY, fontFace: F.BODY, margin: 0,
    });
  }
}

// section: { header, body, bullets: [] }
function sections(s, list, startY, onDark) {
  let y = startY;
  list.forEach((sec) => {
    if (sec.header) {
      s.addText(sec.header, {
        x: 0.42, y, w: 9.2, h: 0.30, fontSize: 13, bold: true,
        color: onDark ? C.LIME : C.RED, fontFace: F.HEAD, margin: 0,
      });
      y += 0.32;
    }
    if (sec.body) {
      s.addText(sec.body, {
        x: 0.42, y, w: 9.2, h: 0.55, fontSize: 12.5, valign: "top",
        color: onDark ? C.WHITE : C.MIDNIGHT, fontFace: F.BODY, margin: 0,
      });
      y += 0.30 + 0.20 * Math.ceil(sec.body.length / 95);
    }
    if (sec.bullets) {
      const items = sec.bullets.map((b, i) => ({
        text: b, options: { bullet: { indent: 14 }, breakLine: i < sec.bullets.length - 1 },
      }));
      const h = sec.bullets.length * 0.30 + 0.05;
      s.addText(items, {
        x: 0.55, y, w: 8.9, h, fontSize: 12.5, valign: "top",
        color: onDark ? C.WHITE : C.MIDNIGHT, fontFace: F.BODY, margin: 0, lineSpacingMultiple: 1.1,
      });
      y += h + 0.10;
    }
    y += 0.12;
  });
}

function cardsDark(pres, s, cards) {
  const cardW = 2.86, gap = 0.28;
  const startX = (10 - (cards.length * cardW + (cards.length - 1) * gap)) / 2;
  const cy = 1.55, cardH = 3.05;
  cards.forEach((c, i) => {
    const cx = startX + i * (cardW + gap);
    s.addShape(pres.shapes.ROUNDED_RECTANGLE, {
      x: cx, y: cy, w: cardW, h: cardH, fill: { color: C.DUSK },
      line: { color: C.LIME, width: 1 }, rectRadius: 0.08,
    });
    s.addShape(pres.shapes.OVAL, {
      x: cx + 0.22, y: cy + 0.26, w: 0.12, h: 0.12,
      fill: { color: C.LIME }, line: { color: C.LIME },
    });
    s.addText(c.label.toUpperCase(), {
      x: cx + 0.42, y: cy + 0.20, w: cardW - 0.5, h: 0.26, fontSize: 9.5, bold: true,
      color: C.LIME, fontFace: F.HEAD, charSpacing: 1, margin: 0,
    });
    s.addText(c.body, {
      x: cx + 0.24, y: cy + 0.66, w: cardW - 0.48, h: cardH - 0.9, fontSize: 12.5,
      color: C.WHITE, fontFace: F.BODY, align: "left", valign: "top", margin: 0, lineSpacingMultiple: 1.15,
    });
  });
}

// A labelled box for the architecture diagram. o = { fill, stroke, title, sub?, tc? }
function archBox(pres, s, x, y, w, h, o) {
  s.addShape(pres.shapes.ROUNDED_RECTANGLE, {
    x, y, w, h, fill: { color: o.fill }, line: { color: o.stroke, width: 1.25 }, rectRadius: 0.05,
  });
  if (o.sub) {
    s.addText(o.title, {
      x, y: y + 0.10, w, h: 0.26, fontSize: 12, bold: true,
      color: o.tc || C.MIDNIGHT, align: "center", valign: "middle", fontFace: F.HEAD, margin: 0,
    });
    s.addText(o.sub, {
      x: x + 0.04, y: y + 0.37, w: w - 0.08, h: 0.22, fontSize: 8.5,
      color: C.MIDGRAY, align: "center", valign: "middle", fontFace: F.BODY, margin: 0,
    });
  } else {
    s.addText(o.title, {
      x, y, w, h, fontSize: 12, bold: true, color: o.tc || C.MIDNIGHT,
      align: "center", valign: "middle", fontFace: F.HEAD, margin: 0,
    });
  }
}

function archArrow(pres, s, x1, y1, x2, y2) {
  s.addShape(pres.shapes.LINE, {
    x: x1, y: y1, w: x2 - x1, h: y2 - y1,
    line: { color: "8896A0", width: 1.5, endArrowType: "triangle" },
  });
}

const pres = new pptxgen();
pres.layout = "LAYOUT_16x9";

/* ── Slide 1 — Title (red) ─────────────────────────────── */
(() => {
  const s = pres.addSlide();
  s.background = { color: C.RED };
  logo(s, 0.42, 0.40, 0.42, true);
  s.addText("Redis Streams, Kafka\n& Korvet", {
    x: 0.42, y: 1.75, w: 8.0, h: 1.7, fontSize: 44, color: C.WHITE,
    fontFace: F.HEAD, lineSpacingMultiple: 0.98, margin: 0,
  });
  s.addText("Event streaming — and how the same workload runs on Redis", {
    x: 0.42, y: 3.55, w: 8.0, h: 0.4, fontSize: 15, color: C.WHITE, fontFace: F.BODY, margin: 0,
  });
  s.addText("Jeremy Plichta  ·  July 2026", {
    x: 0.42, y: 4.05, w: 6.0, h: 0.35, fontSize: 13, color: C.WHITE, fontFace: F.BODY, margin: 0,
  });
  s.addText("© 2026 Redis Ltd. All rights reserved.", {
    x: 0.42, y: 5.22, w: 5.0, h: 0.20, fontSize: 6.5, color: C.WHITE,
    fontFace: F.BODY, transparency: 45, margin: 0,
  });
})();

/* ── Slide 2 — Hook: isn't Redis a cache? ──────────────── */
(() => {
  const s = pres.addSlide();
  s.background = { color: C.MIDNIGHT };
  s.addText("A QUICK MYTH TO BUST", { x: 0.42, y: 0.55, w: 9, h: 0.3, fontSize: 11, bold: true, color: C.LIME, charSpacing: 2, fontFace: F.HEAD, margin: 0 });
  s.addText("“Isn’t Redis just a cache?”", { x: 0.42, y: 1.05, w: 9.2, h: 0.95, fontSize: 40, color: C.DUSK30, fontFace: F.HEAD, margin: 0 });
  s.addText([
    { text: "Redis is a ", options: {} },
    { text: "data-structure server", options: { color: C.RED, bold: true } },
    { text: " — fast, in-memory, and far more than a cache.", options: {} },
  ], { x: 0.42, y: 2.2, w: 9.2, h: 0.95, fontSize: 26, color: C.WHITE, fontFace: F.HEAD, margin: 0, lineSpacingMultiple: 1.05 });
  s.addShape(pres.shapes.ROUNDED_RECTANGLE, { x: 0.42, y: 3.65, w: 9.16, h: 1.0, fill: { color: C.DUSK }, line: { color: C.DUSK }, rectRadius: 0.1 });
  s.addText("878K ops/sec  ·  ~1.3 ms latency", { x: 0.72, y: 3.78, w: 8.5, h: 0.34, fontSize: 18, bold: true, color: C.LIME, fontFace: F.HEAD, margin: 0 });
  s.addText("A European telecom runs real-time mobile signaling on Redis at this scale — serious real-time infrastructure, not a side cache.", { x: 0.72, y: 4.14, w: 8.5, h: 0.44, fontSize: 12.5, color: C.WHITE, fontFace: F.BODY, valign: "top", margin: 0, lineSpacingMultiple: 1.05 });
  footer(s, 2, true);
})();

/* ── Slide 3 — Data structures ─────────────────────────── */
(() => {
  const s = pres.addSlide();
  s.background = { color: C.WHITE };
  titleBlock(s, "Redis is a data-structure server", "Pick the right structure for the job — the Stream is one of them", false);
  const items = [
    ["Strings", "cache, counters", "0F7B8F"],
    ["Hashes", "objects & fields", "80DBFF"],
    ["Lists", "queues & stacks", "C795E3"],
    ["Sets", "membership, tags", "163341"],
    ["Sorted sets", "leaderboards, ranking", "0F7B8F"],
    ["Streams", "append-only event log", "FF4438"],
    ["JSON", "documents", "80DBFF"],
    ["Vectors", "similarity & AI search", "C795E3"],
    ["Geospatial", "location queries", "163341"],
    ["Time series", "metrics over time", "0F7B8F"],
    ["Pub/Sub", "fire-and-forget msgs", "80DBFF"],
    ["Probabilistic", "Bloom, HyperLogLog", "C795E3"],
  ];
  const cols = 4, gap = 0.2, x0 = 0.42, y0 = 1.5, tw = (9.16 - (cols - 1) * gap) / cols, th = 0.98, rgap = 0.16;
  items.forEach((it, i) => {
    const cx = x0 + (i % cols) * (tw + gap), cy = y0 + Math.floor(i / cols) * (th + rgap);
    const hot = it[0] === "Streams";
    s.addShape(pres.shapes.ROUNDED_RECTANGLE, { x: cx, y: cy, w: tw, h: th, fill: { color: hot ? C.RED : C.OFFWHITE }, line: { color: hot ? C.RED : C.DUSK30, width: hot ? 1.5 : 1 }, rectRadius: 0.06 });
    s.addShape(pres.shapes.OVAL, { x: cx + 0.18, y: cy + 0.19, w: 0.16, h: 0.16, fill: { color: hot ? C.WHITE : it[2] }, line: { color: hot ? C.WHITE : it[2] } });
    s.addText(it[0], { x: cx + 0.44, y: cy + 0.12, w: tw - 0.55, h: 0.3, fontSize: 13.5, bold: true, color: hot ? C.WHITE : C.MIDNIGHT, valign: "middle", fontFace: F.HEAD, margin: 0 });
    s.addText(it[1], { x: cx + 0.2, y: cy + 0.52, w: tw - 0.35, h: 0.36, fontSize: 10, color: hot ? C.WHITE : C.MIDGRAY, valign: "top", fontFace: F.BODY, margin: 0, lineSpacingMultiple: 1.02 });
  });
  s.addText("Today's talk lives in the red one — the Stream.", { x: 0.42, y: 4.96, w: 9.16, h: 0.25, fontSize: 12, italic: true, color: C.RED, fontFace: F.BODY, margin: 0 });
  footer(s, 3, false);
})();

/* ── Slide 4 — Redis Streams (dark cards) ──────────────── */
(() => {
  const s = pres.addSlide();
  s.background = { color: C.MIDNIGHT };
  titleBlock(s, "Redis Streams", "A first-class event log inside the Redis you already run", true);
  cardsDark(pres, s, [
    { label: "Append", body: "Add events with XADD. Every entry is time-ordered and gets a unique ID — an append-only log." },
    { label: "Fan-out", body: "Many consumer groups read the same stream independently with XREADGROUP. One log, many readers." },
    { label: "Replay & recover", body: "Each group tracks its own position, so consumers can fall behind, catch up, and replay history." },
  ]);
  footer(s, 4, true);
})();

/* ── Slide 5 — Redis Streams in the wild (stats) ───────── */
(() => {
  const s = pres.addSlide();
  s.background = { color: C.WHITE };
  titleBlock(s, "Redis Streams in the wild", "What teams run on Redis Streams — anonymized by industry + use case", false);
  const cards = [
    { value: "< 4 ms", value2: "Redis-tier latency", body: "A 100% Redis Streams platform for OPRA options & equities market data — sub-4 ms Redis latency inside a <100 ms end-to-end SLA, zero packet loss.", tag: "CAPITAL MARKETS · MARKET DATA" },
    { value: "~100 ms", value2: "end-to-end", body: "Order orchestration on Redis Streams — thousands of order events per second, at 99.99% accuracy.", tag: "RETAIL · ORDER FULFILLMENT" },
    { value: "sub-ms", value2: "responses", body: "Millions of commands per execution cycle streamed through Redis Streams, sub-millisecond across the pipeline.", tag: "AI SAAS · GTM ORCHESTRATION" },
  ];
  const cw = 2.86, gap = 0.28, x0 = (10 - (3 * cw + 2 * gap)) / 2, cy = 1.55, ch = 2.95;
  cards.forEach((c, i) => {
    const cx = x0 + i * (cw + gap);
    const big = c.value.length <= 4;
    s.addShape(pres.shapes.ROUNDED_RECTANGLE, { x: cx, y: cy, w: cw, h: ch, fill: { color: C.OFFWHITE }, line: { color: C.DUSK30, width: 1 }, rectRadius: 0.08 });
    s.addShape(pres.shapes.RECTANGLE, { x: cx, y: cy, w: cw, h: 0.06, fill: { color: C.RED }, line: { color: C.RED } });
    s.addText(c.value, { x: cx + 0.2, y: cy + 0.28, w: cw - 0.4, h: 0.75, fontSize: big ? 48 : 38, bold: true, color: C.RED, valign: "middle", fontFace: F.HEAD, margin: 0 });
    s.addText(c.value2, { x: cx + 0.2, y: cy + (big ? 1.08 : 0.98), w: cw - 0.4, h: 0.3, fontSize: 14, bold: true, color: C.MIDNIGHT, fontFace: F.HEAD, margin: 0 });
    s.addText(c.body, { x: cx + 0.2, y: cy + 1.5, w: cw - 0.4, h: 1.0, fontSize: 11, color: C.MIDNIGHT, valign: "top", fontFace: F.BODY, margin: 0, lineSpacingMultiple: 1.08 });
    s.addText(c.tag, { x: cx + 0.2, y: cy + ch - 0.36, w: cw - 0.4, h: 0.26, fontSize: 8.5, bold: true, color: C.MIDGRAY, charSpacing: 1, fontFace: F.HEAD, margin: 0 });
  });
  s.addText("Anonymized. Published case studies and closed, validated deals.", { x: 0.42, y: 4.72, w: 9.16, h: 0.24, fontSize: 9, italic: true, color: C.MIDGRAY, fontFace: F.BODY, margin: 0 });
  footer(s, 5, false);
})();

/* ── Slide 6 — Korvet ──────────────────────────────────── */
(() => {
  const s = pres.addSlide();
  s.background = { color: C.WHITE };
  titleBlock(s, "Korvet", "Kafka on Redis — speak the Kafka protocol, store in Redis Streams", false);
  sections(s, [
    { header: "What it is", body: "A Kafka-compatible broker backed by Redis Streams. Existing Kafka clients and CLI tools connect to it exactly as they would to Kafka." },
    { header: "What changes in your app", bullets: [
      "Only bootstrap.servers — the client code is unchanged",
      "No Kafka cluster, no ZooKeeper or KRaft to operate",
      "Redis is the log; each topic is stored as a Redis Stream",
    ] },
    { header: "A note on performance", body: "Korvet's published figures are stated design targets, and there's no official head-to-head with Kafka — so we'll measure it live rather than quote a number." },
  ], 1.45, false);
  footer(s, 6, false);
})();

/* ── Slide 7 — Korvet proof (before/after) ────────────── */
(() => {
  const s = pres.addSlide();
  s.background = { color: C.MIDNIGHT };
  titleBlock(s, "Korvet, proven", "Same Kafka tools — Redis Streams underneath", true);
  const by = 1.95;
  s.addShape(pres.shapes.ROUNDED_RECTANGLE, { x: 0.9, y: by, w: 3.2, h: 1.7, fill: { color: C.DUSK }, line: { color: C.DUSK30, width: 1 }, rectRadius: 0.1 });
  s.addText("BEFORE", { x: 0.9, y: by + 0.18, w: 3.2, h: 0.25, fontSize: 10, bold: true, color: C.DUSK30, charSpacing: 2, align: "center", fontFace: F.HEAD, margin: 0 });
  s.addText("~26 s", { x: 0.9, y: by + 0.44, w: 3.2, h: 0.78, fontSize: 46, bold: true, color: C.DUSK30, align: "center", valign: "middle", fontFace: F.HEAD, margin: 0 });
  s.addText("a Kafka-compatible service", { x: 0.9, y: by + 1.28, w: 3.2, h: 0.3, fontSize: 11, color: C.DUSK30, align: "center", fontFace: F.BODY, margin: 0 });
  s.addText("→", { x: 4.2, y: by + 0.4, w: 1.6, h: 0.9, fontSize: 40, bold: true, color: C.LIME, align: "center", valign: "middle", fontFace: F.HEAD, margin: 0 });
  s.addShape(pres.shapes.ROUNDED_RECTANGLE, { x: 5.9, y: by, w: 3.2, h: 1.7, fill: { color: C.RED }, line: { color: C.RED }, rectRadius: 0.1 });
  s.addText("AFTER — KORVET ON REDIS", { x: 5.9, y: by + 0.18, w: 3.2, h: 0.25, fontSize: 9.5, bold: true, color: C.WHITE, charSpacing: 1, align: "center", fontFace: F.HEAD, margin: 0 });
  s.addText("2–3 ms", { x: 5.9, y: by + 0.44, w: 3.2, h: 0.78, fontSize: 46, bold: true, color: C.WHITE, align: "center", valign: "middle", fontFace: F.HEAD, margin: 0 });
  s.addText("existing Kafka clients, unchanged", { x: 5.9, y: by + 1.28, w: 3.2, h: 0.3, fontSize: 11, color: C.WHITE, align: "center", fontFace: F.BODY, margin: 0 });
  s.addText([
    { text: "End-to-end read latency in a proof of concept for a network-security vendor's log pipeline.  ", options: {} },
    { text: "Redis Streams runs in milliseconds — vs Kafka's typical 10–100 ms.", options: { bold: true, color: C.LIME } },
  ], { x: 0.9, y: 4.05, w: 8.2, h: 0.7, fontSize: 13, color: C.WHITE, align: "center", fontFace: F.BODY, valign: "top", margin: 0, lineSpacingMultiple: 1.1 });
  footer(s, 7, true);
})();

/* ── Slide 8 — How Korvet maps Kafka to Redis Streams ───────────── */
// Kafka ⇄ Redis Streams concept mapping — one partition = one stream.
(() => {
  const s = pres.addSlide();
  s.background = { color: C.WHITE };
  titleBlock(s, "How Korvet maps Kafka to Redis Streams", "This is what Korvet does under the hood — your Kafka client is unchanged", false);

  const REDLITE = "FDECEA";
  // Column headers — make the plurality explicit: one topic  ⇄  several streams
  s.addShape(pres.shapes.RECTANGLE, { x: 0.7, y: 1.52, w: 3.7, h: 0.5, fill: { color: C.DUSK }, line: { color: C.DUSK } });
  s.addText("One Kafka topic", { x: 0.7, y: 1.52, w: 3.7, h: 0.5, fontSize: 14, bold: true, color: C.WHITE, align: "center", valign: "middle", fontFace: F.HEAD, margin: 0 });
  s.addText("split into partitions", { x: 0.7, y: 2.06, w: 3.7, h: 0.22, fontSize: 9.5, color: C.MIDGRAY, align: "center", fontFace: F.BODY, margin: 0 });
  s.addShape(pres.shapes.RECTANGLE, { x: 5.6, y: 1.52, w: 3.7, h: 0.5, fill: { color: C.RED }, line: { color: C.RED } });
  s.addText("Three Redis Streams", { x: 5.6, y: 1.52, w: 3.7, h: 0.5, fontSize: 14, bold: true, color: C.WHITE, align: "center", valign: "middle", fontFace: F.HEAD, margin: 0 });
  s.addText("one per partition", { x: 5.6, y: 2.06, w: 3.7, h: 0.22, fontSize: 9.5, color: C.MIDGRAY, align: "center", fontFace: F.BODY, margin: 0 });

  const parts = ["Partition 0", "Partition 1", "Partition 2"];
  const strms = ["stream  payments:0", "stream  payments:1", "stream  payments:2"];
  const rowY = [2.42, 3.02, 3.62];
  rowY.forEach((y, i) => {
    s.addShape(pres.shapes.ROUNDED_RECTANGLE, { x: 0.7, y, w: 3.7, h: 0.5, fill: { color: C.OFFWHITE }, line: { color: C.DUSK30, width: 1.25 }, rectRadius: 0.05 });
    s.addText(parts[i], { x: 0.9, y, w: 3.4, h: 0.5, fontSize: 12.5, bold: true, color: C.MIDNIGHT, valign: "middle", fontFace: F.HEAD, margin: 0 });
    s.addText("=", { x: 4.55, y, w: 0.9, h: 0.5, fontSize: 22, bold: true, color: C.MIDGRAY, align: "center", valign: "middle", fontFace: F.HEAD, margin: 0 });
    s.addShape(pres.shapes.ROUNDED_RECTANGLE, { x: 5.6, y, w: 3.7, h: 0.5, fill: { color: REDLITE }, line: { color: C.RED, width: 1.25 }, rectRadius: 0.05 });
    s.addText(strms[i], { x: 5.8, y, w: 3.4, h: 0.5, fontSize: 12, color: C.MIDNIGHT, valign: "middle", fontFace: F.CODE, margin: 0 });
  });

  // Everything else lines up 1:1
  s.addText([
    { text: "Everything else lines up 1:1:  ", options: { bold: true } },
    { text: "producer → ", options: {} }, { text: "XADD", options: { fontFace: F.CODE } },
    { text: "   consumer → ", options: {} }, { text: "XREAD / XREADGROUP", options: { fontFace: F.CODE } },
    { text: "   consumer groups and offsets are the same.", options: {} },
  ], { x: 0.7, y: 4.32, w: 8.6, h: 0.3, fontSize: 11, color: C.MIDNIGHT, fontFace: F.BODY, valign: "middle", margin: 0 });

  s.addShape(pres.shapes.ROUNDED_RECTANGLE, { x: 0.7, y: 4.68, w: 8.6, h: 0.5, fill: { color: C.OFFWHITE }, line: { color: C.LIME, width: 1.25 }, rectRadius: 0.05 });
  s.addShape(pres.shapes.OVAL, { x: 0.9, y: 4.83, w: 0.14, h: 0.14, fill: { color: C.LIME }, line: { color: C.LIME } });
  s.addText([
    { text: "The one difference:  ", options: { bold: true } },
    { text: "Korvet stores each partition as one Redis Stream. Use Redis Streams directly (the demo's native lane) and there are no topics or partitions — just streams.", options: {} },
  ], { x: 1.18, y: 4.71, w: 7.95, h: 0.44, fontSize: 11, color: C.MIDNIGHT, fontFace: F.BODY, valign: "middle", margin: 0 });

  footer(s, 8, false);
})();

/* ── Slide 9 — How they fit (two lanes) ────────────────── */
(() => {
  const s = pres.addSlide();
  s.background = { color: C.WHITE };
  titleBlock(s, "How they fit together", "Two ways to run the very same workload", false);

  const laneY = 1.55, laneH = 2.35, laneW = 4.35;
  // Lane A card
  s.addShape(pres.shapes.RECTANGLE, { x: 0.42, y: laneY, w: laneW, h: laneH, fill: { color: C.OFFWHITE }, line: { color: C.MIDNIGHT, width: 1 } });
  s.addShape(pres.shapes.RECTANGLE, { x: 0.42, y: laneY, w: laneW, h: 0.06, fill: { color: "0F7B8F" }, line: { color: "0F7B8F" } });
  s.addText("Lane A · Redis Streams", { x: 0.64, y: laneY + 0.22, w: laneW - 0.4, h: 0.34, fontSize: 16, bold: true, color: C.MIDNIGHT, fontFace: F.HEAD, margin: 0 });
  s.addText("native", { x: 0.64, y: laneY + 0.58, w: laneW - 0.4, h: 0.24, fontSize: 10, bold: true, color: C.MIDGRAY, charSpacing: 1, fontFace: F.HEAD, margin: 0 });
  s.addText("A Java producer appends to a transactions stream; consumer groups read it. Pure Redis, XADD to XREADGROUP.", { x: 0.64, y: laneY + 0.95, w: laneW - 0.5, h: 1.2, fontSize: 12.5, color: C.MIDNIGHT, fontFace: F.BODY, valign: "top", margin: 0, lineSpacingMultiple: 1.15 });

  // Lane B card
  const bx = 5.23;
  s.addShape(pres.shapes.RECTANGLE, { x: bx, y: laneY, w: laneW, h: laneH, fill: { color: C.OFFWHITE }, line: { color: C.MIDNIGHT, width: 1 } });
  s.addShape(pres.shapes.RECTANGLE, { x: bx, y: laneY, w: laneW, h: 0.06, fill: { color: "8A5CF6" }, line: { color: "8A5CF6" } });
  s.addText("Lane B · Kafka on Redis", { x: bx + 0.22, y: laneY + 0.22, w: laneW - 0.4, h: 0.34, fontSize: 16, bold: true, color: C.MIDNIGHT, fontFace: F.HEAD, margin: 0 });
  s.addText("via Korvet", { x: bx + 0.22, y: laneY + 0.58, w: laneW - 0.4, h: 0.24, fontSize: 10, bold: true, color: C.MIDGRAY, charSpacing: 1, fontFace: F.HEAD, margin: 0 });
  s.addText("A standard Kafka client produces to a Korvet topic and a Kafka consumer reads it. Only bootstrap.servers differs.", { x: bx + 0.22, y: laneY + 0.95, w: laneW - 0.5, h: 1.2, fontSize: 12.5, color: C.MIDNIGHT, fontFace: F.BODY, valign: "top", margin: 0, lineSpacingMultiple: 1.15 });

  s.addText("Both lanes carry an identical stream of events — so we can compare them side by side.", { x: 0.42, y: 4.15, w: 9.2, h: 0.4, fontSize: 13, italic: true, color: C.MIDGRAY, fontFace: F.BODY, align: "center", margin: 0 });
  footer(s, 9, false);
})();

/* ── Slide 10 — The architecture (two lanes) ────────────── */
(() => {
  const s = pres.addSlide();
  s.background = { color: C.WHITE };
  titleBlock(s, "The architecture", "One Redis behind both lanes — each feeds one live timing view", false);

  const NEUTRAL = { fill: C.OFFWHITE, stroke: C.DUSK30 };
  const REDISBOX = { fill: "FDECEA", stroke: C.RED };
  const KORVETBOX = { fill: "EFEAFE", stroke: "8A5CF6", tc: "3A2A6B" };
  const TEAL = "0F7B8F", PURPLE = "5B3EA8";
  const bh = 0.64;

  // Lane A — native Redis Streams
  s.addText("LANE A · NATIVE REDIS STREAMS", { x: 0.42, y: 1.30, w: 6.2, h: 0.22, fontSize: 9, bold: true, color: TEAL, charSpacing: 1, fontFace: F.HEAD, margin: 0 });
  const ay = 1.54, amid = ay + bh / 2;
  archBox(pres, s, 0.42, ay, 1.25, bh, { ...NEUTRAL, title: "producer" });
  archBox(pres, s, 2.02, ay, 1.50, bh, { ...REDISBOX, title: "transactions", sub: "Redis Stream" });
  archBox(pres, s, 3.92, ay, 2.75, bh, { ...NEUTRAL, title: "consumer groups", sub: "metrics · alerts · monitor · probe" });
  archArrow(pres, s, 1.67, amid, 2.02, amid);
  archArrow(pres, s, 3.52, amid, 3.92, amid);
  archArrow(pres, s, 6.67, amid, 7.13, 2.18);
  s.addText("XADD to append   ·   XREADGROUP to read", { x: 0.42, y: ay + bh + 0.05, w: 6.2, h: 0.22, fontSize: 9.5, italic: true, color: TEAL, fontFace: F.BODY, margin: 0 });

  // Lane B — Kafka on Redis via Korvet
  s.addText("LANE B · KAFKA ON REDIS (KORVET)", { x: 0.42, y: 2.66, w: 6.2, h: 0.22, fontSize: 9, bold: true, color: PURPLE, charSpacing: 1, fontFace: F.HEAD, margin: 0 });
  const by = 2.90, bmid = by + bh / 2;
  archBox(pres, s, 0.42, by, 1.55, bh, { ...NEUTRAL, title: "kafka-producer" });
  archBox(pres, s, 2.37, by, 1.90, bh, { ...KORVETBOX, title: "Korvet", sub: "topic: kafka-transactions" });
  archBox(pres, s, 4.67, by, 1.70, bh, { ...NEUTRAL, title: "kafka-probe" });
  archArrow(pres, s, 1.97, bmid, 2.37, bmid);
  archArrow(pres, s, 4.27, bmid, 4.67, bmid);
  archArrow(pres, s, 6.37, bmid, 7.13, 2.95);
  s.addText("standard Kafka client — only bootstrap.servers changes", { x: 0.42, y: by + bh + 0.05, w: 6.2, h: 0.22, fontSize: 9.5, italic: true, color: PURPLE, fontFace: F.BODY, margin: 0 });

  // Timing view (right, spans both lanes)
  const tx = 7.13, tw = 2.45, ty = 1.54, th = 2.72;
  s.addShape(pres.shapes.ROUNDED_RECTANGLE, { x: tx, y: ty, w: tw, h: th, fill: { color: C.WHITE }, line: { color: C.RED, width: 2 }, rectRadius: 0.08 });
  s.addText("Monitor API → Dashboard", { x: tx + 0.16, y: ty + 0.14, w: tw - 0.3, h: 0.26, fontSize: 12, bold: true, color: C.MIDNIGHT, fontFace: F.HEAD, margin: 0 });
  s.addText("end-to-end latency, per lane", { x: tx + 0.16, y: ty + 0.42, w: tw - 0.3, h: 0.22, fontSize: 9, color: C.MIDGRAY, fontFace: F.BODY, margin: 0 });
  s.addShape(pres.shapes.LINE, { x: tx + 0.16, y: ty + 0.70, w: tw - 0.32, h: 0, line: { color: C.DUSK30, width: 0.75 } });
  s.addShape(pres.shapes.RECTANGLE, { x: tx + 0.16, y: ty + 0.88, w: 0.14, h: 0.14, fill: { color: "0F7B8F" }, line: { color: "0F7B8F" } });
  s.addText("Redis Streams", { x: tx + 0.38, y: ty + 0.84, w: tw - 0.5, h: 0.22, fontSize: 10, bold: true, color: C.MIDNIGHT, fontFace: F.HEAD, margin: 0 });
  s.addShape(pres.shapes.RECTANGLE, { x: tx + 0.16, y: ty + 1.24, w: 0.14, h: 0.14, fill: { color: "8A5CF6" }, line: { color: "8A5CF6" } });
  s.addText("Korvet", { x: tx + 0.38, y: ty + 1.20, w: tw - 0.5, h: 0.22, fontSize: 10, bold: true, color: C.MIDNIGHT, fontFace: F.HEAD, margin: 0 });
  s.addText("p50 · p95 · throughput,\nmeasured live and side by side", { x: tx + 0.16, y: ty + 1.66, w: tw - 0.3, h: 0.7, fontSize: 9, color: C.MIDGRAY, fontFace: F.BODY, valign: "top", margin: 0, lineSpacingMultiple: 1.1 });

  s.addText("One Redis backs both lanes: the transactions stream and Korvet's topic storage (korvet:storage:local:kafka-transactions:0).", { x: 0.42, y: 4.55, w: 9.2, h: 0.3, fontSize: 9, color: C.MIDGRAY, fontFace: F.BODY, margin: 0 });
  footer(s, 10, false);
})();

/* ── Slide 11 — What you'll see (dark cards) ────────────── */
(() => {
  const s = pres.addSlide();
  s.background = { color: C.MIDNIGHT };
  titleBlock(s, "What you'll see in the demo", "Fast, resilient, and simple — with the Redis you already run", true);
  cardsDark(pres, s, [
    { label: "React in milliseconds", body: "Events flow through Redis and the timing panel shows produce-to-consume latency live — microseconds on the Redis lane, well inside a tight SLA." },
    { label: "Resilient — no lost events", body: "Stop a worker: the backlog rises, then drains when it returns. Consumer groups and acks mean nothing is dropped — a missed event is not a lost one." },
    { label: "Simple at scale", body: "One Redis, fan-out to many consumer groups. The same code scales out with Redis Cluster and more consumers — nothing extra to operate." },
  ]);
  footer(s, 11, true);
})();

/* ── Slide 12 — Takeaways (red) ─────────────────────────── */
(() => {
  const s = pres.addSlide();
  s.background = { color: C.RED };
  logo(s, 0.42, 0.40, 0.36, true);
  s.addText("Three things to take away", { x: 0.42, y: 1.05, w: 9.0, h: 0.7, fontSize: 34, color: C.WHITE, fontFace: F.HEAD, margin: 0 });

  const points = [
    "Redis Streams is a real event log — in the Redis you already run.",
    "Korvet runs your existing Kafka apps on Redis — only bootstrap.servers changes.",
    "Comparable end-to-end latency, with far less to operate.",
  ];
  points.forEach((p, i) => {
    const y = 2.05 + i * 0.85;
    s.addShape(pres.shapes.OVAL, { x: 0.46, y: y + 0.05, w: 0.34, h: 0.34, fill: { color: C.LIME }, line: { color: C.LIME } });
    s.addText(String(i + 1), { x: 0.46, y: y + 0.04, w: 0.34, h: 0.34, fontSize: 15, bold: true, color: C.MIDNIGHT, align: "center", valign: "middle", fontFace: F.HEAD, margin: 0 });
    s.addText(p, { x: 1.0, y, w: 8.3, h: 0.7, fontSize: 17, color: C.WHITE, fontFace: F.BODY, valign: "middle", margin: 0, lineSpacingMultiple: 1.05 });
  });

  s.addText("Now — let's look at the demo.", { x: 0.42, y: 4.70, w: 8.0, h: 0.4, fontSize: 15, bold: true, color: C.LIME, fontFace: F.HEAD, margin: 0 });
  s.addText("© 2026 Redis Ltd. All rights reserved.", { x: 0.42, y: 5.22, w: 5.0, h: 0.20, fontSize: 6.5, color: C.WHITE, fontFace: F.BODY, transparency: 45, margin: 0 });
})();

pres.writeFile({ fileName: OUT }).then((f) => console.log("Wrote:", f));
