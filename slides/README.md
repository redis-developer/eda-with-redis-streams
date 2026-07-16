# Intro slides

`redis-streams-korvet-intro.pptx` — an ~8-slide, non-technical intro deck in the 2026 Redis
theme, meant to set up background before the live demo:

1. Title
2. Why event-driven?
3. Apache Kafka — the standard, but a cluster to operate
4. Redis Streams — append, fan-out, replay
5. Korvet — Kafka on Redis (only `bootstrap.servers` changes)
6. How they fit — the two lanes
7. What you'll see in the demo
8. Three takeaways

## Regenerating

The deck is generated with [pptxgenjs](https://gitbrent.github.io/PptxGenJS/):

```bash
npm install pptxgenjs
node build-deck.js            # writes redis-streams-korvet-intro.pptx
# or: node build-deck.js /path/to/output.pptx
```

## Notes

- Footers and title use the official Redis wordmark: `assets/redis-logo-white.png` on the
  red/dark slides and `assets/redis-logo-red.png` on the light slides (the red one is derived
  from the white wordmark's alpha mask). Both are embedded into the `.pptx` at build time, so the
  file is self-contained. `build-deck.js` resolves them relative to its own folder, so keep
  `assets/` next to it when regenerating.
- Fonts are Space Grotesk (headings/body) and Space Mono (code), the 2026 Redis fonts. If they
  aren't installed, PowerPoint/Keynote will substitute a sans-serif — install them for exact
  fidelity.
- Performance framing is deliberate: Korvet's published numbers are design targets and there is
  no official head-to-head vs Apache Kafka, so the deck says "measured live, comparable," not a
  benchmark claim.
