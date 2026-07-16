# Demo Runbook — Redis Streams, Kafka & Korvet

A step-by-step script for running this demo live. Every step has **one command to run** and a **"you should see"** line. If something looks wrong, jump to [Troubleshooting](#troubleshooting).

- **Total demo time:** ~20–25 min (after the intro slides)
- **What the audience watches:** the dashboard at `http://localhost:8088` and, optionally, Redis Insight at `http://localhost:5540`
- **The story in one sentence:** Redis Streams is a first-class event log; Korvet lets your existing Kafka apps run on that same Redis with comparable latency and no Kafka cluster.

---

## 0. Preflight — do this ~15 minutes before you go live

**0.1 — Make sure Docker Desktop is running.**
```bash
docker info >/dev/null && echo "docker OK"
```
You should see `docker OK`.

**0.2 — Start everything (first run builds images; ~3–5 min).**
```bash
docker compose up --build -d
```
You should see a list of containers ending in `Started`.

**0.3 — Confirm every service is healthy.**
```bash
docker compose ps
```
You should see 13 services, all `Up ... (healthy)` (except `kafka-tools`, which just says `Up`). Wait ~30s and re-run if any still say `health: starting`.

**0.4 — Open the two browser tabs:**
- Dashboard: **http://localhost:8088**
- Redis Insight: **http://localhost:5540** (if it doesn't auto-connect, add database `redis-database:6379`)

**0.5 — Confirm both latency lanes are live (the one check that matters).**
```bash
curl -s http://localhost:8080/api/monitor | python3 -m json.tool | grep -A2 '"lane"'
```
You should see two lanes, `redis` and `korvet`, both with non-zero `p50Micros` and `ratePerSecond` (~90/s). On the dashboard, the **End-to-end latency** panel shows two cards; the Redis Streams lane is usually sub-millisecond so it reads in **µs**, and the Korvet lane reads in µs or a few ms.

> If the `korvet` lane shows zeros, give it ~20s (it starts reading at the tail). Still zero → see Troubleshooting.

**0.6 — Leave it running.** Let it warm up for a minute so the numbers are steady before you present. Do not restart `kafka-probe` mid-demo.

---

## 1. Intro (slides)
The deck opens by busting the "Redis is just a cache" myth, then walks to Streams and Korvet. Beats to hit:

1. **Hook — "Isn't Redis just a cache?"** No — it's a *data-structure server*. Proof point: **a European telecom runs real-time mobile signaling on Redis at ~878K ops/sec, ~1.3 ms latency** — serious real-time infrastructure, not a side cache.
2. **Data structures** — strings, hashes, sorted sets, JSON, vectors, time series… and Streams. "Today's talk lives in one of them — the Stream."
3. **Redis Streams** — a first-class event log: append with `XADD`, fan-out with `XREADGROUP`, replay.
4. **Streams in the wild (stats slide)** — absolute numbers teams *run on* Streams (anonymized). These are what they do on Streams, not comparisons to an old system:
   - **Capital markets: sub-4 ms Redis latency** — a 100% Redis Streams platform for OPRA options/equities market data, inside a <100 ms end-to-end SLA, zero packet loss (network egress ~1–4 GB/s).
   - **Retail order fulfillment: ~100 ms** end-to-end orchestration, thousands of order events/sec, 99.99% accuracy.
   - **AI-GTM SaaS: sub-millisecond** stream responsiveness, millions of commands per execution cycle.
5. **Korvet** — speak the Kafka protocol, store in Redis Streams; only `bootstrap.servers` changes.
6. **Korvet, proven (stats slide)** — a network-security **POC: end-to-end read latency ~26 s → ~2–3 ms** vs a Kafka-compatible service. (Redis Streams runs in ms; Kafka is typically 10–100 ms.)
7. **How Korvet maps Kafka → Redis Streams** — one partition = one stream; everything else maps 1:1.
8. **How they fit / architecture** — the two lanes you're about to see live.

Then switch to the dashboard.

---

## 2. Act 1 — Redis Streams, native (~5 min)

**Say:** "One producer is appending synthetic transactions to a Redis Stream. Multiple independent consumer groups read the same stream for different jobs — that's fan-out."

**Show on the dashboard:** the **Transactions Stream** counter climbing, the **Recent Transactions** table updating, **Volume by Category** / **Count by Region** filling in, and **Recent Alerts** (a stateful consumer deriving alerts into a second stream).

**Show the fan-out in Redis (optional, strong):**
```bash
docker compose exec redis-database redis-cli XINFO GROUPS transactions
```
You should see four consumer groups on one stream: `metrics-cg`, `alerts-cg`, `monitor-cg`, `probe-redis-cg`.

**Say:** "Every group has its own cursor over the same log. Add a consumer, it gets its own view. This is the pattern people usually stand up a Kafka cluster for."

**Flash the code (optional) — it's just two calls.** The whole producer is one line in `src/main/java/io/redis/devrel/demo/eda/producer/TransactionProducer.java`:
```java
jedis.xadd(TRANSACTIONS_STREAM_KEY, StreamEntryID.NEW_ENTRY, txCodec.toFields(transaction));
```
and each group reads with `XREADGROUP` (`consumer/MetricsConsumer.java`):
```java
jedis.xreadGroup(METRICS_GROUP_NAME, consumerName, params, Map.of(TRANSACTIONS_STREAM_KEY, ">"));
```
**Say:** "Append with XADD, read your group's new entries with XREADGROUP. That's the whole API."

**Scale point (say):** "This is also how it scales — each consumer group is independent, and Redis Cluster shards streams across nodes. You add consumers, not infrastructure. Same code from a laptop to a cluster."

---

## 3. Act 2 — Lag & recovery (~4 min)

**Say:** "Consumer groups track their own progress, so one slow or dead worker doesn't lose data — it just falls behind and catches up."

**Run — take a worker down:**
```bash
docker compose stop metrics-2
```
You should see, within ~10s: **Metrics Consumers** drop from 2 → 1, and **Metrics Backlog (entries)** start rising while the producer keeps going.

**Run — bring it back:**
```bash
docker compose start metrics-2
```
You should see **Metrics Consumers** return to 2 and the backlog drain back toward 0.

**Say:** "Nothing was lost — the backlog is just unread entries, and the recovered worker picks up exactly where the group left off."

**Raise the stakes (say):** "In a real pipeline a missed event is missed revenue — a dropped payment, an alert that never fired. Here nothing is dropped: each group tracks its own position, and un-acked entries stay pending until they're processed. In production, Redis Cloud failover is sub-second and transparent to clients — the pipeline keeps moving."

---

## 4. Act 3 — Kafka on Redis, via Korvet (~6 min) — the payoff

**Say:** "Now the same workload over the Kafka protocol. `kafka-producer` and `kafka-probe` are ordinary Kafka client apps — the only difference from talking to Apache Kafka is `bootstrap.servers=redis-korvet:9092`. The broker is Korvet, and Redis is the log."

**Flash the code (optional) — the "only `bootstrap.servers`" proof.** `producer/KafkaTransactionProducer.java` and `probe/KafkaLatencyProbe.java` are stock `KafkaProducer` / `KafkaConsumer` apps (`producer.send(...)`, `consumer.subscribe(...)` + `consumer.poll(...)`). The one Redis-specific line lives in `docker-compose.yml`:
```yaml
KAFKA_BOOTSTRAP_SERVERS: redis-korvet:9092
```
For the strongest beat, show the two probes side by side — `probe/RedisLatencyProbe.java` (XREADGROUP) next to `probe/KafkaLatencyProbe.java` (Kafka subscribe/poll): the *same* latency measurement, one over the Redis Streams API, one over the Kafka protocol — both hitting the same Redis.
**Say:** "This is the exact Kafka client you'd point at a Kafka cluster. Change one line — the broker address — and it's running on Redis."

**Show the timing panel:** point at the two lanes side by side.

**Say:** "Same 100 events/sec, same end-to-end measurement — produce timestamp to consume time. The Kafka-on-Redis lane tracks the native Redis lane closely. No Kafka cluster, no ZooKeeper/KRaft — just Redis."

**Frame it as an SLA (say):** "Pick a number — say a 10 ms p99 SLA. Watch both lanes: the Redis-native lane sits in the microseconds, and Kafka-on-Redis stays well under the ceiling. The design target is react in milliseconds, not tens of milliseconds."

> Honesty line for the room: "This is one laptop over loopback, so it's a functional, apples-to-apples comparison, not a benchmark."

> Optional throughput beat: the producers are capped at 100 msg/s today. To push more load live you'd raise `PRODUCER_RATE_PER_SECOND` on the `producer` / `kafka-producer` services and rebuild those two — ask me to raise the cap if you want this.

**Show that the Kafka topic IS a Redis stream (the "aha"):**
```bash
docker compose exec redis-database redis-cli XRANGE korvet:storage:local:kafka-transactions:0 - + COUNT 2
```
You should see the Kafka-produced messages sitting in a Redis Stream. **Say:** "Your Kafka topic is a Redis Stream underneath. Same data, both worlds."

**Optional live flourish — hand-produce a Kafka message and watch the lane react:**
```bash
TS=$(date +%s%3N)
docker compose exec kafka-tools /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server redis-korvet:9092 --topic kafka-transactions <<EOF
{"txn_id":"TXN-live1","amount":"48500.00","category":"wire","region":"southeast","risk_score":"93","timestamp":"$TS"}
EOF
```
(Use a **current** timestamp as shown — a stale one is ignored by the latency stats on purpose.)

---

## 5. Wrap (~1 min)
- Redis Streams = a real event log you already have.
- Korvet = your Kafka apps, unchanged, on Redis — comparable latency, far less to operate.
- Point back to the architecture slide (the two-lane diagram; also in `images/architecture.svg`).

---

## Hitting the webinar promises

A quick map from the abstract's promises to where each one lands, so you hit them consciously:

| Promise | Where it lands | One line to say |
|---|---|---|
| React in **milliseconds**, strict **SLAs** | Timing panel (Act 3) | "Microseconds on Redis, well under a 10 ms SLA." |
| **High-throughput at scale** | Fan-out (Act 1) + narration | "Add consumers, shard with Redis Cluster — scale out, same code." |
| **Resilient / fault-tolerant**, no lost events | Lag & recovery (Act 2) | "A missed event isn't a lost one — nothing is dropped; failover is sub-second." |
| **Simple — tools you already have** | Whole demo + Korvet | "It's the Redis you already run; Korvet reuses it for your Kafka apps." |

---

## Troubleshooting

| Symptom | Fix |
|---|---|
| `up` fails: **"port ... already in use"** for `6379` | Expected on a machine already running Redis. This demo already publishes Redis on **6380**, so you shouldn't hit this. If another tool holds **8088 / 9092 / 8080 / 5540**, stop it or change that service's published port in `docker-compose.yml`. |
| Dashboard loads but **korvet lane is 0** | Wait ~20s (it starts at the topic tail). Then `docker compose logs --tail=50 kafka-probe` and confirm `docker compose ps` shows `redis-korvet` healthy. |
| **Dashboard not updating** | `docker compose logs --tail=100 monitor-api` |
| A lane shows an **absurd latency** (huge number) | You produced an event with a stale/old timestamp, or restarted a probe onto a backlog. It self-clears in a few seconds; for a clean slate run `docker compose down && docker compose up -d`. |
| **Metrics not moving** | `docker compose logs --tail=100 metrics-1` |
| Want a **totally clean start** | `docker compose down` then `docker compose up -d` (Redis data is in-container, so this resets all streams and metrics). |

---

## Command cheat-sheet (copy/paste)

```bash
# start / stop
docker compose up --build -d        # start everything
docker compose ps                   # health check
docker compose down                 # stop + clean

# the lag/recovery beat
docker compose stop metrics-2
docker compose start metrics-2

# inspect
curl -s http://localhost:8080/api/monitor | python3 -m json.tool   # full snapshot
docker compose exec redis-database redis-cli XINFO GROUPS transactions
docker compose exec redis-database redis-cli XRANGE korvet:storage:local:kafka-transactions:0 - + COUNT 3
docker compose logs --tail=50 kafka-probe kafka-producer           # kafka lane
```

**URLs:** dashboard `http://localhost:8088` · Redis Insight `http://localhost:5540` · monitor API `http://localhost:8080/api/monitor` · Korvet broker `redis-korvet:9092` (host `localhost:9092`) · Redis `localhost:6380`
