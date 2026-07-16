# Redis Streams EDA Demo

## Overview
This demo shows event-driven processing on Redis Streams and, side by side, the same workload running over the Kafka protocol against [Korvet](https://github.com/redis-field-engineering/korvet-dist) — a Kafka-compatible broker backed by Redis Streams. Built with Java, Redis, Redis Insight, and a lightweight web dashboard, it runs two independent lanes on one machine:

* **Lane A — native Redis Streams.** A Java producer `XADD`s synthetic transactions to a `transactions` stream; independent consumer groups materialize analytics, emit a derived `alerts` stream, and power a live dashboard.
* **Lane B — Kafka on Redis (Korvet).** A standard Kafka client produces the same transactions to a Korvet topic and a standard Kafka consumer group reads them back. The only difference from a real Kafka deployment is `bootstrap.servers` — the broker is Korvet, and Redis is the log.

A timing probe on each lane measures end-to-end latency (produce → consume) and the dashboard renders the two lanes head to head, so you can see that the Kafka-on-Redis path behaves comparably to the Redis-native one.

## Table of Contents

* Demo Objectives
* How It Works
* Setup
* Running the Demo
* The Timing View
* Demonstrating Lag and Recovery
* Lane B: The Kafka Protocol via Korvet
* Inspecting State in Redis Insight
* Known Issues
* Resources
* Maintainers
* License

## Demo Objectives

* Demonstrate Redis Streams as the source event log for a multi-consumer workflow
* Show fan-out from one `transactions` stream to multiple independent consumer groups
* Highlight materialized analytics written directly into Redis data structures
* Illustrate stateful processing that emits a derived `alerts` stream
* Show consumer-group lag and recovery while the producer keeps publishing
* Demonstrate that the **same workload runs over the Kafka protocol via Korvet with no code change beyond `bootstrap.servers`**, and compare its end-to-end latency to the Redis-native lane live

## How It Works

There are two lanes, and they never touch each other. That separation is deliberate: it lets you compare them fairly.

```
Lane A (native Redis Streams)
  producer ──XADD──▶ "transactions" (Redis Stream) ──XREADGROUP──▶ metrics-cg  (analytics)
                                                     │             alerts-cg   (derived "alerts" stream)
                                                     │             monitor-cg  (dashboard)
                                                     └──────────▶ probe-redis-cg (latency probe → lane "redis")

Lane B (Kafka on Redis via Korvet)
  kafka-producer ──Kafka protocol──▶ Korvet topic "kafka-transactions" ──Kafka consumer group──▶ kafka-probe
                                     (stored as a Redis Stream)                                   (latency probe → lane "korvet")
```

Every event carries a producer `timestamp`. Each lane's probe computes end-to-end latency as `consume_time − timestamp`, then publishes samples, a per-second throughput gauge, and a running count into Redis. The `monitor-api` service reads both lanes and the dashboard renders them side by side.

> Korvet is a Kafka-compatible broker that implements the Kafka wire protocol and stores each topic partition as a Redis Stream. Existing Kafka clients (and CLI tools like `kafka-console-producer`) connect to it exactly as they would to a Kafka broker — no Kafka cluster, no ZooKeeper/KRaft. In this demo the Korvet topic `kafka-transactions` is backed by the Redis stream `korvet:storage:local:kafka-transactions:0`.

## Setup

### Dependencies

* Docker 24+
* Docker Compose v2
* A modern browser
* Enough Docker resources to run Redis, Redis Insight, Korvet, and the Java services comfortably

### Running the demo locally
This demo runs from the repository root using the provided `docker-compose.yml`. The Java services are built from the included `Dockerfile`; the web dashboard is served separately from `monitor-web` using Nginx. Both lanes share a single Redis deployment (Korvet is backed by the same Redis), so you can inspect the streams, consumer groups, materialized analytics, and latency telemetry in one place.

To start the full stack, open a terminal in the repository root and run:

```bash
docker compose up --build
```

The first build compiles the Java application and assembles the container images, so it takes longer than subsequent runs.

### Configuration knobs
Configuration is via environment variables in `docker-compose.yml`. The most useful settings are:

* `PRODUCER_RATE_PER_SECOND` on the `producer` and `kafka-producer` services — how fast each lane publishes (1–100)
* `METRICS_PROCESSING_DELAY_MS` on the `metrics-*` services — a small artificial delay so lag and recovery stay visible during the demo

After changing a value, rebuild with `docker compose up --build`.

## Running the Demo

1. Open a terminal in the repository root.

2. Start the demo:

```bash
docker compose up --build
```

This will:

* Start a Redis database, published on host port `6380` (still `6379` inside the compose network, so a Redis already running on your host's `6379` will not clash)
* Start Redis Insight on port `5540`
* Start Korvet, a Kafka-compatible broker backed by the same Redis, on port `9092`
* Start a `kafka-tools` container with the standard Kafka CLI tools
* **Lane A:** start the native `producer` (writes to the `transactions` stream), two `metrics` consumers (`metrics-cg`), an `alerts` consumer (`alerts-cg`), and a `redis-probe` latency probe (`probe-redis-cg`)
* **Lane B:** start a `kafka-producer` (writes to the Korvet topic `kafka-transactions`) and a `kafka-probe` Kafka consumer group (`probe-korvet`)
* Start the monitor API and the browser dashboard

3. Verify the containers are running:

```bash
docker compose ps
```

You should see: `redis-database`, `redis-insight`, `redis-korvet`, `kafka-tools`, `producer`, `metrics-1`, `metrics-2`, `alerts`, `redis-probe`, `kafka-producer`, `kafka-probe`, `monitor-api`, `monitor-web`.

4. Access the demo in your browser:

* Web dashboard: `http://localhost:8088`
* Redis Insight: `http://localhost:5540`

Once the stack is up, the dashboard polls the monitor API automatically. Redis Insight can inspect streams and keys directly.

To stop the demo:

```bash
docker compose down
```

## The Timing View
The **End-to-end latency** panel at the top of the dashboard compares the two lanes. For each lane it shows:

* **p50 / p95** — median and 95th-percentile end-to-end latency, produce → consume. Latency is recorded in microseconds; the dashboard shows µs when it is sub-millisecond (typical for the Redis Streams lane on one host) and ms otherwise
* **throughput** — messages per second the probe is currently consuming
* a comparison bar scaled to the higher p95, plus the running processed count

Both lanes generate the same workload at the same rate, so the panel is an apples-to-apples comparison. In a typical run the Kafka-on-Redis lane tracks the Redis-native lane closely.

> **Read the numbers honestly on stage.** Everything here runs on one host over the loopback interface, so all latencies are small and the comparison is *relative and functional*, not a benchmark. Korvet's published performance figures are stated design targets, and there is no official head-to-head against Apache Kafka — so let the live numbers speak rather than asserting a specific figure. For real load testing, Korvet ships a `load-testing` sample with Prometheus and Grafana.

## Demonstrating Lag and Recovery
One goal of Lane A is to show that consumer groups fall behind and recover independently while the producer keeps publishing. Stop one metrics worker:

```bash
docker compose stop metrics-2
```

After a short pause the dashboard shows fewer active metrics consumers and a rising **Metrics Backlog (entries)** count (this is the consumer-group backlog — a count of unread entries, not a latency). The `transactions` stream keeps growing because the producer is still running, and the remaining worker processes at a slower rate.

Bring the worker back:

```bash
docker compose start metrics-2
```

The backlog drains and the dashboard returns to two metrics consumers.

To show isolation between consumer groups, stop the monitor backend independently — `monitor-cg` falls behind temporarily while `metrics-cg` and `alerts-cg` keep processing:

```bash
docker compose stop monitor-api
docker compose start monitor-api
```

## Lane B: The Kafka Protocol via Korvet
Lane B runs continuously alongside Lane A — `kafka-producer` and `kafka-probe` are standard Kafka client applications whose only broker is Korvet. Nothing about them is Redis-aware except the fact that, under the hood, Korvet stores their topic as a Redis Stream.

To make the point live, you can also produce to the same topic by hand with the stock Kafka console producer and watch the Korvet lane's throughput and latency react on the dashboard:

```bash
docker compose exec kafka-tools /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server redis-korvet:9092 \
  --topic kafka-transactions
```

Then paste one JSON event per line (press Enter after each). The `timestamp` must be a **current** epoch-milliseconds value so the latency probe measures a small, realistic delay — a stale timestamp is treated as a backlog outlier and excluded from the latency percentiles (see `LATENCY_MAX_SAMPLE_MS`). Get the current value with `date +%s%3N` and substitute it. Use the demo's categories (`payroll`, `wire`, `pos`, `ach`, `internal`) and regions (`northeast`, `southeast`, `west`, `midwest`):

```json
{"txn_id":"TXN-cli01","amount":"48500.00","category":"wire","region":"southeast","risk_score":"93","timestamp":"<current-epoch-ms>"}
{"txn_id":"TXN-cli02","amount":"250.00","category":"pos","region":"west","risk_score":"12","timestamp":"<current-epoch-ms>"}
```

Confirm the topic and its single partition:

```bash
docker compose exec kafka-tools /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server redis-korvet:9092 --describe --topic kafka-transactions
```

> Korvet creates topics lazily on first produce, so listing topics before any message is sent shows nothing — that is expected. This demo also sets `KORVET_STORAGE_LOCAL_COMPRESSION_CODEC=none` so the stored value stays plain JSON you can read directly in Redis Insight.

## Inspecting State in Redis Insight
Open Redis Insight (`http://localhost:5540`) and look at:

* Stream `transactions` — the native (Lane A) event log
* Stream `korvet:storage:local:kafka-transactions:0` — Korvet's backing store for the Lane B topic
* Stream `alerts` — the derived alert events
* `metrics:total_count`, `metrics:total_volume`, `metrics:high_risk_count`
* `metrics:volume_by_category` (sorted set), `metrics:count_by_region` (JSON)
* `latency:samples:redis` / `latency:samples:korvet` — recent latency samples per lane (microseconds)
* `latency:rate:redis` / `latency:rate:korvet` — current per-second throughput per lane

Command-line inspection from any Redis runner:

```redis
XLEN transactions
XINFO GROUPS transactions
XRANGE transactions - + COUNT 5
XRANGE korvet:storage:local:kafka-transactions:0 - + COUNT 5
LRANGE latency:samples:redis 0 9
LRANGE latency:samples:korvet 0 9
```

## Known Issues

* Redis Insight may not connect automatically. If that happens, add `redis-database:6379` manually from the Redis Insight UI.
* The first `docker compose up --build` takes longer because the Java application must be compiled and the images built.
* This is a demo workload with synthetic data and intentionally simplified operational behavior. It illustrates stream patterns and a transport comparison; it is not a production reference architecture or a benchmark.
* If the web dashboard is not updating, inspect the monitor logs: `docker compose logs --tail=100 monitor-api`.
* If the Kafka lane shows no latency data, check the Kafka client logs: `docker compose logs --tail=100 kafka-producer` and `docker compose logs --tail=100 kafka-probe`, and confirm `redis-korvet` is healthy (`docker compose ps`).
* `MONITOR_API_PORT` is fixed at `8080` in this demo: the Nginx dashboard proxies to `monitor-api:8080` (`monitor-web/nginx.conf`), so changing it requires editing the compose port mapping, the healthcheck, and `nginx.conf` together.

## Resources

* `docker-compose.yml` — the service topology for both lanes
* `src/main/java/io/redis/devrel/demo/eda/producer` — the native and Kafka transaction producers
* `src/main/java/io/redis/devrel/demo/eda/consumer` — the metrics and alerts consumers
* `src/main/java/io/redis/devrel/demo/eda/probe` — the two latency probes (`RedisLatencyProbe`, `KafkaLatencyProbe`) and shared `LatencyRecorder`
* `src/main/java/io/redis/devrel/demo/eda/web` — the monitor API
* `monitor-web` — the browser dashboard
* [Korvet](https://github.com/redis-field-engineering/korvet-dist) — the Kafka-compatible broker backed by Redis Streams, and its [Kafka CLI sample](https://github.com/redis-field-engineering/korvet-dist/tree/main/samples/kafka-cli)

## Maintainers
* Ricardo Ferreira — [@riferrei](https://github.com/riferrei)

## License
This project is licensed under the MIT License. See the `LICENSE` file for details.
