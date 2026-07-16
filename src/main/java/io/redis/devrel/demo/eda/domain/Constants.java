package io.redis.devrel.demo.eda.domain;

import java.util.List;

public final class Constants {
    public static final String REDIS_HOST_ENV = "REDIS_HOST";
    public static final String REDIS_PORT_ENV = "REDIS_PORT";
    public static final String REDIS_DEFAULT_HOST = "localhost";
    public static final String REDIS_DEFAULT_PORT = "6379";
    public static final String STREAM_GROUP_START_ID = "0-0";
    public static final String HEALTH_FILE_PATH = "/tmp/healthy";

    // Lane A (native Redis Streams) source event log. This is an ordinary Redis Stream the demo
    // owns outright: the native producer writes here with XADD and the consumer groups below read
    // from it with XREADGROUP. Lane B (the Kafka protocol via Korvet) is a fully independent
    // pipeline with its own topic and consumer — see the KAFKA_* constants below.
    public static final String TRANSACTIONS_STREAM_KEY = "transactions";
    public static final String PRODUCER_RATE_PER_SECOND_ENV = "PRODUCER_RATE_PER_SECOND";
    public static final int PRODUCER_DEFAULT_RATE_PER_SECOND = 100;
    public static final int PRODUCER_MAX_RATE_PER_SECOND = 100;

    public static final String METRICS_GROUP_NAME = "metrics-cg";
    public static final String METRICS_CONSUMER_NAME_ENV = "METRICS_CONSUMER_NAME";
    public static final String METRICS_PROCESSING_DELAY_MS_ENV = "METRICS_PROCESSING_DELAY_MS";
    public static final String METRICS_CONSUMER_NAME = "metrics-aggregator";
    public static final String METRICS_TOTAL_COUNT_KEY = "metrics:total_count";
    public static final String METRICS_TOTAL_VOLUME_KEY = "metrics:total_volume";
    public static final String METRICS_VOLUME_BY_CATEGORY_KEY = "metrics:volume_by_category";
    public static final String METRICS_COUNT_BY_REGION_KEY = "metrics:count_by_region";
    public static final String METRICS_HIGH_RISK_COUNT_KEY = "metrics:high_risk_count";

    public static final String ALERTS_STREAM_KEY = "alerts";
    public static final String ALERTS_GROUP_NAME = "alerts-cg";
    public static final String ALERTS_CONSUMER_NAME_ENV = "ALERTS_CONSUMER_NAME";
    public static final String ALERTS_CONSUMER_NAME = "alert-engine";

    public static final String AUTOCLAIM_IDLE_THRESHOLD_MS_ENV = "AUTOCLAIM_IDLE_THRESHOLD_MS";
    public static final long AUTOCLAIM_IDLE_THRESHOLD_DEFAULT_MS = 10_000L;

    public static final String MONITOR_GROUP_NAME = "monitor-cg";
    public static final String MONITOR_CONSUMER_NAME_ENV = "MONITOR_CONSUMER_NAME";
    public static final String MONITOR_CONSUMER_NAME = "monitor-web-ui";

    // --- Lane B: the same workload over the Kafka protocol, served by Korvet ---
    // Korvet is a Kafka-compatible broker backed by Redis Streams, so a standard Kafka client
    // produces to and consumes from it with no code change beyond bootstrap.servers. This lane is
    // deliberately separate from the native Redis stream above so the two can be compared head to
    // head in the timing view.
    public static final String KAFKA_BOOTSTRAP_SERVERS_ENV = "KAFKA_BOOTSTRAP_SERVERS";
    public static final String KAFKA_BOOTSTRAP_DEFAULT = "localhost:9092";
    public static final String KAFKA_TOPIC_ENV = "KAFKA_TOPIC";
    public static final String KAFKA_TOPIC_DEFAULT = "kafka-transactions";
    public static final String KAFKA_PROBE_GROUP_ENV = "KAFKA_PROBE_GROUP";
    public static final String KAFKA_PROBE_GROUP_DEFAULT = "probe-korvet";

    // --- Latency probes / timing view ---
    // One probe per lane measures end-to-end latency (consume time minus the producer timestamp
    // carried on every event) and publishes samples, a per-second rate, and a running count into
    // Redis so the monitor API can render a live side-by-side comparison.
    public static final String REDIS_PROBE_GROUP_NAME = "probe-redis-cg";
    public static final String PROBE_CONSUMER_NAME_ENV = "PROBE_CONSUMER_NAME";
    public static final String PROBE_CONSUMER_NAME_DEFAULT = "probe";
    public static final String LANE_REDIS = "redis";
    public static final String LANE_KORVET = "korvet";
    public static final List<String> LATENCY_LANES = List.of(LANE_REDIS, LANE_KORVET);
    public static final String LATENCY_SAMPLES_KEY_PREFIX = "latency:samples:";
    public static final String LATENCY_RATE_KEY_PREFIX = "latency:rate:";
    public static final String LATENCY_COUNT_KEY_PREFIX = "latency:count:";
    public static final int LATENCY_SAMPLE_WINDOW = 500;
    // Latency is recorded in MICROSECONDS: on a single host produce->consume is often sub-millisecond,
    // which would floor to 0 if measured in whole milliseconds. Demo hygiene: samples above this
    // ceiling (60s) are outliers (backlog replay after a restart, or a hand-produced event carrying a
    // stale timestamp) and are excluded from the latency percentiles so they do not wreck the
    // comparison panel. Throughput and processed counts still include every event.
    public static final long LATENCY_MAX_SAMPLE_US = 60_000_000L;

    public static String latencySamplesKey(String lane) {
        return LATENCY_SAMPLES_KEY_PREFIX + lane;
    }

    public static String latencyRateKey(String lane) {
        return LATENCY_RATE_KEY_PREFIX + lane;
    }

    public static String latencyCountKey(String lane) {
        return LATENCY_COUNT_KEY_PREFIX + lane;
    }

    public static final List<String> TRANSACTION_CATEGORIES = List.of(
            "payroll",
            "wire",
            "pos",
            "ach",
            "internal"
    );
    public static final List<String> TRANSACTION_REGIONS = List.of(
            "northeast",
            "southeast",
            "west",
            "midwest"
    );

    private Constants() {}
}
