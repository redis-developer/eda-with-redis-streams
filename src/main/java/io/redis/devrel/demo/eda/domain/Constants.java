package io.redis.devrel.demo.eda.domain;

import java.util.List;

public final class Constants {
    public static final String REDIS_HOST_ENV = "REDIS_HOST";
    public static final String REDIS_PORT_ENV = "REDIS_PORT";
    public static final String REDIS_DEFAULT_HOST = "localhost";
    public static final String REDIS_DEFAULT_PORT = "6379";
    public static final String STREAM_GROUP_START_ID = "0-0";
    public static final String HEALTH_FILE_PATH = "/tmp/healthy";

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

    public static final String MONITOR_GROUP_NAME = "monitor-cg";
    public static final String MONITOR_CONSUMER_NAME_ENV = "MONITOR_CONSUMER_NAME";
    public static final String MONITOR_CONSUMER_NAME = "monitor-web-ui";

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
