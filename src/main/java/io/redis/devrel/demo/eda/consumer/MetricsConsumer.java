package io.redis.devrel.demo.eda.consumer;

import io.redis.devrel.demo.eda.runtime.RuntimeSupport;
import io.redis.devrel.demo.eda.domain.Transaction;
import io.redis.devrel.demo.eda.domain.TransactionCodec;
import redis.clients.jedis.AbstractTransaction;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.json.JsonSetParams;
import redis.clients.jedis.json.Path2;
import redis.clients.jedis.params.XReadGroupParams;
import redis.clients.jedis.resps.StreamEntry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static io.redis.devrel.demo.eda.domain.Constants.METRICS_CONSUMER_NAME_ENV;
import static io.redis.devrel.demo.eda.domain.Constants.METRICS_CONSUMER_NAME;
import static io.redis.devrel.demo.eda.domain.Constants.METRICS_COUNT_BY_REGION_KEY;
import static io.redis.devrel.demo.eda.domain.Constants.METRICS_GROUP_NAME;
import static io.redis.devrel.demo.eda.domain.Constants.METRICS_HIGH_RISK_COUNT_KEY;
import static io.redis.devrel.demo.eda.domain.Constants.METRICS_PROCESSING_DELAY_MS_ENV;
import static io.redis.devrel.demo.eda.domain.Constants.METRICS_TOTAL_COUNT_KEY;
import static io.redis.devrel.demo.eda.domain.Constants.METRICS_TOTAL_VOLUME_KEY;
import static io.redis.devrel.demo.eda.domain.Constants.METRICS_VOLUME_BY_CATEGORY_KEY;
import static io.redis.devrel.demo.eda.domain.Constants.TRANSACTIONS_STREAM_KEY;
import static io.redis.devrel.demo.eda.domain.Constants.STREAM_GROUP_START_ID;
import static io.redis.devrel.demo.eda.domain.Constants.TRANSACTION_REGIONS;
import static redis.clients.jedis.StreamEntryID.XREADGROUP_UNDELIVERED_ENTRY;

public final class MetricsConsumer {
    private static final StreamEntryID PENDING_ID = new StreamEntryID(STREAM_GROUP_START_ID);
    private static final StreamEntryID NEW_ENTRY_ID = XREADGROUP_UNDELIVERED_ENTRY;
    private static final String HOSTNAME_ENV = "HOSTNAME";

    private final UnifiedJedis jedis;
    private final TransactionCodec txCodec;
    private final RuntimeSupport runtimeSupport;
    private final String consumerName;
    private final long processingDelayMs;

    public MetricsConsumer(
            UnifiedJedis jedis,
            TransactionCodec txCodec,
            RuntimeSupport runtimeSupport,
            String consumerName,
            long processingDelayMs
    ) {
        this.jedis = Objects.requireNonNull(jedis, "jedis must not be null");
        this.txCodec = Objects.requireNonNull(txCodec, "codec must not be null");
        this.runtimeSupport = Objects.requireNonNull(runtimeSupport, "runtimeSupport must not be null");
        this.consumerName = Objects.requireNonNull(consumerName, "consumerName must not be null");
        this.processingDelayMs = processingDelayMs;
    }

    public void run() {
        long processed = 0L;

        try {
            jedis.ping();
            createConsumerGroup();
            ensureRegionCounts();
            runtimeSupport.writeHeartbeat();
            System.out.printf(
                    "Metrics consumer ready on group %s as %s (delay=%dms)%n",
                    METRICS_GROUP_NAME,
                    consumerName,
                    processingDelayMs
            );

            while (!Thread.currentThread().isInterrupted()) {
                List<StreamMessage> pendingEntries = readGroup(PENDING_ID, 10);
                if (!pendingEntries.isEmpty()) {
                    processed += processEntries(pendingEntries);
                    continue;
                }

                List<StreamMessage> newEntries = readGroup(NEW_ENTRY_ID, 10);

                if (!newEntries.isEmpty()) {
                    processed += processEntries(newEntries);
                    if (processed % 100 == 0) {
                        System.out.printf("Metrics consumer processed %,d transactions%n", processed);
                    }
                } else {
                    Thread.sleep(200L);
                }

                runtimeSupport.writeHeartbeat();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            throw new IllegalStateException("Metrics consumer failed", e);
        }
    }

    private void createConsumerGroup() {
        try {
            jedis.xgroupCreate(
                    TRANSACTIONS_STREAM_KEY,
                    METRICS_GROUP_NAME,
                    new StreamEntryID(STREAM_GROUP_START_ID),
                    true
            );
        } catch (JedisDataException e) {
            if (!e.getMessage().contains("BUSYGROUP")) {
                throw e;
            }
        }
    }

    private List<StreamMessage> readGroup(StreamEntryID streamEntryID, int count) {
        XReadGroupParams params = XReadGroupParams.xReadGroupParams().count(count);

        List<Map.Entry<String, List<StreamEntry>>> rawEntries = jedis.xreadGroup(
                METRICS_GROUP_NAME,
                consumerName,
                params,
                Map.of(TRANSACTIONS_STREAM_KEY, streamEntryID)
        );

        return parseEntries(rawEntries);
    }

    private long processEntries(List<StreamMessage> entries) throws InterruptedException {
        long processed = 0L;

        for (StreamMessage entry : entries) {
            Transaction transaction = txCodec.fromFields(entry.fields());
            applyMetricsAndAckAtomically(entry.id(), transaction);
            maybeDelay();
            processed++;
        }

        runtimeSupport.writeHeartbeat();
        return processed;
    }

    private void applyMetricsAndAckAtomically(String entryId, Transaction transaction) {
        try (AbstractTransaction redisTx = jedis.multi()) {
            redisTx.incr(METRICS_TOTAL_COUNT_KEY);
            redisTx.incrByFloat(METRICS_TOTAL_VOLUME_KEY, transaction.amount());
            redisTx.zincrby(
                    METRICS_VOLUME_BY_CATEGORY_KEY,
                    transaction.amount(),
                    transaction.category()
            );
            redisTx.jsonNumIncrBy(
                    METRICS_COUNT_BY_REGION_KEY,
                    Path2.of("$." + transaction.region()),
                    1.0d
            );
            if (transaction.isHighRisk()) {
                redisTx.incr(METRICS_HIGH_RISK_COUNT_KEY);
            }
            redisTx.xack(TRANSACTIONS_STREAM_KEY, METRICS_GROUP_NAME, new StreamEntryID(entryId));
            redisTx.exec();
        }
    }

    private static List<StreamMessage> parseEntries(List<Map.Entry<String, List<StreamEntry>>> rawEntries) {
        if (rawEntries == null || rawEntries.isEmpty()) {
            return Collections.emptyList();
        }

        List<StreamMessage> entries = new ArrayList<>();

        for (Map.Entry<String, List<StreamEntry>> streamData : rawEntries) {
            for (StreamEntry streamEntry : streamData.getValue()) {
                entries.add(new StreamMessage(streamEntry.getID().toString(), streamEntry.getFields()));
            }
        }

        return entries;
    }

    private void ensureRegionCounts() {
        String currentType = jedis.type(METRICS_COUNT_BY_REGION_KEY);
        boolean isJsonValue = currentType.toLowerCase().contains("json");

        if (!"none".equals(currentType) && !isJsonValue) {
            jedis.del(METRICS_COUNT_BY_REGION_KEY);
        }

        if ("none".equals(currentType) || !isJsonValue) {
            jedis.jsonSetWithEscape(METRICS_COUNT_BY_REGION_KEY, Path2.ROOT_PATH, Map.of());
        }

        for (String region : TRANSACTION_REGIONS) {
            jedis.jsonSet(
                    METRICS_COUNT_BY_REGION_KEY,
                    Path2.of("$." + region),
                    0,
                    JsonSetParams.jsonSetParams().nx()
            );
        }
    }

    private record StreamMessage(String id, Map<String, String> fields) {
    }

    private static String loadConsumerName() {
        String rawConsumerName = System.getenv(METRICS_CONSUMER_NAME_ENV);
        if (rawConsumerName != null && !rawConsumerName.isBlank()) {
            return rawConsumerName.trim();
        }

        String hostname = System.getenv(HOSTNAME_ENV);
        if (hostname != null && !hostname.isBlank()) {
            return hostname.trim();
        }

        return METRICS_CONSUMER_NAME;
    }

    private static long loadProcessingDelayMs() {
        String rawDelay = System.getenv(METRICS_PROCESSING_DELAY_MS_ENV);
        if (rawDelay == null || rawDelay.isBlank()) {
            return 0L;
        }

        try {
            long parsedDelay = Long.parseLong(rawDelay.trim());
            if (parsedDelay < 0L) {
                throw new IllegalArgumentException(
                        METRICS_PROCESSING_DELAY_MS_ENV + " must be 0 or greater"
                );
            }
            return parsedDelay;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    METRICS_PROCESSING_DELAY_MS_ENV + " must be an integer number of milliseconds",
                    e
            );
        }
    }

    private void maybeDelay() throws InterruptedException {
        if (processingDelayMs > 0L) {
            Thread.sleep(processingDelayMs);
        }
    }

    public static void main(String[] args) {
        RuntimeSupport runtimeSupport = new RuntimeSupport();
        String consumerName = loadConsumerName();
        long processingDelayMs = loadProcessingDelayMs();

        try (UnifiedJedis jedis = runtimeSupport.createJedisFromEnv()) {
            MetricsConsumer consumer = new MetricsConsumer(
                    jedis,
                    new TransactionCodec(),
                    runtimeSupport,
                    consumerName,
                    processingDelayMs
            );
            consumer.run();
        }
    }
}
