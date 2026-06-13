package io.redis.devrel.demo.eda.consumer;

import io.redis.devrel.demo.eda.runtime.RuntimeSupport;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.params.XReadGroupParams;
import redis.clients.jedis.resps.StreamEntry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static io.redis.devrel.demo.eda.domain.Constants.TRANSLATOR_CONSUMER_NAME;
import static io.redis.devrel.demo.eda.domain.Constants.TRANSLATOR_CONSUMER_NAME_ENV;
import static io.redis.devrel.demo.eda.domain.Constants.TRANSLATOR_GROUP_NAME;
import static io.redis.devrel.demo.eda.domain.Constants.KORVET_TRANSACTIONS_STREAM_KEY;
import static io.redis.devrel.demo.eda.domain.Constants.STREAM_GROUP_START_ID;
import static io.redis.devrel.demo.eda.domain.Constants.TRANSACTIONS_STREAM_KEY;
import static redis.clients.jedis.StreamEntryID.XREADGROUP_UNDELIVERED_ENTRY;

/**
 * Bridges the Kafka-protocol path into the native Redis Streams pipeline.
 *
 * <p>This is the single component that is aware of Korvet's internal storage layout. It runs a
 * Redis Streams consumer group over the partition stream Korvet uses to back the Kafka topic
 * {@code transactions} ({@link io.redis.devrel.demo.eda.domain.Constants#KORVET_TRANSACTIONS_STREAM_KEY}),
 * and copies each record's {@code value} field verbatim into the demo's own
 * {@code transactions} stream ({@link io.redis.devrel.demo.eda.domain.Constants#TRANSACTIONS_STREAM_KEY}).
 * The native producer and the metrics/alerts/monitor consumers therefore stay Korvet-agnostic:
 * events produced over the Kafka protocol show up in the pipeline as ordinary native entries.
 *
 * <p>It forwards the opaque {@code value} payload as-is, so it is format-agnostic for the
 * plain-JSON path. Korvet must be configured with {@code storage.compression.type=none} (set in
 * docker-compose) so the value is readable rather than an LZ4 frame.
 */
public final class KorvetConsumer {
    private static final StreamEntryID PENDING_ID = new StreamEntryID(STREAM_GROUP_START_ID);
    private static final StreamEntryID NEW_ENTRY_ID = XREADGROUP_UNDELIVERED_ENTRY;
    private static final String HOSTNAME_ENV = "HOSTNAME";
    private static final String VALUE_FIELD = "value";
    private static final int READ_COUNT = 50;
    private static final int BLOCK_MS = 1_000;

    private final UnifiedJedis jedis;
    private final RuntimeSupport runtimeSupport;
    private final String consumerName;

    public KorvetConsumer(UnifiedJedis jedis, RuntimeSupport runtimeSupport, String consumerName) {
        this.jedis = Objects.requireNonNull(jedis, "jedis must not be null");
        this.runtimeSupport = Objects.requireNonNull(runtimeSupport, "runtimeSupport must not be null");
        this.consumerName = Objects.requireNonNull(consumerName, "consumerName must not be null");
    }

    public void run() {
        long forwarded = 0L;

        try {
            jedis.ping();
            createConsumerGroup();
            runtimeSupport.writeHeartbeat();
            System.out.printf(
                    "Korvet consumer ready: %s -> %s on group %s as %s%n",
                    KORVET_TRANSACTIONS_STREAM_KEY,
                    TRANSACTIONS_STREAM_KEY,
                    TRANSLATOR_GROUP_NAME,
                    consumerName
            );

            // Drain anything already delivered-but-unacked (e.g. after a restart) before tailing.
            forwarded += drainPending();

            while (!Thread.currentThread().isInterrupted()) {
                List<StreamMessage> entries = readGroup(NEW_ENTRY_ID, READ_COUNT, BLOCK_MS);
                if (!entries.isEmpty()) {
                    forwarded += forward(entries);
                    if (forwarded % 100 == 0) {
                        System.out.printf("Korvet consumer forwarded %,d events%n", forwarded);
                    }
                }
                runtimeSupport.writeHeartbeat();
            }
        } catch (Exception e) {
            throw new IllegalStateException("Korvet consumer failed", e);
        }
    }

    private void createConsumerGroup() {
        try {
            jedis.xgroupCreate(
                    KORVET_TRANSACTIONS_STREAM_KEY,
                    TRANSLATOR_GROUP_NAME,
                    new StreamEntryID(STREAM_GROUP_START_ID),
                    true
            );
        } catch (JedisDataException e) {
            if (!e.getMessage().contains("BUSYGROUP")) {
                throw e;
            }
        }
    }

    private long drainPending() {
        long forwarded = 0L;
        while (true) {
            List<StreamMessage> pending = readGroup(PENDING_ID, READ_COUNT, null);
            if (pending.isEmpty()) {
                return forwarded;
            }
            forwarded += forward(pending);
        }
    }

    private List<StreamMessage> readGroup(StreamEntryID streamEntryID, int count, Integer blockMs) {
        XReadGroupParams params = XReadGroupParams.xReadGroupParams().count(count);
        if (blockMs != null) {
            params.block(blockMs);
        }

        List<Map.Entry<String, List<StreamEntry>>> rawEntries = jedis.xreadGroup(
                TRANSLATOR_GROUP_NAME,
                consumerName,
                params,
                Map.of(KORVET_TRANSACTIONS_STREAM_KEY, streamEntryID)
        );

        if (rawEntries == null || rawEntries.isEmpty()) {
            return Collections.emptyList();
        }

        List<StreamMessage> entries = new ArrayList<>();
        for (Map.Entry<String, List<StreamEntry>> streamData : rawEntries) {
            for (StreamEntry entry : streamData.getValue()) {
                entries.add(new StreamMessage(entry.getID(), entry.getFields()));
            }
        }
        return entries;
    }

    private long forward(List<StreamMessage> entries) {
        long forwarded = 0L;
        for (StreamMessage entry : entries) {
            String value = entry.fields().get(VALUE_FIELD);
            if (value != null) {
                Map<String, String> fields = new LinkedHashMap<>();
                fields.put(VALUE_FIELD, value);
                jedis.xadd(TRANSACTIONS_STREAM_KEY, StreamEntryID.NEW_ENTRY, fields);
                forwarded++;
            }
            // Ack on the source stream regardless: a value-less record (tombstone) has nothing to
            // forward but must still be acknowledged so it is not redelivered.
            jedis.xack(KORVET_TRANSACTIONS_STREAM_KEY, TRANSLATOR_GROUP_NAME, entry.id());
        }
        runtimeSupport.writeHeartbeat();
        return forwarded;
    }

    private static String loadConsumerName() {
        String rawConsumerName = System.getenv(TRANSLATOR_CONSUMER_NAME_ENV);
        if (rawConsumerName != null && !rawConsumerName.isBlank()) {
            return rawConsumerName.trim();
        }

        String hostname = System.getenv(HOSTNAME_ENV);
        if (hostname != null && !hostname.isBlank()) {
            return hostname.trim();
        }

        return TRANSLATOR_CONSUMER_NAME;
    }

    public static void main(String[] args) {
        RuntimeSupport runtimeSupport = new RuntimeSupport();

        try (UnifiedJedis jedis = runtimeSupport.createJedisFromEnv()) {
            new KorvetConsumer(jedis, runtimeSupport, loadConsumerName()).run();
        }
    }

    private record StreamMessage(StreamEntryID id, Map<String, String> fields) {
    }
}
