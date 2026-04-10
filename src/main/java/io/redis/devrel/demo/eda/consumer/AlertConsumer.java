package io.redis.devrel.demo.eda.consumer;

import io.redis.devrel.demo.eda.domain.Transaction;
import io.redis.devrel.demo.eda.domain.TransactionCodec;
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
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import static io.redis.devrel.demo.eda.domain.Constants.ALERTS_CONSUMER_NAME;
import static io.redis.devrel.demo.eda.domain.Constants.ALERTS_CONSUMER_NAME_ENV;
import static io.redis.devrel.demo.eda.domain.Constants.ALERTS_GROUP_NAME;
import static io.redis.devrel.demo.eda.domain.Constants.ALERTS_STREAM_KEY;
import static io.redis.devrel.demo.eda.domain.Constants.STREAM_GROUP_START_ID;
import static io.redis.devrel.demo.eda.domain.Constants.TRANSACTIONS_STREAM_KEY;

import static redis.clients.jedis.StreamEntryID.XREADGROUP_UNDELIVERED_ENTRY;

public final class AlertConsumer {
    private static final StreamEntryID PENDING_ID = new StreamEntryID(STREAM_GROUP_START_ID);
    private static final StreamEntryID NEW_ENTRY_ID = XREADGROUP_UNDELIVERED_ENTRY;
    private static final String HOSTNAME_ENV = "HOSTNAME";
    private static final String MEMBER_SEPARATOR = "|";

    private static final long HIGH_RISK_WINDOW_MS = 8_000L;
    private static final int HIGH_RISK_COUNT_THRESHOLD = 15;
    private static final long HIGH_RISK_COOLDOWN_MS = 60_000L;

    private static final long WIRE_VOLUME_WINDOW_MS = 5_000L;
    private static final double WIRE_VOLUME_THRESHOLD = 1_000_000.0d;
    private static final long WIRE_VOLUME_COOLDOWN_MS = 60_000L;

    private static final long STATE_KEY_TTL_MS = 120_000L;
    private static final String HIGH_RISK_WINDOW_KEY = "internal:state:alerts:high_risk_window";
    private static final String WIRE_VOLUME_WINDOW_KEY = "internal:state:alerts:wire_volume_window";
    private static final String COOLDOWNS_KEY = "internal:state:alerts:cooldowns";

    private final UnifiedJedis jedis;
    private final TransactionCodec txCodec;
    private final RuntimeSupport runtimeSupport;
    private final String consumerName;

    public AlertConsumer(
            UnifiedJedis jedis,
            TransactionCodec txCodec,
            RuntimeSupport runtimeSupport,
            String consumerName
    ) {
        this.jedis = Objects.requireNonNull(jedis, "jedis must not be null");
        this.txCodec = Objects.requireNonNull(txCodec, "codec must not be null");
        this.runtimeSupport = Objects.requireNonNull(runtimeSupport, "runtimeSupport must not be null");
        this.consumerName = Objects.requireNonNull(consumerName, "consumerName must not be null");
    }

    public void run() {
        long processed = 0L;

        try {
            jedis.ping();
            createConsumerGroup();
            runtimeSupport.writeHeartbeat();
            System.out.printf(
                    "Alert consumer ready on group %s as %s%n",
                    ALERTS_GROUP_NAME,
                    consumerName
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
                    if (processed % 250 == 0) {
                        System.out.printf("Alert consumer evaluated %,d transactions%n", processed);
                    }
                } else {
                    Thread.sleep(200L);
                }

                runtimeSupport.writeHeartbeat();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            throw new IllegalStateException("Alert consumer failed", e);
        }
    }

    private void createConsumerGroup() {
        try {
            jedis.xgroupCreate(
                    TRANSACTIONS_STREAM_KEY,
                    ALERTS_GROUP_NAME,
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
                ALERTS_GROUP_NAME,
                consumerName,
                params,
                Map.of(TRANSACTIONS_STREAM_KEY, streamEntryID)
        );

        return parseEntries(rawEntries);
    }

    private long processEntries(List<StreamMessage> entries) {
        long processed = 0L;

        for (StreamMessage entry : entries) {
            Transaction transaction = txCodec.fromFields(entry.fields());
            evaluateAndAck(entry.id(), transaction);
            processed++;
        }

        runtimeSupport.writeHeartbeat();
        return processed;
    }

    private void evaluateAndAck(String entryId, Transaction transaction) {
        try {
            evaluateHighRiskCluster(entryId, transaction);
            evaluateWireVolumeSurge(entryId, transaction);
            jedis.xack(TRANSACTIONS_STREAM_KEY, ALERTS_GROUP_NAME, new StreamEntryID(entryId));
        } catch (RuntimeException e) {
            throw new IllegalStateException("Unable to evaluate alerts for " + entryId, e);
        }
    }

    private void evaluateHighRiskCluster(String entryId, Transaction transaction) {
        if (!transaction.isHighRisk()) {
            return;
        }

        long now = transaction.timestamp();
        long minScore = now - HIGH_RISK_WINDOW_MS;
        String member = transaction.region() + MEMBER_SEPARATOR + entryId;

        jedis.zadd(HIGH_RISK_WINDOW_KEY, now, member);
        trimWindow(HIGH_RISK_WINDOW_KEY, minScore);

        long observedCount = countByRegion(HIGH_RISK_WINDOW_KEY, minScore, now, transaction.region());
        if (observedCount < HIGH_RISK_COUNT_THRESHOLD) {
            return;
        }

        if (!acquireCooldown("high_risk_cluster", "global", HIGH_RISK_COOLDOWN_MS, now)) {
            return;
        }

        AlertEvent alert = new AlertEvent(
                "high_risk_cluster",
                "high",
                transaction.region(),
                transaction.category(),
                entryId,
                transaction.txnId(),
                formatObservedCount(observedCount),
                Integer.toString(HIGH_RISK_COUNT_THRESHOLD),
                formatWindowSeconds(HIGH_RISK_WINDOW_MS),
                String.format(
                        Locale.US,
                        "%d high-risk transactions in %s within %s seconds",
                        observedCount,
                        transaction.region(),
                        formatWindowSeconds(HIGH_RISK_WINDOW_MS)
                ),
                now
        );

        emitAlert(alert);
    }

    private void evaluateWireVolumeSurge(String entryId, Transaction transaction) {
        if (!"wire".equals(transaction.category())) {
            return;
        }

        long now = transaction.timestamp();
        long minScore = now - WIRE_VOLUME_WINDOW_MS;
        String member = transaction.region()
                + MEMBER_SEPARATOR
                + String.format(Locale.US, "%.2f", transaction.amount())
                + MEMBER_SEPARATOR
                + entryId;

        jedis.zadd(WIRE_VOLUME_WINDOW_KEY, now, member);
        trimWindow(WIRE_VOLUME_WINDOW_KEY, minScore);

        double observedVolume = sumAmountsByRegion(WIRE_VOLUME_WINDOW_KEY, minScore, now, transaction.region());
        if (observedVolume < WIRE_VOLUME_THRESHOLD) {
            return;
        }

        if (!acquireCooldown("wire_volume_surge", "global", WIRE_VOLUME_COOLDOWN_MS, now)) {
            return;
        }

        AlertEvent alert = new AlertEvent(
                "wire_volume_surge",
                "medium",
                transaction.region(),
                transaction.category(),
                entryId,
                transaction.txnId(),
                formatCurrency(observedVolume),
                formatCurrency(WIRE_VOLUME_THRESHOLD),
                formatWindowSeconds(WIRE_VOLUME_WINDOW_MS),
                String.format(
                        Locale.US,
                        "Wire volume reached %s in %s within %s seconds",
                        formatCurrency(observedVolume),
                        transaction.region(),
                        formatWindowSeconds(WIRE_VOLUME_WINDOW_MS)
                ),
                now
        );

        emitAlert(alert);
    }

    private void trimWindow(String key, long minScore) {
        jedis.zremrangeByScore(key, 0, minScore);
        jedis.pexpire(key, STATE_KEY_TTL_MS);
    }

    private long countByRegion(String key, long minScore, long maxScore, String region) {
        List<String> members = jedis.zrangeByScore(key, minScore, maxScore);
        long count = 0L;
        for (String member : members) {
            if (matchesRegion(member, region)) {
                count++;
            }
        }
        return count;
    }

    private double sumAmountsByRegion(String key, long minScore, long maxScore, String region) {
        List<String> members = jedis.zrangeByScore(key, minScore, maxScore);
        double total = 0.0d;

        for (String member : members) {
            if (!matchesRegion(member, region)) {
                continue;
            }

            String[] parts = member.split("\\|", 3);
            if (parts.length < 3) {
                continue;
            }

            total += Double.parseDouble(parts[1]);
        }

        return total;
    }

    private boolean acquireCooldown(String alertType, String scope, long cooldownMs, long now) {
        String cooldownField = alertType + ":" + scope;
        String rawLastTriggeredAt = jedis.hget(COOLDOWNS_KEY, cooldownField);

        if (rawLastTriggeredAt != null) {
            long lastTriggeredAt = Long.parseLong(rawLastTriggeredAt);
            if (now - lastTriggeredAt < cooldownMs) {
                return false;
            }
        }

        jedis.hset(COOLDOWNS_KEY, cooldownField, Long.toString(now));
        return true;
    }

    private void emitAlert(AlertEvent alert) {
        jedis.xadd(ALERTS_STREAM_KEY, StreamEntryID.NEW_ENTRY, alert.toFields());
        System.out.printf(
                "ALERT %-18s region=%-10s observed=%s threshold=%s%n",
                alert.alertType(),
                alert.region(),
                alert.observedValue(),
                alert.threshold()
        );
    }

    private static boolean matchesRegion(String member, String region) {
        return member.startsWith(region + MEMBER_SEPARATOR);
    }

    private static String formatObservedCount(long value) {
        return Long.toString(value);
    }

    private static String formatCurrency(double value) {
        return String.format(Locale.US, "%.2f", value);
    }

    private static String formatWindowSeconds(long windowMs) {
        if (windowMs % 1_000L == 0L) {
            return Long.toString(windowMs / 1_000L);
        }
        return String.format(Locale.US, "%.1f", windowMs / 1_000.0d);
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

    private record StreamMessage(String id, Map<String, String> fields) {
    }

    private record AlertEvent(
            String alertType,
            String severity,
            String region,
            String category,
            String sourceStreamEntryId,
            String sourceTxnId,
            String observedValue,
            String threshold,
            String windowSeconds,
            String message,
            long timestamp
    ) {
        Map<String, String> toFields() {
            Map<String, String> fields = new LinkedHashMap<>();
            fields.put("alert_id", "ALERT-" + UUID.randomUUID().toString().substring(0, 8));
            fields.put("alert_type", alertType);
            fields.put("severity", severity);
            fields.put("region", region);
            fields.put("category", category);
            fields.put("source_stream_entry_id", sourceStreamEntryId);
            fields.put("source_txn_id", sourceTxnId);
            fields.put("observed_value", observedValue);
            fields.put("threshold", threshold);
            fields.put("window_seconds", windowSeconds);
            fields.put("message", message);
            fields.put("timestamp", Long.toString(timestamp));
            return fields;
        }
    }

    private static String loadConsumerName() {
        String rawConsumerName = System.getenv(ALERTS_CONSUMER_NAME_ENV);
        if (rawConsumerName != null && !rawConsumerName.isBlank()) {
            return rawConsumerName.trim();
        }

        String hostname = System.getenv(HOSTNAME_ENV);
        if (hostname != null && !hostname.isBlank()) {
            return hostname.trim();
        }

        return ALERTS_CONSUMER_NAME;
    }

    public static void main(String[] args) {
        RuntimeSupport runtimeSupport = new RuntimeSupport();
        String consumerName = loadConsumerName();

        try (UnifiedJedis jedis = runtimeSupport.createJedisFromEnv()) {
            AlertConsumer consumer = new AlertConsumer(
                    jedis,
                    new TransactionCodec(),
                    runtimeSupport,
                    consumerName
            );
            consumer.run();
        }
    }
}
