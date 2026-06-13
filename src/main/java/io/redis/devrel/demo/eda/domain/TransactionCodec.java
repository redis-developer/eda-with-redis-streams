package io.redis.devrel.demo.eda.domain;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Encodes a {@link Transaction} as a single Kafka-compatible {@code value} field holding a JSON
 * document, and decodes it back.
 *
 * <p>This layout matches how Korvet (the Kafka-on-Redis adapter) stores a Kafka record in a Redis
 * Stream entry: the record value lands verbatim in a {@code value} field. Because both this native
 * producer and a Kafka client producing through Korvet write the same {@code value} JSON into the
 * same partition stream, the downstream consumers process events identically regardless of which
 * path produced them.
 *
 * <p>{@link #fromFields(Map)} is lenient: it tolerates a missing/blank timestamp (defaulting to
 * now) and falls back to the original flat-field layout ({@code txn_id}, {@code amount}, ...) so
 * older entries still parse.
 */
public final class TransactionCodec {
    private static final String VALUE_FIELD = "value";

    public Map<String, String> toFields(Transaction transaction) {
        JsonObject json = new JsonObject();
        json.addProperty("txnId", transaction.txnId());
        json.addProperty("amount", transaction.amount());
        json.addProperty("category", transaction.category());
        json.addProperty("region", transaction.region());
        json.addProperty("riskScore", transaction.riskScore());
        json.addProperty("timestamp", transaction.timestamp());

        Map<String, String> fields = new LinkedHashMap<>();
        fields.put(VALUE_FIELD, json.toString());
        return fields;
    }

    public Transaction fromFields(Map<String, String> fields) {
        String rawValue = fields.get(VALUE_FIELD);
        if (rawValue != null && !rawValue.isBlank()) {
            return fromJson(rawValue);
        }
        if (fields.containsKey("txn_id")) {
            return fromFlatFields(fields);
        }
        throw new IllegalArgumentException(
                "Stream entry has neither a 'value' JSON field nor legacy flat fields"
        );
    }

    private Transaction fromJson(String rawValue) {
        JsonObject json = JsonParser.parseString(rawValue).getAsJsonObject();

        long timestamp = (long) getDouble(json, "timestamp", 0.0d);
        if (timestamp <= 0L) {
            timestamp = System.currentTimeMillis();
        }

        return new Transaction(
                getString(json, "txnId", "TXN-unknown"),
                getDouble(json, "amount", 0.0d),
                getString(json, "category", "uncategorized"),
                getString(json, "region", "unknown"),
                (int) getDouble(json, "riskScore", 0.0d),
                timestamp
        );
    }

    private Transaction fromFlatFields(Map<String, String> fields) {
        return new Transaction(
                fields.get("txn_id"),
                Double.parseDouble(fields.get("amount")),
                fields.get("category"),
                fields.get("region"),
                Integer.parseInt(fields.get("risk_score")),
                Long.parseLong(fields.get("timestamp"))
        );
    }

    private static String getString(JsonObject json, String key, String fallback) {
        return json.has(key) && !json.get(key).isJsonNull() ? json.get(key).getAsString() : fallback;
    }

    private static double getDouble(JsonObject json, String key, double fallback) {
        return json.has(key) && !json.get(key).isJsonNull() ? json.get(key).getAsDouble() : fallback;
    }
}
