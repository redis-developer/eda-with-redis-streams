package io.redis.devrel.demo.eda.domain;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

public final class TransactionCodec {
    public Map<String, String> toFields(Transaction transaction) {
        Map<String, String> fields = new LinkedHashMap<>();
        fields.put("txn_id", transaction.txnId());
        fields.put("amount", String.format(Locale.US, "%.2f", transaction.amount()));
        fields.put("category", transaction.category());
        fields.put("region", transaction.region());
        fields.put("risk_score", Integer.toString(transaction.riskScore()));
        fields.put("timestamp", Long.toString(transaction.timestamp()));
        fields.put("produced_micros", Long.toString(transaction.producedMicros()));
        return fields;
    }

    public Transaction fromFields(Map<String, String> fields) {
        long timestamp = Long.parseLong(fields.get("timestamp"));
        String producedMicrosRaw = fields.get("produced_micros");
        // Fall back to timestamp*1000 so events without the microsecond field (e.g. a message
        // hand-produced with the Kafka console producer) still yield a usable latency reading.
        long producedMicros = (producedMicrosRaw != null && !producedMicrosRaw.isBlank())
                ? Long.parseLong(producedMicrosRaw)
                : timestamp * 1_000L;
        return new Transaction(
                fields.get("txn_id"),
                Double.parseDouble(fields.get("amount")),
                fields.get("category"),
                fields.get("region"),
                Integer.parseInt(fields.get("risk_score")),
                timestamp,
                producedMicros
        );
    }
}
