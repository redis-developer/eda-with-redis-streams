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
        return fields;
    }

    public Transaction fromFields(Map<String, String> fields) {
        return new Transaction(
                fields.get("txn_id"),
                Double.parseDouble(fields.get("amount")),
                fields.get("category"),
                fields.get("region"),
                Integer.parseInt(fields.get("risk_score")),
                Long.parseLong(fields.get("timestamp"))
        );
    }
}
