package io.redis.devrel.demo.eda.domain;

public record Transaction(
        String txnId,
        double amount,
        String category,
        String region,
        int riskScore,
        long timestamp
) {
    public boolean isHighRisk() {
        return riskScore > 80;
    }

    public String shortId() {
        return txnId.length() <= 8 ? txnId : txnId.substring(0, 8);
    }
}
