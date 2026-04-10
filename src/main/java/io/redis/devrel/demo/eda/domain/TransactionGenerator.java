package io.redis.devrel.demo.eda.domain;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static io.redis.devrel.demo.eda.domain.Constants.TRANSACTION_CATEGORIES;
import static io.redis.devrel.demo.eda.domain.Constants.TRANSACTION_REGIONS;

public final class TransactionGenerator {
    public Transaction nextTransaction() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        String category = pick(TRANSACTION_CATEGORIES, random);
        String region = pick(TRANSACTION_REGIONS, random);
        double amount = amountFor(category, random);
        int riskScore = riskFor(category, region, amount, random);

        return new Transaction(
                "TXN-" + randomHex(random, 6),
                amount,
                category,
                region,
                riskScore,
                System.currentTimeMillis()
        );
    }

    private static String pick(List<String> values, ThreadLocalRandom random) {
        return values.get(random.nextInt(values.size()));
    }

    private static String randomHex(ThreadLocalRandom random, int length) {
        StringBuilder builder = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            builder.append(Integer.toHexString(random.nextInt(16)));
        }
        return builder.toString();
    }

    private static double amountFor(String category, ThreadLocalRandom random) {
        double amount;
        switch (category) {
            case "payroll" -> amount = random.nextDouble(1_250.0, 8_500.0);
            case "wire" -> amount = random.nextDouble(2_000.0, 60_000.0);
            case "pos" -> amount = random.nextDouble(8.0, 900.0);
            case "ach" -> amount = random.nextDouble(80.0, 7_500.0);
            case "internal" -> amount = random.nextDouble(250.0, 18_000.0);
            default -> amount = random.nextDouble(100.0, 1_000.0);
        }

        if ("wire".equals(category) && random.nextDouble() < 0.12) {
            amount += random.nextDouble(15_000.0, 35_000.0);
        }

        if ("internal".equals(category) && random.nextDouble() < 0.08) {
            amount += random.nextDouble(8_000.0, 18_000.0);
        }

        return Math.round(amount * 100.0) / 100.0;
    }

    private static int riskFor(String category, String region, double amount, ThreadLocalRandom random) {
        int risk = random.nextInt(5, 35);

        if ("wire".equals(category)) {
            risk += random.nextInt(18, 31);
        }
        if ("internal".equals(category)) {
            risk += random.nextInt(6, 18);
        }
        if ("southeast".equals(region)) {
            risk += random.nextInt(0, 10);
        }
        if (amount >= 10_000.0) {
            risk += random.nextInt(10, 25);
        }
        if (amount >= 40_000.0) {
            risk += random.nextInt(10, 20);
        }
        if (random.nextDouble() < 0.1) {
            risk += random.nextInt(15, 35);
        }

        return Math.max(0, Math.min(risk, 100));
    }

}
