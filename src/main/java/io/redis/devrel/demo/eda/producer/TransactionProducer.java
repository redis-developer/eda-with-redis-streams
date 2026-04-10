package io.redis.devrel.demo.eda.producer;

import io.redis.devrel.demo.eda.runtime.RuntimeSupport;
import io.redis.devrel.demo.eda.domain.TransactionGenerator;
import io.redis.devrel.demo.eda.domain.Transaction;
import io.redis.devrel.demo.eda.domain.TransactionCodec;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.UnifiedJedis;

import java.util.Objects;

import static io.redis.devrel.demo.eda.domain.Constants.PRODUCER_DEFAULT_RATE_PER_SECOND;
import static io.redis.devrel.demo.eda.domain.Constants.PRODUCER_MAX_RATE_PER_SECOND;
import static io.redis.devrel.demo.eda.domain.Constants.PRODUCER_RATE_PER_SECOND_ENV;
import static io.redis.devrel.demo.eda.domain.Constants.TRANSACTIONS_STREAM_KEY;

public final class TransactionProducer {
    private final UnifiedJedis jedis;
    private final TransactionGenerator txGenerator;
    private final TransactionCodec txCodec;
    private final RuntimeSupport runtimeSupport;
    private final int producerRatePerSecond;

    public TransactionProducer(
            UnifiedJedis jedis,
            TransactionGenerator txGenerator,
            TransactionCodec txCodec,
            RuntimeSupport runtimeSupport,
            int producerRatePerSecond
    ) {
        this.jedis = Objects.requireNonNull(jedis, "jedis must not be null");
        this.txGenerator = Objects.requireNonNull(txGenerator, "generator must not be null");
        this.txCodec = Objects.requireNonNull(txCodec, "codec must not be null");
        this.runtimeSupport = Objects.requireNonNull(runtimeSupport, "runtimeSupport must not be null");
        this.producerRatePerSecond = producerRatePerSecond;
    }

    public void run() throws InterruptedException {
        jedis.ping();
        runtimeSupport.writeHeartbeat();
        System.out.printf(
                "Producer connected and publishing transactions at %d msg/s%n",
                producerRatePerSecond
        );

        long publishedSinceStartup = 0L;

        while (!Thread.currentThread().isInterrupted()) {
            Transaction transaction = txGenerator.nextTransaction();
            StreamEntryID entryId =
                    jedis.xadd(TRANSACTIONS_STREAM_KEY, StreamEntryID.NEW_ENTRY, txCodec.toFields(transaction));
            publishedSinceStartup++;

            if (publishedSinceStartup == 1 || publishedSinceStartup % 25 == 0) {
                long streamLength = jedis.xlen(TRANSACTIONS_STREAM_KEY);
                System.out.printf(
                        "Published %,d transactions. Latest stream entry: %s (%s, $%,.2f)%n",
                        streamLength,
                        entryId,
                        transaction.category(),
                        transaction.amount()
                );
            }

            runtimeSupport.writeHeartbeat();
            Thread.sleep(computeSleepMs(producerRatePerSecond));
        }
    }

    private static long computeSleepMs(int ratePerSecond) {
        return Math.max(1L, 1_000L / ratePerSecond);
    }

    private static int loadProducerRatePerSecond() {
        String raw = System.getenv(PRODUCER_RATE_PER_SECOND_ENV);
        if (raw == null || raw.isBlank()) {
            return PRODUCER_DEFAULT_RATE_PER_SECOND;
        }

        int parsedRate;
        try {
            parsedRate = Integer.parseInt(raw);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    PRODUCER_RATE_PER_SECOND_ENV + " must be an integer between 1 and "
                            + PRODUCER_MAX_RATE_PER_SECOND,
                    e
            );
        }

        if (parsedRate < 1 || parsedRate > PRODUCER_MAX_RATE_PER_SECOND) {
            throw new IllegalArgumentException(
                    PRODUCER_RATE_PER_SECOND_ENV + " must be between 1 and "
                            + PRODUCER_MAX_RATE_PER_SECOND
            );
        }

        return parsedRate;
    }

    public static void main(String[] args) throws InterruptedException {
        RuntimeSupport runtimeSupport = new RuntimeSupport();
        int producerRatePerSecond = loadProducerRatePerSecond();

        try (UnifiedJedis jedis = runtimeSupport.createJedisFromEnv()) {
            TransactionProducer producer = new TransactionProducer(
                    jedis,
                    new TransactionGenerator(),
                    new TransactionCodec(),
                    runtimeSupport,
                    producerRatePerSecond
            );
            producer.run();
        }
    }
}
