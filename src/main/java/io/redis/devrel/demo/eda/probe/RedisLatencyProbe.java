package io.redis.devrel.demo.eda.probe;

import io.redis.devrel.demo.eda.domain.Transaction;
import io.redis.devrel.demo.eda.domain.TransactionCodec;
import io.redis.devrel.demo.eda.runtime.RuntimeSupport;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.params.XReadGroupParams;
import redis.clients.jedis.resps.StreamEntry;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static io.redis.devrel.demo.eda.domain.Constants.LANE_REDIS;
import static io.redis.devrel.demo.eda.domain.Constants.LATENCY_SAMPLE_WINDOW;
import static io.redis.devrel.demo.eda.domain.Constants.PROBE_CONSUMER_NAME_DEFAULT;
import static io.redis.devrel.demo.eda.domain.Constants.PROBE_CONSUMER_NAME_ENV;
import static io.redis.devrel.demo.eda.domain.Constants.REDIS_PROBE_GROUP_NAME;
import static io.redis.devrel.demo.eda.domain.Constants.TRANSACTIONS_STREAM_KEY;

/**
 * Lane A timing probe. Reads the native {@code transactions} Redis Stream with XREADGROUP and
 * records the end-to-end latency of each event: the difference between the time it is consumed
 * here and the producer timestamp carried on the event. This is the Redis-native counterpart of
 * {@link KafkaLatencyProbe}; the two are intentionally near-identical so the only real difference
 * between the lanes is the transport.
 */
public final class RedisLatencyProbe {
    private static final int READ_COUNT = 50;
    private static final int BLOCK_MS = 1_000;

    private final UnifiedJedis jedis;
    private final TransactionCodec txCodec;
    private final RuntimeSupport runtimeSupport;
    private final LatencyRecorder recorder;
    private final String consumerName;

    public RedisLatencyProbe(
            UnifiedJedis jedis,
            TransactionCodec txCodec,
            RuntimeSupport runtimeSupport,
            LatencyRecorder recorder,
            String consumerName
    ) {
        this.jedis = Objects.requireNonNull(jedis, "jedis must not be null");
        this.txCodec = Objects.requireNonNull(txCodec, "txCodec must not be null");
        this.runtimeSupport = Objects.requireNonNull(runtimeSupport, "runtimeSupport must not be null");
        this.recorder = Objects.requireNonNull(recorder, "recorder must not be null");
        this.consumerName = Objects.requireNonNull(consumerName, "consumerName must not be null");
    }

    public void run() throws InterruptedException {
        jedis.ping();
        createConsumerGroup();
        runtimeSupport.writeHeartbeat();
        System.out.printf(
                "Redis latency probe ready on group %s as %s%n",
                REDIS_PROBE_GROUP_NAME,
                consumerName
        );

        XReadGroupParams params = XReadGroupParams.xReadGroupParams().count(READ_COUNT).block(BLOCK_MS);

        while (!Thread.currentThread().isInterrupted()) {
            List<Map.Entry<String, List<StreamEntry>>> rawEntries = jedis.xreadGroup(
                    REDIS_PROBE_GROUP_NAME,
                    consumerName,
                    params,
                    Map.of(TRANSACTIONS_STREAM_KEY, StreamEntryID.XREADGROUP_UNDELIVERED_ENTRY)
            );

            if (rawEntries != null) {
                for (Map.Entry<String, List<StreamEntry>> streamData : rawEntries) {
                    for (StreamEntry entry : streamData.getValue()) {
                        try {
                            Transaction transaction = txCodec.fromFields(entry.getFields());
                            long latencyMicros = LatencyRecorder.nowMicros() - transaction.producedMicros();
                            recorder.record(latencyMicros);
                        } catch (RuntimeException e) {
                            System.err.printf(
                                    "Skipping malformed entry %s: %s%n",
                                    entry.getID(),
                                    e.getMessage()
                            );
                        } finally {
                            // Always acknowledge so a malformed entry cannot linger as poison-pending.
                            jedis.xack(TRANSACTIONS_STREAM_KEY, REDIS_PROBE_GROUP_NAME, entry.getID());
                        }
                    }
                }
            }

            recorder.tick();
            runtimeSupport.writeHeartbeat();
        }
    }

    /**
     * Create the probe's consumer group starting at the tail of the stream so the timing view
     * reflects live latency rather than the backlog that accumulated before the probe started.
     */
    private void createConsumerGroup() {
        try {
            jedis.xgroupCreate(
                    TRANSACTIONS_STREAM_KEY,
                    REDIS_PROBE_GROUP_NAME,
                    StreamEntryID.LAST_ENTRY,
                    true
            );
        } catch (JedisDataException e) {
            if (!e.getMessage().contains("BUSYGROUP")) {
                throw e;
            }
        }
    }

    private static String loadConsumerName() {
        String raw = System.getenv(PROBE_CONSUMER_NAME_ENV);
        if (raw != null && !raw.isBlank()) {
            return raw.trim();
        }
        return PROBE_CONSUMER_NAME_DEFAULT;
    }

    public static void main(String[] args) throws InterruptedException {
        RuntimeSupport runtimeSupport = new RuntimeSupport();
        try (UnifiedJedis jedis = runtimeSupport.createJedisFromEnv()) {
            LatencyRecorder recorder = new LatencyRecorder(jedis, LANE_REDIS, LATENCY_SAMPLE_WINDOW);
            RedisLatencyProbe probe = new RedisLatencyProbe(
                    jedis,
                    new TransactionCodec(),
                    runtimeSupport,
                    recorder,
                    loadConsumerName()
            );
            probe.run();
        }
    }
}
