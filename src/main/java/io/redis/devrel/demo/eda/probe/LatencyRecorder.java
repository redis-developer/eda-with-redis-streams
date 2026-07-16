package io.redis.devrel.demo.eda.probe;

import redis.clients.jedis.UnifiedJedis;

import java.time.Instant;
import java.util.Objects;

import static io.redis.devrel.demo.eda.domain.Constants.LATENCY_MAX_SAMPLE_US;
import static io.redis.devrel.demo.eda.domain.Constants.latencyCountKey;
import static io.redis.devrel.demo.eda.domain.Constants.latencyRateKey;
import static io.redis.devrel.demo.eda.domain.Constants.latencySamplesKey;

/**
 * Publishes per-lane latency telemetry into Redis so the monitor API can render a live comparison.
 *
 * <p>For each processed event it records the end-to-end latency (in MICROSECONDS) into a capped
 * list, bumps a running processed-count, and — at most once per second — writes the current
 * throughput as a gauge. Keeping the rate calculation here (rather than deriving it from the
 * dashboard poll cadence) makes the reported throughput accurate and independent of who is watching.
 */
public final class LatencyRecorder {
    private final UnifiedJedis jedis;
    private final String samplesKey;
    private final String rateKey;
    private final String countKey;
    private final int sampleWindow;

    private long windowStartMs;
    private long windowCount;

    public LatencyRecorder(UnifiedJedis jedis, String lane, int sampleWindow) {
        this.jedis = Objects.requireNonNull(jedis, "jedis must not be null");
        Objects.requireNonNull(lane, "lane must not be null");
        this.samplesKey = latencySamplesKey(lane);
        this.rateKey = latencyRateKey(lane);
        this.countKey = latencyCountKey(lane);
        this.sampleWindow = sampleWindow;
        this.windowStartMs = System.currentTimeMillis();
        this.windowCount = 0L;
    }

    /** Current wall-clock time as epoch microseconds (comparable across containers on one host). */
    public static long nowMicros() {
        Instant now = Instant.now();
        return now.getEpochSecond() * 1_000_000L + now.getNano() / 1_000L;
    }

    /** Record one processed event given its end-to-end latency in microseconds. */
    public void record(long latencyMicros) {
        long clamped = Math.max(0L, latencyMicros);
        // Throughput and processed count reflect every consumed event; only the latency
        // percentiles exclude outliers so a backlog replay or stale timestamp cannot skew them.
        if (clamped <= LATENCY_MAX_SAMPLE_US) {
            jedis.lpush(samplesKey, Long.toString(clamped));
            jedis.ltrim(samplesKey, 0, sampleWindow - 1L);
        }
        jedis.incr(countKey);
        windowCount++;
        maybeFlushRate();
    }

    /**
     * Call once per loop iteration so the throughput gauge decays to zero when a lane goes quiet
     * (for example when its producer is stopped during the demo).
     */
    public void tick() {
        maybeFlushRate();
    }

    private void maybeFlushRate() {
        long now = System.currentTimeMillis();
        long elapsed = now - windowStartMs;
        if (elapsed >= 1_000L) {
            long ratePerSecond = Math.round(windowCount * 1_000.0 / elapsed);
            jedis.set(rateKey, Long.toString(ratePerSecond));
            windowStartMs = now;
            windowCount = 0L;
        }
    }
}
