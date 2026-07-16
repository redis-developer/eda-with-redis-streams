package io.redis.devrel.demo.eda.probe;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.redis.devrel.demo.eda.domain.Transaction;
import io.redis.devrel.demo.eda.domain.TransactionCodec;
import io.redis.devrel.demo.eda.runtime.RuntimeSupport;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import redis.clients.jedis.UnifiedJedis;

import java.lang.reflect.Type;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import static io.redis.devrel.demo.eda.domain.Constants.KAFKA_BOOTSTRAP_DEFAULT;
import static io.redis.devrel.demo.eda.domain.Constants.KAFKA_BOOTSTRAP_SERVERS_ENV;
import static io.redis.devrel.demo.eda.domain.Constants.KAFKA_PROBE_GROUP_DEFAULT;
import static io.redis.devrel.demo.eda.domain.Constants.KAFKA_PROBE_GROUP_ENV;
import static io.redis.devrel.demo.eda.domain.Constants.KAFKA_TOPIC_DEFAULT;
import static io.redis.devrel.demo.eda.domain.Constants.KAFKA_TOPIC_ENV;
import static io.redis.devrel.demo.eda.domain.Constants.LANE_KORVET;
import static io.redis.devrel.demo.eda.domain.Constants.LATENCY_SAMPLE_WINDOW;

/**
 * Lane B timing probe. Consumes the Korvet topic with a standard Kafka client and records the same
 * end-to-end latency metric as {@link RedisLatencyProbe}: consume time minus the producer timestamp
 * embedded in each event. It writes its samples to Redis through {@link LatencyRecorder} so the two
 * lanes surface side by side in the monitor. Using a normal consumer group ({@code subscribe}) is
 * the point — it is the same code an existing Kafka consumer would run against Apache Kafka.
 */
public final class KafkaLatencyProbe {
    private static final Type FIELDS_TYPE = new TypeToken<Map<String, String>>() { }.getType();
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(1_000);

    private final Consumer<String, String> consumer;
    private final TransactionCodec txCodec;
    private final RuntimeSupport runtimeSupport;
    private final LatencyRecorder recorder;
    private final Gson gson;
    private final String topic;

    public KafkaLatencyProbe(
            Consumer<String, String> consumer,
            TransactionCodec txCodec,
            RuntimeSupport runtimeSupport,
            LatencyRecorder recorder,
            Gson gson,
            String topic
    ) {
        this.consumer = Objects.requireNonNull(consumer, "consumer must not be null");
        this.txCodec = Objects.requireNonNull(txCodec, "txCodec must not be null");
        this.runtimeSupport = Objects.requireNonNull(runtimeSupport, "runtimeSupport must not be null");
        this.recorder = Objects.requireNonNull(recorder, "recorder must not be null");
        this.gson = Objects.requireNonNull(gson, "gson must not be null");
        this.topic = Objects.requireNonNull(topic, "topic must not be null");
    }

    public void run() {
        consumer.subscribe(List.of(topic));
        runtimeSupport.writeHeartbeat();
        System.out.printf("Kafka latency probe subscribed to topic '%s'%n", topic);

        try {
            while (!Thread.currentThread().isInterrupted()) {
                ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT);
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        Map<String, String> fields = gson.fromJson(record.value(), FIELDS_TYPE);
                        if (fields == null) {
                            continue;
                        }
                        Transaction transaction = txCodec.fromFields(fields);
                        long latencyMicros = LatencyRecorder.nowMicros() - transaction.producedMicros();
                        recorder.record(latencyMicros);
                    } catch (RuntimeException e) {
                        // A single malformed record (e.g. a hand-produced line during the demo)
                        // must not take down the probe — skip it and keep consuming.
                        System.err.printf(
                                "Skipping malformed record at offset %d: %s%n",
                                record.offset(),
                                e.getMessage()
                        );
                    }
                }

                recorder.tick();
                runtimeSupport.writeHeartbeat();
            }
        } catch (org.apache.kafka.common.errors.WakeupException e) {
            // Normal shutdown signal — fall through to close.
        } finally {
            consumer.close();
        }
    }

    private static Consumer<String, String> createConsumer(String bootstrapServers, String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "kafka-latency-probe");
        return new KafkaConsumer<>(props);
    }

    private static String loadBootstrapServers() {
        String raw = System.getenv(KAFKA_BOOTSTRAP_SERVERS_ENV);
        return (raw == null || raw.isBlank()) ? KAFKA_BOOTSTRAP_DEFAULT : raw.trim();
    }

    private static String loadTopic() {
        String raw = System.getenv(KAFKA_TOPIC_ENV);
        return (raw == null || raw.isBlank()) ? KAFKA_TOPIC_DEFAULT : raw.trim();
    }

    private static String loadGroupId() {
        String raw = System.getenv(KAFKA_PROBE_GROUP_ENV);
        return (raw == null || raw.isBlank()) ? KAFKA_PROBE_GROUP_DEFAULT : raw.trim();
    }

    public static void main(String[] args) {
        RuntimeSupport runtimeSupport = new RuntimeSupport();
        try (UnifiedJedis jedis = runtimeSupport.createJedisFromEnv()) {
            LatencyRecorder recorder = new LatencyRecorder(jedis, LANE_KORVET, LATENCY_SAMPLE_WINDOW);
            KafkaLatencyProbe probe = new KafkaLatencyProbe(
                    createConsumer(loadBootstrapServers(), loadGroupId()),
                    new TransactionCodec(),
                    runtimeSupport,
                    recorder,
                    new Gson(),
                    loadTopic()
            );
            probe.run();
        }
    }
}
