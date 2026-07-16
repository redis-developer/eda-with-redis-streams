package io.redis.devrel.demo.eda.producer;

import com.google.gson.Gson;
import io.redis.devrel.demo.eda.domain.Transaction;
import io.redis.devrel.demo.eda.domain.TransactionCodec;
import io.redis.devrel.demo.eda.domain.TransactionGenerator;
import io.redis.devrel.demo.eda.runtime.RuntimeSupport;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Objects;
import java.util.Properties;

import static io.redis.devrel.demo.eda.domain.Constants.KAFKA_BOOTSTRAP_DEFAULT;
import static io.redis.devrel.demo.eda.domain.Constants.KAFKA_BOOTSTRAP_SERVERS_ENV;
import static io.redis.devrel.demo.eda.domain.Constants.KAFKA_TOPIC_DEFAULT;
import static io.redis.devrel.demo.eda.domain.Constants.KAFKA_TOPIC_ENV;
import static io.redis.devrel.demo.eda.domain.Constants.PRODUCER_DEFAULT_RATE_PER_SECOND;
import static io.redis.devrel.demo.eda.domain.Constants.PRODUCER_MAX_RATE_PER_SECOND;
import static io.redis.devrel.demo.eda.domain.Constants.PRODUCER_RATE_PER_SECOND_ENV;

/**
 * Lane B producer. Generates the exact same synthetic transactions as {@link TransactionProducer}
 * but publishes them over the Kafka protocol to a Korvet topic using a standard Kafka client. The
 * only thing that makes this "Kafka" rather than "Redis" is the client and {@code bootstrap.servers}
 * — the broker is Korvet, which stores the topic as a Redis Stream.
 */
public final class KafkaTransactionProducer {
    private final Producer<String, String> producer;
    private final TransactionGenerator txGenerator;
    private final TransactionCodec txCodec;
    private final RuntimeSupport runtimeSupport;
    private final Gson gson;
    private final String topic;
    private final int ratePerSecond;

    public KafkaTransactionProducer(
            Producer<String, String> producer,
            TransactionGenerator txGenerator,
            TransactionCodec txCodec,
            RuntimeSupport runtimeSupport,
            Gson gson,
            String topic,
            int ratePerSecond
    ) {
        this.producer = Objects.requireNonNull(producer, "producer must not be null");
        this.txGenerator = Objects.requireNonNull(txGenerator, "txGenerator must not be null");
        this.txCodec = Objects.requireNonNull(txCodec, "txCodec must not be null");
        this.runtimeSupport = Objects.requireNonNull(runtimeSupport, "runtimeSupport must not be null");
        this.gson = Objects.requireNonNull(gson, "gson must not be null");
        this.topic = Objects.requireNonNull(topic, "topic must not be null");
        this.ratePerSecond = ratePerSecond;
    }

    public void run() throws InterruptedException {
        runtimeSupport.writeHeartbeat();
        System.out.printf(
                "Kafka producer publishing to topic '%s' at %d msg/s%n",
                topic,
                ratePerSecond
        );

        long publishedSinceStartup = 0L;

        try {
            while (!Thread.currentThread().isInterrupted()) {
                Transaction transaction = txGenerator.nextTransaction();
                String payload = gson.toJson(txCodec.toFields(transaction));
                producer.send(new ProducerRecord<>(topic, transaction.txnId(), payload));
                publishedSinceStartup++;

                if (publishedSinceStartup == 1 || publishedSinceStartup % 100 == 0) {
                    System.out.printf(
                            "Published %,d transactions to '%s'. Latest: %s (%s, $%,.2f)%n",
                            publishedSinceStartup,
                            topic,
                            transaction.txnId(),
                            transaction.category(),
                            transaction.amount()
                    );
                }

                runtimeSupport.writeHeartbeat();
                Thread.sleep(computeSleepMs(ratePerSecond));
            }
        } finally {
            producer.flush();
            producer.close();
        }
    }

    private static long computeSleepMs(int ratePerSecond) {
        return Math.max(1L, 1_000L / ratePerSecond);
    }

    private static Producer<String, String> createProducer(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "kafka-transaction-producer");
        return new KafkaProducer<>(props);
    }

    private static String loadBootstrapServers() {
        String raw = System.getenv(KAFKA_BOOTSTRAP_SERVERS_ENV);
        return (raw == null || raw.isBlank()) ? KAFKA_BOOTSTRAP_DEFAULT : raw.trim();
    }

    private static String loadTopic() {
        String raw = System.getenv(KAFKA_TOPIC_ENV);
        return (raw == null || raw.isBlank()) ? KAFKA_TOPIC_DEFAULT : raw.trim();
    }

    private static int loadRatePerSecond() {
        String raw = System.getenv(PRODUCER_RATE_PER_SECOND_ENV);
        if (raw == null || raw.isBlank()) {
            return PRODUCER_DEFAULT_RATE_PER_SECOND;
        }

        int parsedRate;
        try {
            parsedRate = Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    PRODUCER_RATE_PER_SECOND_ENV + " must be an integer between 1 and "
                            + PRODUCER_MAX_RATE_PER_SECOND,
                    e
            );
        }

        if (parsedRate < 1 || parsedRate > PRODUCER_MAX_RATE_PER_SECOND) {
            throw new IllegalArgumentException(
                    PRODUCER_RATE_PER_SECOND_ENV + " must be between 1 and " + PRODUCER_MAX_RATE_PER_SECOND
            );
        }

        return parsedRate;
    }

    public static void main(String[] args) throws InterruptedException {
        KafkaTransactionProducer app = new KafkaTransactionProducer(
                createProducer(loadBootstrapServers()),
                new TransactionGenerator(),
                new TransactionCodec(),
                new RuntimeSupport(),
                new Gson(),
                loadTopic(),
                loadRatePerSecond()
        );
        app.run();
    }
}
