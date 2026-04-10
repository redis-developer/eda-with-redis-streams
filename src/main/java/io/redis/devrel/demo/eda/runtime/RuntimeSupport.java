package io.redis.devrel.demo.eda.runtime;

import redis.clients.jedis.RedisClient;
import redis.clients.jedis.UnifiedJedis;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;

import static io.redis.devrel.demo.eda.domain.Constants.HEALTH_FILE_PATH;
import static io.redis.devrel.demo.eda.domain.Constants.REDIS_DEFAULT_HOST;
import static io.redis.devrel.demo.eda.domain.Constants.REDIS_DEFAULT_PORT;
import static io.redis.devrel.demo.eda.domain.Constants.REDIS_HOST_ENV;
import static io.redis.devrel.demo.eda.domain.Constants.REDIS_PORT_ENV;

public final class RuntimeSupport {
    private static final Path HEALTH_FILE = Path.of(HEALTH_FILE_PATH);

    public UnifiedJedis createJedisFromEnv() {
        String redisHost = System.getenv().getOrDefault(REDIS_HOST_ENV, REDIS_DEFAULT_HOST);
        int redisPort = Integer.parseInt(System.getenv().getOrDefault(REDIS_PORT_ENV, REDIS_DEFAULT_PORT));
        return RedisClient.builder().hostAndPort(redisHost, redisPort).build();
    }

    public void writeHeartbeat() {
        try {
            Files.writeString(
                    HEALTH_FILE,
                    Instant.now().toString(),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING
            );
        } catch (IOException e) {
            throw new IllegalStateException("Unable to write health heartbeat", e);
        }
    }
}
