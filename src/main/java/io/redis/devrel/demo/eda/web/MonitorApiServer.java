package io.redis.devrel.demo.eda.web;

import com.google.gson.Gson;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.redis.devrel.demo.eda.domain.Transaction;
import io.redis.devrel.demo.eda.domain.TransactionCodec;
import io.redis.devrel.demo.eda.runtime.RuntimeSupport;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.params.XReadGroupParams;
import redis.clients.jedis.resps.StreamConsumerInfo;
import redis.clients.jedis.resps.StreamEntry;
import redis.clients.jedis.resps.StreamGroupInfo;
import redis.clients.jedis.resps.Tuple;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import static io.redis.devrel.demo.eda.domain.Constants.ALERTS_STREAM_KEY;
import static io.redis.devrel.demo.eda.domain.Constants.METRICS_COUNT_BY_REGION_KEY;
import static io.redis.devrel.demo.eda.domain.Constants.METRICS_GROUP_NAME;
import static io.redis.devrel.demo.eda.domain.Constants.METRICS_HIGH_RISK_COUNT_KEY;
import static io.redis.devrel.demo.eda.domain.Constants.METRICS_TOTAL_COUNT_KEY;
import static io.redis.devrel.demo.eda.domain.Constants.METRICS_TOTAL_VOLUME_KEY;
import static io.redis.devrel.demo.eda.domain.Constants.METRICS_VOLUME_BY_CATEGORY_KEY;
import static io.redis.devrel.demo.eda.domain.Constants.MONITOR_CONSUMER_NAME;
import static io.redis.devrel.demo.eda.domain.Constants.MONITOR_CONSUMER_NAME_ENV;
import static io.redis.devrel.demo.eda.domain.Constants.MONITOR_GROUP_NAME;
import static io.redis.devrel.demo.eda.domain.Constants.STREAM_GROUP_START_ID;
import static io.redis.devrel.demo.eda.domain.Constants.TRANSACTIONS_STREAM_KEY;

public final class MonitorApiServer {
    private static final String API_PORT_ENV = "MONITOR_API_PORT";
    private static final int DEFAULT_API_PORT = 8080;
    private static final long ACTIVE_METRICS_CONSUMER_WINDOW_MS = 5_000L;
    private static final int RECENT_TRANSACTIONS_LIMIT = 5;
    private static final int RECENT_ALERTS_LIMIT = 6;
    private static final int CATEGORY_LEADERBOARD_LIMIT = 5;
    private static final StreamEntryID PENDING_ID = new StreamEntryID(STREAM_GROUP_START_ID);
    private static final StreamEntryID NEW_ENTRY_ID = StreamEntryID.XREADGROUP_UNDELIVERED_ENTRY;
    private static final int MONITOR_READ_COUNT = 10;
    private static final int MONITOR_BLOCK_MS = 1_000;

    private final RuntimeSupport runtimeSupport;
    private final TransactionCodec transactionCodec;
    private final Gson gson;
    private final int port;
    private final String consumerName;
    private final MonitorState monitorState;
    private final AtomicReference<Throwable> consumerFailure;

    public MonitorApiServer(
            RuntimeSupport runtimeSupport,
            TransactionCodec transactionCodec,
            Gson gson,
            int port,
            String consumerName
    ) {
        this.runtimeSupport = Objects.requireNonNull(runtimeSupport, "runtimeSupport must not be null");
        this.transactionCodec = Objects.requireNonNull(transactionCodec, "transactionCodec must not be null");
        this.gson = Objects.requireNonNull(gson, "gson must not be null");
        this.port = port;
        this.consumerName = Objects.requireNonNull(consumerName, "consumerName must not be null");
        this.monitorState = new MonitorState();
        this.consumerFailure = new AtomicReference<>();
    }

    public void run() throws IOException {
        createConsumerGroup();
        startConsumerLoop();

        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/health", this::handleHealth);
        server.createContext("/api/monitor", this::handleSnapshot);
        server.setExecutor(null);

        runtimeSupport.writeHeartbeat();
        System.out.printf(
                "Monitor API listening on port %d as consumer %s in group %s%n",
                port,
                consumerName,
                MONITOR_GROUP_NAME
        );
        server.start();
    }

    private void handleHealth(HttpExchange exchange) throws IOException {
        if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
            sendJson(exchange, 405, "{\"error\":\"method_not_allowed\"}");
            return;
        }

        Throwable failure = consumerFailure.get();
        if (failure != null) {
            sendJson(
                    exchange,
                    500,
                    gson.toJson(Map.of(
                            "status", "error",
                            "message", failure.getMessage()
                    ))
            );
            return;
        }

        runtimeSupport.writeHeartbeat();
        sendJson(exchange, 200, "{\"status\":\"ok\"}");
    }

    private void handleSnapshot(HttpExchange exchange) throws IOException {
        if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
            sendJson(exchange, 405, "{\"error\":\"method_not_allowed\"}");
            return;
        }

        Throwable failure = consumerFailure.get();
        if (failure != null) {
            sendJson(
                    exchange,
                    500,
                    gson.toJson(Map.of(
                            "error", "monitor_consumer_failed",
                            "message", failure.getMessage()
                    ))
            );
            return;
        }

        try {
            Snapshot snapshot = loadSnapshot();
            runtimeSupport.writeHeartbeat();
            sendJson(exchange, 200, gson.toJson(snapshot));
        } catch (Exception e) {
            sendJson(
                    exchange,
                    500,
                    gson.toJson(Map.of(
                            "error", "snapshot_unavailable",
                            "message", e.getMessage()
                    ))
            );
        }
    }

    private void startConsumerLoop() {
        Thread consumerThread = new Thread(this::consumeLoop, "monitor-api-consumer");
        consumerThread.setDaemon(true);
        consumerThread.setUncaughtExceptionHandler((thread, throwable) -> consumerFailure.compareAndSet(null, throwable));
        consumerThread.start();
    }

    private void consumeLoop() {
        try (UnifiedJedis jedis = runtimeSupport.createJedisFromEnv()) {
            recoverPendingEntries(jedis);

            while (!Thread.currentThread().isInterrupted()) {
                List<StreamMessage> entries = readGroup(
                        jedis,
                        NEW_ENTRY_ID,
                        MONITOR_READ_COUNT,
                        MONITOR_BLOCK_MS
                );

                if (!entries.isEmpty()) {
                    processEntries(jedis, entries);
                }

                runtimeSupport.writeHeartbeat();
            }
        } catch (Throwable throwable) {
            consumerFailure.compareAndSet(null, throwable);
        }
    }

    private void createConsumerGroup() {
        try (UnifiedJedis jedis = runtimeSupport.createJedisFromEnv()) {
            try {
                jedis.xgroupCreate(
                        TRANSACTIONS_STREAM_KEY,
                        MONITOR_GROUP_NAME,
                        new StreamEntryID(STREAM_GROUP_START_ID),
                        true
                );
            } catch (JedisDataException e) {
                if (!e.getMessage().contains("BUSYGROUP")) {
                    throw e;
                }
            }
        }
    }

    private void recoverPendingEntries(UnifiedJedis jedis) {
        while (true) {
            List<StreamMessage> pendingEntries = readGroup(jedis, PENDING_ID, 50, null);
            if (pendingEntries.isEmpty()) {
                return;
            }
            processEntries(jedis, pendingEntries);
        }
    }

    private List<StreamMessage> readGroup(
            UnifiedJedis jedis,
            StreamEntryID streamEntryID,
            int count,
            Integer blockMs
    ) {
        XReadGroupParams params = XReadGroupParams.xReadGroupParams().count(count);
        if (blockMs != null) {
            params.block(blockMs);
        }

        List<Map.Entry<String, List<StreamEntry>>> rawEntries = jedis.xreadGroup(
                MONITOR_GROUP_NAME,
                consumerName,
                params,
                Map.of(TRANSACTIONS_STREAM_KEY, streamEntryID)
        );

        return parseEntries(rawEntries);
    }

    private void processEntries(UnifiedJedis jedis, List<StreamMessage> entries) {
        for (StreamMessage entry : entries) {
            Transaction transaction = transactionCodec.fromFields(entry.fields());
            monitorState.record(new TransactionView(
                    entry.id(),
                    transaction.txnId(),
                    transaction.shortId(),
                    transaction.amount(),
                    transaction.category(),
                    transaction.region(),
                    transaction.riskScore(),
                    transaction.isHighRisk(),
                    transaction.timestamp()
            ));
            jedis.xack(TRANSACTIONS_STREAM_KEY, MONITOR_GROUP_NAME, new StreamEntryID(entry.id()));
        }
    }

    private Snapshot loadSnapshot() {
        try (UnifiedJedis jedis = runtimeSupport.createJedisFromEnv()) {
            List<StreamGroupInfo> groups = loadGroupInfo(jedis);
            DashboardStatus dashboardStatus = loadDashboardStatus(jedis, groups);

            return new Snapshot(
                    Instant.now().toString(),
                    dashboardStatus,
                    new StreamSummary(
                            jedis.xlen(TRANSACTIONS_STREAM_KEY),
                            jedis.xlen(ALERTS_STREAM_KEY)
                    ),
                    new MetricsSummary(
                            parseLong(jedis.get(METRICS_TOTAL_COUNT_KEY)),
                            parseDouble(jedis.get(METRICS_TOTAL_VOLUME_KEY)),
                            parseLong(jedis.get(METRICS_HIGH_RISK_COUNT_KEY)),
                            loadCategoryLeaderboard(jedis),
                            loadRegionCounts(jedis)
                    ),
                    monitorState.snapshot(),
                    loadAlerts(jedis)
            );
        }
    }

    private List<StreamGroupInfo> loadGroupInfo(UnifiedJedis jedis) {
        try {
            return jedis.xinfoGroups(TRANSACTIONS_STREAM_KEY);
        } catch (JedisDataException e) {
            return Collections.emptyList();
        }
    }

    private DashboardStatus loadDashboardStatus(UnifiedJedis jedis, List<StreamGroupInfo> groups) {
        long processedCount = groups.stream()
                .filter(groupInfo -> MONITOR_GROUP_NAME.equals(groupInfo.getName()))
                .findFirst()
                .map(MonitorApiServer::extractEntriesRead)
                .orElse(0L);

        long metricsLag = groups.stream()
                .filter(groupInfo -> METRICS_GROUP_NAME.equals(groupInfo.getName()))
                .findFirst()
                .map(MonitorApiServer::extractLag)
                .orElse(0L);

        long metricsConsumers;
        try {
            metricsConsumers = jedis.xinfoConsumers2(TRANSACTIONS_STREAM_KEY, METRICS_GROUP_NAME).stream()
                    .filter(MonitorApiServer::isActiveMetricsConsumer)
                    .count();
        } catch (JedisDataException e) {
            metricsConsumers = 0L;
        }

        return new DashboardStatus(processedCount, metricsLag, metricsConsumers);
    }

    private List<AlertView> loadAlerts(UnifiedJedis jedis) {
        List<StreamEntry> entries = jedis.xrevrange(ALERTS_STREAM_KEY, "+", "-", RECENT_ALERTS_LIMIT);
        List<AlertView> alerts = new ArrayList<>();

        for (StreamEntry entry : entries) {
            Map<String, String> fields = entry.getFields();
            alerts.add(new AlertView(
                    entry.getID().toString(),
                    fields.getOrDefault("alert_id", ""),
                    fields.getOrDefault("alert_type", ""),
                    fields.getOrDefault("severity", ""),
                    fields.getOrDefault("region", ""),
                    fields.getOrDefault("category", ""),
                    fields.getOrDefault("observed_value", ""),
                    fields.getOrDefault("threshold", ""),
                    fields.getOrDefault("window_seconds", ""),
                    fields.getOrDefault("message", ""),
                    parseLong(fields.get("timestamp"))
            ));
        }

        return alerts;
    }

    private List<CategoryMetric> loadCategoryLeaderboard(UnifiedJedis jedis) {
        List<Tuple> leaderboard = jedis.zrangeWithScores(
                METRICS_VOLUME_BY_CATEGORY_KEY,
                0,
                CATEGORY_LEADERBOARD_LIMIT - 1L
        );

        List<CategoryMetric> metrics = new ArrayList<>();
        for (Tuple tuple : leaderboard) {
            metrics.add(new CategoryMetric(tuple.getElement(), tuple.getScore()));
        }
        return metrics;
    }

    private Map<String, Double> loadRegionCounts(UnifiedJedis jedis) {
        Object json = jedis.jsonGet(METRICS_COUNT_BY_REGION_KEY);
        if (json instanceof Map<?, ?> rawMap) {
            return rawMap.entrySet().stream()
                    .filter(entry -> entry.getKey() != null && entry.getValue() != null)
                    .collect(java.util.stream.Collectors.toMap(
                            entry -> entry.getKey().toString(),
                            entry -> Double.parseDouble(entry.getValue().toString())
                    ));
        }
        return Collections.emptyMap();
    }

    private static List<StreamMessage> parseEntries(List<Map.Entry<String, List<StreamEntry>>> rawEntries) {
        if (rawEntries == null || rawEntries.isEmpty()) {
            return Collections.emptyList();
        }

        List<StreamMessage> entries = new ArrayList<>();
        rawEntries.forEach(streamData ->
                streamData.getValue().forEach(streamEntry ->
                        entries.add(new StreamMessage(streamEntry.getID().toString(), streamEntry.getFields()))
                )
        );
        return entries;
    }

    private static long extractLag(StreamGroupInfo groupInfo) {
        Object rawLag = groupInfo.getGroupInfo().get("lag");
        if (rawLag instanceof Number number) {
            return Math.max(0L, number.longValue());
        }
        if (rawLag == null) {
            return 0L;
        }
        try {
            return Math.max(0L, Long.parseLong(rawLag.toString()));
        } catch (NumberFormatException e) {
            return 0L;
        }
    }

    private static long extractEntriesRead(StreamGroupInfo groupInfo) {
        Object rawEntriesRead = groupInfo.getGroupInfo().get("entries-read");
        if (rawEntriesRead instanceof Number number) {
            return Math.max(0L, number.longValue());
        }
        if (rawEntriesRead == null) {
            return 0L;
        }
        try {
            return Math.max(0L, Long.parseLong(rawEntriesRead.toString()));
        } catch (NumberFormatException e) {
            return 0L;
        }
    }

    private static boolean isActiveMetricsConsumer(StreamConsumerInfo consumerInfo) {
        Long inactive = consumerInfo.getInactive();
        if (inactive != null) {
            return inactive <= ACTIVE_METRICS_CONSUMER_WINDOW_MS;
        }
        return consumerInfo.getIdle() <= ACTIVE_METRICS_CONSUMER_WINDOW_MS;
    }

    private static long parseLong(String value) {
        if (value == null || value.isBlank()) {
            return 0L;
        }
        return Long.parseLong(value.trim());
    }

    private static double parseDouble(String value) {
        if (value == null || value.isBlank()) {
            return 0.0d;
        }
        return Double.parseDouble(value.trim());
    }

    private static int loadPort() {
        String rawPort = System.getenv(API_PORT_ENV);
        if (rawPort == null || rawPort.isBlank()) {
            return DEFAULT_API_PORT;
        }

        try {
            return Integer.parseInt(rawPort.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(API_PORT_ENV + " must be an integer", e);
        }
    }

    private static String loadConsumerName() {
        return System.getenv().getOrDefault(MONITOR_CONSUMER_NAME_ENV, MONITOR_CONSUMER_NAME);
    }

    private static void sendJson(HttpExchange exchange, int statusCode, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(statusCode, bytes.length);
        try (OutputStream outputStream = exchange.getResponseBody()) {
            outputStream.write(bytes);
        } finally {
            exchange.close();
        }
    }

    public static void main(String[] args) throws IOException {
        MonitorApiServer server = new MonitorApiServer(
                new RuntimeSupport(),
                new TransactionCodec(),
                new Gson(),
                loadPort(),
                loadConsumerName()
        );
        server.run();
    }

    private record StreamMessage(String id, Map<String, String> fields) {
    }

    private record Snapshot(
            String generatedAt,
            DashboardStatus dashboard,
            StreamSummary streams,
            MetricsSummary metrics,
            List<TransactionView> transactions,
            List<AlertView> alerts
    ) {
    }

    private record DashboardStatus(long processedCount, long metricsLag, long metricsConsumers) {
    }

    private record StreamSummary(long transactions, long alerts) {
    }

    private record MetricsSummary(
            long totalCount,
            double totalVolume,
            long highRiskCount,
            List<CategoryMetric> categories,
            Map<String, Double> regionCounts
    ) {
    }

    private record CategoryMetric(String category, double volume) {
    }

    private record TransactionView(
            String streamEntryId,
            String txnId,
            String shortId,
            double amount,
            String category,
            String region,
            int riskScore,
            boolean highRisk,
            long timestamp
    ) {
    }

    private record AlertView(
            String streamEntryId,
            String alertId,
            String alertType,
            String severity,
            String region,
            String category,
            String observedValue,
            String threshold,
            String windowSeconds,
            String message,
            long timestamp
    ) {
    }

    private static final class MonitorState {
        private final Deque<TransactionView> recentTransactions = new ArrayDeque<>();

        private synchronized void record(TransactionView transactionView) {
            recentTransactions.addFirst(transactionView);
            while (recentTransactions.size() > RECENT_TRANSACTIONS_LIMIT) {
                recentTransactions.removeLast();
            }
        }

        private synchronized List<TransactionView> snapshot() {
            return new ArrayList<>(recentTransactions);
        }
    }
}
