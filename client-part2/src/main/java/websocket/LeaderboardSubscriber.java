
package websocket;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LeaderboardSubscriber implements WebSocket.Listener {

    private static final Pattern TYPE_PATTERN =
            Pattern.compile("\"type\"\\s*:\\s*\"([^\"]+)\"");

    private static final Pattern TIMESTAMP_PATTERN =
            Pattern.compile("\"serverTimestamp\"\\s*:\\s*\"([^\"]+)\"");

    private static final Pattern RESORT_PATTERN =
            Pattern.compile("\"resortId\"\\s*:\\s*\"([^\"]+)\"");

    private static final Pattern DAY_PATTERN =
            Pattern.compile("\"dayId\"\\s*:\\s*\"([^\"]+)\"");

    private final HttpClient httpClient;
    private final String url;
    private final int resortID;
    private final String dayID;

    private final AtomicInteger messageCount = new AtomicInteger();
    private final AtomicInteger distinctChangeCount = new AtomicInteger();
    private final AtomicInteger invalidUpdateCount = new AtomicInteger();
    private final AtomicInteger staleUpdateCount = new AtomicInteger();
    private final LeaderboardUpdateTracker updateTracker = new LeaderboardUpdateTracker();
    private final AtomicInteger connectionCount = new AtomicInteger();
    private final AtomicInteger reconnectionCount = new AtomicInteger();

    private final ConcurrentLinkedQueue<Long> pushLatencies =
            new ConcurrentLinkedQueue<>();

    private final ConcurrentLinkedQueue<Double> connectionTimes =
            new ConcurrentLinkedQueue<>();

    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor();

    private final AtomicBoolean reconnectScheduled =
            new AtomicBoolean(false);

    private final CompletableFuture<Void> initialSubscription =
            new CompletableFuture<>();

    private final StringBuilder messageBuffer = new StringBuilder();

    private volatile WebSocket webSocket;
    private volatile boolean running = false;
    private volatile long connectionStartTime;

    public LeaderboardSubscriber(
            HttpClient httpClient,
            String baseUrl,
            int resortID) {
        this(httpClient, baseUrl, resortID, 1);
    }

    public LeaderboardSubscriber(HttpClient httpClient, String baseUrl, int resortID, int dayId) {

        this.httpClient = httpClient;
        this.url = baseUrl;
        this.resortID = resortID;
        this.dayID = Integer.toString(dayId);
    }

    public void start() {
        if (running) {
            return;
        }

        running = true;
        connect();
    }

    public void awaitSubscribed(Duration timeout)
            throws InterruptedException, ExecutionException,
            TimeoutException {

        initialSubscription.get(
                timeout.toMillis(),
                TimeUnit.MILLISECONDS
        );
    }

    private void connect() {
        if (!running) {
            return;
        }

        connectionStartTime = System.nanoTime();

        httpClient.newWebSocketBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .buildAsync(URI.create(url), this)
                .whenComplete((ws, error) -> {
                    if (error != null) {
                        System.err.println(
                                "WebSocket connection failed for resort "
                                        + resortID + ": " + error
                        );
                        reconnect();
                    }
                });
    }

    @Override
    public void onOpen(WebSocket socket) {
        this.webSocket = socket;
        updateTracker.reset();

        if (!running) {
            socket.sendClose(
                    WebSocket.NORMAL_CLOSURE,
                    "Client shutting down"
            );
            return;
        }

        connectionCount.incrementAndGet();

        double connectionTimeMs =
                (System.nanoTime() - connectionStartTime)
                        / 1_000_000.0;

        connectionTimes.add(connectionTimeMs);

        System.out.printf(
                "Resort %d WebSocket connected in %.2f ms%n",
                resortID,
                connectionTimeMs
        );

        String subscribeMessage = String.format(
                "{\"action\":\"subscribe\","
                        + "\"resortId\":\"%d\","
                        + "\"dayId\":\"%s\"}",
                resortID,
                dayID
        );

        socket.request(1);

        socket.sendText(subscribeMessage, true)
                .whenComplete((result, error) -> {
                    if (error != null) {
                        System.err.println(
                                "Subscription send failed for resort "
                                        + resortID + ": " + error
                        );
                        reconnect();
                    }
                });
    }

    @Override
    public CompletionStage<?> onText(
            WebSocket socket,
            CharSequence data,
            boolean last) {

        synchronized (messageBuffer) {
            messageBuffer.append(data);

            if (last) {
                String message = messageBuffer.toString();
                messageBuffer.setLength(0);
                processMessage(message);
            }
        }

        socket.request(1);
        return null;
    }

    private void processMessage(String message) {
        String type = extract(TYPE_PATTERN, message);

        if ("subscribed".equals(type)) {
            String receivedResort = extract(RESORT_PATTERN, message);
            String receivedDay = extract(DAY_PATTERN, message);

            if (Integer.toString(resortID).equals(receivedResort)
                    && dayID.equals(receivedDay)) {

                initialSubscription.complete(null);

                System.out.printf(
                        "Resort %d Day %s subscribed successfully%n",
                        resortID,
                        dayID
                );
            }
            return;
        }

        if ("leaderboard_snapshot".equals(type)) {
            if (Integer.toString(resortID).equals(extract(RESORT_PATTERN, message))
                    && dayID.equals(extract(DAY_PATTERN, message))) {
                try {
                    updateTracker.accept(message, true);
                } catch (IllegalArgumentException e) {
                    invalidUpdateCount.incrementAndGet();
                    System.err.println("Invalid leaderboard snapshot: " + e.getMessage());
                }
            }
            return;
        }

        if ("error".equals(type)) {
            System.err.println(
                    "WebSocket server error for resort "
                            + resortID + ": " + message
            );
            return;
        }

        if (!"leaderboard_update".equals(type)) {
            return;
        }

        String receivedResort = extract(RESORT_PATTERN, message);
        String receivedDay = extract(DAY_PATTERN, message);

        if (!Integer.toString(resortID).equals(receivedResort)
                || !dayID.equals(receivedDay)) {
            return;
        }

        Instant receiveTime = Instant.now();
        messageCount.incrementAndGet();
        try {
            var result = updateTracker.accept(message, false);
            if (result == LeaderboardUpdateTracker.Result.CHANGED) distinctChangeCount.incrementAndGet();
            if (result == LeaderboardUpdateTracker.Result.STALE) staleUpdateCount.incrementAndGet();
        } catch (IllegalArgumentException e) {
            invalidUpdateCount.incrementAndGet();
            System.err.println("Invalid leaderboard update: " + e.getMessage());
        }

        String timestamp = extract(TIMESTAMP_PATTERN, message);

        if (timestamp == null) {
            System.err.println(
                    "Leaderboard update missing serverTimestamp"
            );
            return;
        }

        try {
            Instant serverTime = Instant.parse(timestamp);

            long latencyMs =
                    Duration.between(serverTime, receiveTime)
                            .toMillis();

            if (latencyMs >= 0) {
                pushLatencies.add(latencyMs);
            } else {
                System.err.println(
                        "Negative push latency for resort "
                                + resortID + ": " + latencyMs
                                + " ms; check clock synchronization"
                );
            }

        } catch (DateTimeParseException e) {
            System.err.println(
                    "Invalid serverTimestamp: " + timestamp
            );
        }
    }

    public int getDistinctChangeCount() { return distinctChangeCount.get(); }
    public int getInvalidUpdateCount() { return invalidUpdateCount.get(); }
    public int getStaleUpdateCount() { return staleUpdateCount.get(); }

    private static String extract(Pattern pattern, String message) {
        Matcher matcher = pattern.matcher(message);
        return matcher.find() ? matcher.group(1) : null;
    }

    @Override
    public CompletionStage<?> onClose(
            WebSocket socket,
            int statusCode,
            String reason) {

        System.out.println(
                "WebSocket closed for resort " + resortID
        );

        reconnect();
        return null;
    }

    @Override
    public void onError(WebSocket socket, Throwable error) {
        System.err.println(
                "WebSocket error for resort "
                        + resortID + ": " + error
        );

        reconnect();
    }

    private void reconnect() {
        if (!running) {
            return;
        }

        if (!reconnectScheduled.compareAndSet(false, true)) {
            return;
        }

        scheduler.schedule(() -> {
            reconnectScheduled.set(false);

            if (running) {
                reconnectionCount.incrementAndGet();
                connect();
            }
        }, 2, TimeUnit.SECONDS);
    }

    public void stop() {
        running = false;

        initialSubscription.completeExceptionally(
                new IllegalStateException("Subscriber stopped")
        );

        WebSocket socket = webSocket;

        if (socket != null) {
            socket.sendClose(
                    WebSocket.NORMAL_CLOSURE,
                    "Client shutting down"
            );
        }

        scheduler.shutdownNow();
    }

    public int getResortID() {
        return resortID;
    }

    public int getMessageCount() {
        return messageCount.get();
    }

    public int getConnectionCount() {
        return connectionCount.get();
    }

    public int getReconnectionCount() {
        return reconnectionCount.get();
    }

    public List<Long> getPushLatencies() {
        return new ArrayList<>(pushLatencies);
    }

    public List<Double> getConnectionTimes() {
        return new ArrayList<>(connectionTimes);
    }
}
