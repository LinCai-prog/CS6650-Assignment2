package leaderboard;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.rabbitmq.client.*;
import model.LeaderboardUpdate;
import rabbitmq.RabbitMQConfig;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.function.Consumer;

/** Dedicated subscriber/RPC connection; the HTTP publisher keeps its channel pool. */
public final class RabbitLeaderboardBackplane implements SnapshotClient, AutoCloseable {
    public static final String EXCHANGE = "skitrak.leaderboard.updates";
    public static final String SNAPSHOT_QUEUE = "skitrak.leaderboard.snapshots";
    private final ScheduledExecutorService worker = Executors.newSingleThreadScheduledExecutor();
    private final Map<String, CompletableFuture<LeaderboardUpdate>> requests = new ConcurrentHashMap<>();
    private final Consumer<LeaderboardUpdate> updates;
    private final Runnable recovered;
    private volatile Connection connection;
    private volatile Channel rpc;
    private volatile String replyQueue;
    private volatile boolean ready;
    private volatile boolean stopped;

    public RabbitLeaderboardBackplane(Consumer<LeaderboardUpdate> updates, Runnable recovered) {
        this.updates = updates; this.recovered = recovered;
    }
    public void start() { worker.scheduleWithFixedDelay(this::ensureConnected, 0, 3, TimeUnit.SECONDS); }
    public boolean isReady() { return ready && connection != null && connection.isOpen(); }

    private void ensureConnected() {
        if (stopped || isReady()) return;
        disconnect();
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(RabbitMQConfig.HOST); factory.setPort(RabbitMQConfig.PORT);
            factory.setUsername(RabbitMQConfig.USERNAME); factory.setPassword(RabbitMQConfig.PASSWORD);
            factory.setConnectionTimeout(5000); factory.setRequestedHeartbeat(15);
            factory.setAutomaticRecoveryEnabled(false);
            Connection opened = factory.newConnection("skitrak-ingest-leaderboard");
            connection = opened;
            opened.addShutdownListener(cause -> {
                if (connection != opened) return;
                ready = false;
                requests.values().forEach(future -> future.completeExceptionally(new IllegalStateException("Backplane disconnected")));
                requests.clear();
            });
            Channel notifications = opened.createChannel();
            notifications.exchangeDeclare(EXCHANGE, BuiltinExchangeType.TOPIC, true);
            String updateQueue = notifications.queueDeclare().getQueue();
            notifications.queueBind(updateQueue, EXCHANGE, "leaderboard.*.*");
            notifications.basicQos(20);
            notifications.basicConsume(updateQueue, false, (tag, delivery) -> {
                try {
                    LeaderboardUpdate update = LeaderboardUpdate.fromJson(JsonParser.parseString(
                            new String(delivery.getBody(), StandardCharsets.UTF_8)).getAsJsonObject());
                    updates.accept(update);
                    notifications.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                } catch (RuntimeException malformed) {
                    notifications.basicNack(delivery.getEnvelope().getDeliveryTag(), false, false);
                }
            }, tag -> ready = false);
            Channel replies = opened.createChannel();
            // Snapshot service must exist before this host advertises readiness.
            if (replies.queueDeclarePassive(SNAPSHOT_QUEUE).getConsumerCount() == 0) {
                throw new IllegalStateException("Snapshot service is not ready");
            }
            replyQueue = replies.queueDeclare().getQueue();
            replies.addReturnListener((code, text, exchange, key, properties, body) -> {
                CompletableFuture<LeaderboardUpdate> request = requests.remove(properties.getCorrelationId());
                if (request != null) request.completeExceptionally(new IllegalStateException("Snapshot service unavailable"));
            });
            replies.basicConsume(replyQueue, true, (tag, delivery) -> {
                String id = delivery.getProperties().getCorrelationId();
                if (id == null) return;
                CompletableFuture<LeaderboardUpdate> request = requests.remove(id);
                if (request == null) return;
                try {
                    request.complete(LeaderboardUpdate.fromSnapshot(JsonParser.parseString(
                            new String(delivery.getBody(), StandardCharsets.UTF_8)).getAsJsonObject()));
                } catch (RuntimeException e) { request.completeExceptionally(e); }
            }, tag -> ready = false);
            rpc = replies; ready = true;
            recovered.run();
            System.out.println("Leaderboard backplane ready: " + updateQueue);
        } catch (Exception e) {
            disconnect();
            System.err.println("Leaderboard backplane unavailable; retrying: " + e.getClass().getSimpleName());
        }
    }

    @Override
    public CompletableFuture<LeaderboardUpdate> fetch(String resort, String day) {
        CompletableFuture<LeaderboardUpdate> future = new CompletableFuture<>();
        if (!isReady() || requests.size() >= 1000) {
            future.completeExceptionally(new IllegalStateException("Backplane not ready")); return future;
        }
        String id = UUID.randomUUID().toString();
        requests.put(id, future);
        future.orTimeout(5, TimeUnit.SECONDS).whenComplete((value, error) -> requests.remove(id, future));
        try {
            JsonObject request = new JsonObject(); request.addProperty("resortId", resort); request.addProperty("dayId", day);
            Channel channel = rpc;
            synchronized (channel) {
                channel.basicPublish("", SNAPSHOT_QUEUE, true,
                        new AMQP.BasicProperties.Builder().contentType("application/json").replyTo(replyQueue)
                                .correlationId(id).expiration("5000").build(),
                        request.toString().getBytes(StandardCharsets.UTF_8));
            }
        } catch (Exception e) { future.completeExceptionally(e); }
        return future;
    }
    private void disconnect() {
        ready = false;
        Connection previous = connection; connection = null; rpc = null;
        if (previous != null) try { previous.close(); } catch (Exception ignored) { }
        requests.values().forEach(future -> future.completeExceptionally(new IllegalStateException("Backplane unavailable")));
        requests.clear();
    }
    @Override
    public void close() {
        stopped = true; worker.shutdownNow(); disconnect();
    }
}
