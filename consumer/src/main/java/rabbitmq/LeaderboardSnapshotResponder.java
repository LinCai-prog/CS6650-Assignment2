package rabbitmq;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.rabbitmq.client.*;
import storage.LeaderboardStore;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/** Authoritative read-only snapshot service for the ingest hosts. */
public final class LeaderboardSnapshotResponder {
    public static final String QUEUE_NAME = "skitrak.leaderboard.snapshots";
    private final Channel channel;
    private final LeaderboardStore store;
    public LeaderboardSnapshotResponder(Channel channel, LeaderboardStore store) { this.channel = channel; this.store = store; }
    public String start() throws Exception {
        channel.queueDeclare(QUEUE_NAME, true, false, false,
                Map.of("x-message-ttl", 5000, "x-max-length", 1000, "x-overflow", "reject-publish"));
        channel.basicQos(10);
        return channel.basicConsume(QUEUE_NAME, false, (tag, delivery) -> {
            var properties = delivery.getProperties();
            long deliveryTag = delivery.getEnvelope().getDeliveryTag();
            if (properties.getReplyTo() == null || properties.getCorrelationId() == null) {
                channel.basicNack(deliveryTag, false, false); return;
            }
            String resort;
            String day;
            try {
                JsonObject request = JsonParser.parseString(new String(delivery.getBody(), StandardCharsets.UTF_8)).getAsJsonObject();
                resort = request.get("resortId").getAsString(); day = request.get("dayId").getAsString();
                if (!resort.matches("[1-9][0-9]{0,8}") || Integer.parseInt(resort) > 10 || !day.matches("[1-9][0-9]{0,8}")) {
                    throw new IllegalArgumentException("Invalid snapshot identifiers");
                }
            } catch (RuntimeException malformed) {
                channel.basicNack(deliveryTag, false, false); return;
            }
            try {
                byte[] snapshot = store.getSnapshot(resort, day).toSnapshotJson().toString().getBytes(StandardCharsets.UTF_8);
                channel.basicPublish("", properties.getReplyTo(),
                        new AMQP.BasicProperties.Builder().correlationId(properties.getCorrelationId())
                                .contentType("application/json").build(), snapshot);
                channel.basicAck(deliveryTag, false);
            } catch (Exception failure) {
                if (channel.isOpen()) channel.basicNack(deliveryTag, false, true);
            }
        }, tag -> System.err.println("Leaderboard snapshot consumer cancelled: " + tag));
    }
}
