
package rabbitmq;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;

import websocket.LeaderboardWebSocketServer;
import websocket.SubscriptionKey;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class LeaderboardUpdateSubscriber {

    public static final String QUEUE_NAME =
            "skitrak.websocket.updates";

    private static final String BINDING_KEY =
            "leaderboard.*.*";

    private final Channel channel;
    private final LeaderboardWebSocketServer webSocketServer;

    public LeaderboardUpdateSubscriber(
            Channel channel,
            LeaderboardWebSocketServer webSocketServer) {

        this.channel = Objects.requireNonNull(channel);
        this.webSocketServer =
                Objects.requireNonNull(webSocketServer);
    }

    public String start() throws IOException {

        // Ensure the update exchange exists.
        channel.exchangeDeclare(
                LeaderboardUpdatePublisher.EXCHANGE_NAME,
                BuiltinExchangeType.TOPIC,
                true
        );

        // Durable, non-exclusive, non-auto-delete queue.
        channel.queueDeclare(
                QUEUE_NAME,
                true,
                false,
                false,
                null
        );

        // Receive updates for any resort and day.
        channel.queueBind(
                QUEUE_NAME,
                LeaderboardUpdatePublisher.EXCHANGE_NAME,
                BINDING_KEY
        );

        channel.basicQos(10);

        DeliverCallback callback = (consumerTag, delivery) -> {

            long deliveryTag =
                    delivery.getEnvelope().getDeliveryTag();

            try {
                String json = new String(
                        delivery.getBody(),
                        StandardCharsets.UTF_8
                );

                JsonObject message =
                        JsonParser.parseString(json)
                                .getAsJsonObject();

                String type = message.get("type").getAsString();

                if (!"leaderboard_changed".equals(type)) {
                    throw new IllegalArgumentException(
                            "Unexpected notification type: " + type
                    );
                }

                String resortId =
                        message.get("resortId").getAsString();

                String dayId =
                        message.get("dayId").getAsString();

                SubscriptionKey key =
                        new SubscriptionKey(resortId, dayId);

                webSocketServer.broadcastLeaderboardUpdate(key);

            } catch (Exception e) {
                // Best-effort update notification.
                // A future snapshot can recover the latest state.
                System.err.println(
                        "Leaderboard update notification failed: " + e
                );

            } finally {
                // Acknowledge this notification once.
                // Do not create an infinite requeue loop.
                synchronized (channel) {
                    if (channel.isOpen()) {
                        channel.basicAck(deliveryTag, false);
                    }
                }
            }
        };

        String consumerTag = channel.basicConsume(
                QUEUE_NAME,
                false,
                callback,
                tag -> System.err.println(
                        "Leaderboard update subscriber cancelled: "
                                + tag
                )
        );

        System.out.println(
                "Listening for leaderboard updates on: " + QUEUE_NAME
        );

        return consumerTag;
    }
}
