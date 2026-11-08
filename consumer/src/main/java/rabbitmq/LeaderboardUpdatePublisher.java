
package rabbitmq;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class LeaderboardUpdatePublisher {

    public static final String EXCHANGE_NAME =
            "skitrak.leaderboard.updates";

    private final Channel channel;
    private final Gson gson = new Gson();

    public LeaderboardUpdatePublisher(Channel channel)
            throws IOException {

        this.channel = Objects.requireNonNull(channel);

        // Durable topic exchange.
        channel.exchangeDeclare(
                EXCHANGE_NAME,
                BuiltinExchangeType.TOPIC,
                true
        );
    }

    public void publish(
            String eventId,
            String resortId,
            String dayId) throws IOException {

        if (eventId == null || eventId.trim().isEmpty()
                || resortId == null || !resortId.matches("\\d+")
                || dayId == null || !dayId.matches("\\d+")) {
            throw new IllegalArgumentException(
                    "Invalid leaderboard update identifiers"
            );
        }

        JsonObject message = new JsonObject();

        message.addProperty("type", "leaderboard_changed");
        message.addProperty("eventId", eventId);
        message.addProperty("resortId", resortId);
        message.addProperty("dayId", dayId);

        String routingKey =
                "leaderboard." + resortId + "." + dayId;

        byte[] body = gson.toJson(message)
                .getBytes(StandardCharsets.UTF_8);

        AMQP.BasicProperties properties =
                new AMQP.BasicProperties.Builder()
                        .contentType("application/json")
                        .deliveryMode(2)
                        .build();

        // Avoid concurrent publishing on the same channel.
        synchronized (channel) {
            channel.basicPublish(
                    EXCHANGE_NAME,
                    routingKey,
                    false,
                    properties,
                    body
            );
        }
    }
}
