
package rabbitmq;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;

import websocket.LeaderboardWebSocketServer;
import model.LeaderboardUpdate;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class LeaderboardUpdateSubscriber {

    private String queueName;

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

        if (queueName == null) prepare();

        channel.basicQos(10);

        DeliverCallback callback = (consumerTag, delivery) -> {
            long deliveryTag = delivery.getEnvelope().getDeliveryTag();
            LeaderboardUpdate update;
            try {
                String json = new String(delivery.getBody(), StandardCharsets.UTF_8);
                update = LeaderboardUpdate.fromJson(JsonParser.parseString(json).getAsJsonObject());
            } catch (RuntimeException e) {
                System.err.println("Invalid leaderboard notification: " + e);
                synchronized (channel) {
                    channel.basicNack(deliveryTag, false, false);
                }
                return;
            }

            try {
                webSocketServer.broadcastLeaderboardUpdate(update);
                synchronized (channel) {
                    channel.basicAck(deliveryTag, false);
                }
            } catch (Exception e) {
                System.err.println("Leaderboard delivery deferred for retry: " + e);
                synchronized (channel) {
                    if (channel.isOpen()) channel.basicNack(deliveryTag, false, true);
                }
            }
        };

        String consumerTag = channel.basicConsume(queueName, false, callback,
                tag -> System.err.println("Leaderboard update subscriber cancelled: " + tag));
        System.out.println("Listening for leaderboard updates on: " + queueName);
        return consumerTag;
    }

    // Bind before exposing the WebSocket listener so no change can slip between
    // a client's initial snapshot and this process starting its subscription.
    public void prepare() throws IOException {
        if (queueName != null) return;
        channel.exchangeDeclare(
                LeaderboardUpdatePublisher.EXCHANGE_NAME,
                BuiltinExchangeType.TOPIC,
                true
        );

        // Every WebSocket process needs its own copy, not competing consumers
        // on one shared queue. Recovery re-declares this exclusive queue.
        queueName = channel.queueDeclare().getQueue();

        // Receive updates for any resort and day.
        channel.queueBind(
                queueName,
                LeaderboardUpdatePublisher.EXCHANGE_NAME,
                BINDING_KEY
        );

    }
}
