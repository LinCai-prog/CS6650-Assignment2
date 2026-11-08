
package rabbitmq;

import com.google.gson.Gson;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ReturnListener;
import model.SwipeEventMessage;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

public class SwipePublisher {

    private static final int MAX_ATTEMPTS = 3;
    private static final long CONFIRM_TIMEOUT_MS = 5000;

    private final ChannelPool channelPool;
    private final Gson gson = new Gson();

    public SwipePublisher(ChannelPool channelPool) {
        this.channelPool = channelPool;
    }

    public void publish(SwipeEventMessage event) throws Exception {

        // Serialize once. Retries must reuse the same eventId.
        byte[] body = gson.toJson(event)
                .getBytes(StandardCharsets.UTF_8);

        String routingKey = "resort." + event.getResortID();

        Exception lastFailure = null;

        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {

            Channel channel = null;
            ReturnListener returnListener = null;

            try {
                channel = channelPool.borrowChannel();

                AtomicBoolean returned = new AtomicBoolean(false);

                // Detect unroutable mandatory messages.
                returnListener = (replyCode, replyText,
                                  exchange, key, properties, message) -> {
                    returned.set(true);
                };

                channel.addReturnListener(returnListener);

                AMQP.BasicProperties properties =
                        new AMQP.BasicProperties.Builder()
                                .deliveryMode(2)
                                .contentType("application/json")
                                .contentEncoding("UTF-8")
                                .messageId(event.getEventId())
                                .build();

                // mandatory=true requests a return if not routable.
                channel.basicPublish(
                        RabbitMQConfig.SWIPE_EXCHANGE,
                        routingKey,
                        true,
                        properties,
                        body
                );

                // Wait for broker confirmation.
                channel.waitForConfirmsOrDie(CONFIRM_TIMEOUT_MS);

                if (returned.get()) {
                    throw new IllegalStateException(
                            "Message was not routed: " + routingKey);
                }

                // Successful broker confirmation.
                return;

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                // Do not reuse a channel with uncertain outcome.
                if (channel != null) {
                    closeQuietly(channel);
                }

                throw e;

            } catch (Exception e) {
                lastFailure = e;

                // Discard channel after an uncertain failure.
                if (channel != null) {
                    closeQuietly(channel);
                }

                if (attempt == MAX_ATTEMPTS) {
                    break;
                }

                // Restore the pool before retrying.
                channelPool.rebuildPool();

                Thread.sleep(100L * attempt);

            } finally {
                if (channel != null) {
                    if (returnListener != null) {
                        channel.removeReturnListener(returnListener);
                    }

                    channelPool.returnChannel(channel);
                }
            }
        }

        throw new Exception(
                "Failed to publish event after " +
                        MAX_ATTEMPTS + " attempts: " + event.getEventId(),
                lastFailure
        );
    }

    private static void closeQuietly(Channel channel) {
        try {
            if (channel.isOpen()) {
                channel.close();
            }
        } catch (Exception ignored) {
            // Channel will be discarded by ChannelPool.
        }
    }
}
