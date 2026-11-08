
package rabbitmq;

import com.google.gson.Gson;
import com.google.gson.JsonParseException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.CancelCallback;

import config.ConsumerConfig;
import concurrency.ResortSerialExecutor;
import concurrency.TaskAdmissionGate;

import model.SwipeEvent;
import storage.LeaderboardStore;
import validation.SwipeEventValidator;
import validation.InvalidSwipeEventException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class ResortConsumer {

    private final Channel channel;
    private final Gson gson = new Gson();

    private final ResortSerialExecutor serialExecutor;
    private final LeaderboardUpdatePublisher updatePublisher;
    private final LeaderboardStore store;

    private final TaskAdmissionGate admissionGate =
            new TaskAdmissionGate();

    public ResortConsumer(
            Channel channel,
            LeaderboardStore store,
            ResortSerialExecutor serialExecutor) {

        this(
                channel,
                store,
                serialExecutor,
                null
        );
    }

    public ResortConsumer(
            Channel channel,
            LeaderboardStore store,
            ResortSerialExecutor serialExecutor,
            LeaderboardUpdatePublisher updatePublisher) {

        this.channel = channel;
        this.store = store;
        this.serialExecutor = serialExecutor;
        this.updatePublisher = updatePublisher;
    }

    // Stop accepting new tasks during shutdown.
    public void stopAccepting() {
        admissionGate.close();
    }

    public String start(int resortId) throws IOException {

        String queueName = "resort." + resortId;

        // Limit the number of unacknowledged deliveries.
        channel.basicQos(ConsumerConfig.PREFETCH_COUNT);

        DeliverCallback deliverCallback =
                (consumerTag, delivery) -> {

                    long deliveryTag =
                            delivery.getEnvelope().getDeliveryTag();

                    byte[] body = delivery.getBody();

                    Runnable task = () -> {

                        SwipeEvent event;

                        try {
                            // Stage 1: Deserialize the message.
                            String json = new String(
                                    body,
                                    StandardCharsets.UTF_8
                            );

                            event = gson.fromJson(
                                    json,
                                    SwipeEvent.class
                            );

                            // Stage 2: Validate the event.
                            SwipeEventValidator.validate(
                                    event,
                                    resortId
                            );

                            // Stage 3: Write to SQLite.
                            //
                            // processEvent() performs:
                            // - Event ID deduplication
                            // - Vertical accumulation
                            // - Transaction commit
                            //
                            // Duplicate events return false.
                            boolean isNewEvent =
                                    store.processEvent(event);

                            // Stage 4: Publish a leaderboard notification
                            // only when a NEW event was committed.
                            //
                            // The publisher uses best-effort delivery.
                            // Notification failure does not roll back
                            // a successful SQLite transaction.
                            if (isNewEvent) {
                                publishLeaderboardUpdate(event);
                            }

                        } catch (JsonParseException
                                 | InvalidSwipeEventException e) {

                            // Invalid message: do not requeue.
                            System.err.println(
                                    "Invalid message for resort "
                                            + resortId + ": "
                                            + e.getMessage()
                            );

                            nackSafely(deliveryTag, false);
                            return;

                        } catch (Exception e) {

                            // Transient or unexpected processing error:
                            // request redelivery.
                            System.err.println(
                                    "Processing failure for resort "
                                            + resortId + ": " + e
                            );

                            nackSafely(deliveryTag, true);
                            return;
                        }

                        // Stage 5: ACK after successful processing.
                        //
                        // The SQLite transaction has already committed.
                        // If a duplicate was detected, ACK it too.
                        try {
                            synchronized (channel) {
                                channel.basicAck(
                                        deliveryTag,
                                        false
                                );
                            }

                        } catch (Exception e) {

                            // ACK outcome is uncertain.
                            // Do not NACK the same delivery here.
                            System.err.println(
                                    "ACK failed for resort "
                                            + resortId + ": " + e
                            );
                        }
                    };

                    // Preserve existing task admission logic.
                    boolean accepted = admissionGate.submit(
                            task,
                            serialExecutor
                    );

                    if (!accepted) {

                        System.err.println(
                                "Task not accepted for resort "
                                        + resortId
                                        + " (shutdown or worker rejection)."
                        );

                        nackSafely(deliveryTag, true);
                    }
                };

        CancelCallback cancelCallback =
                consumerTag -> System.err.println(
                        "Consumer cancelled for queue: "
                                + queueName
                );

        // autoAck = false: the application controls ACK/NACK.
        String consumerTag = channel.basicConsume(
                queueName,
                false,
                deliverCallback,
                cancelCallback
        );

        System.out.println(
                "Listening on queue: " + queueName
        );

        return consumerTag;
    }

    private void nackSafely(
            long deliveryTag,
            boolean requeue) {

        try {
            synchronized (channel) {

                if (channel.isOpen()) {
                    channel.basicNack(
                            deliveryTag,
                            false,
                            requeue
                    );
                }
            }

        } catch (IOException e) {
            System.err.println(
                    "Failed to NACK message: "
                            + e.getMessage()
            );
        }
    }

    private void publishLeaderboardUpdate(
            SwipeEvent event) {

        if (updatePublisher == null) {
            return;
        }

        try {
            updatePublisher.publish(
                    event.getEventId(),
                    event.getResortID(),
                    event.getDayID()
            );

        } catch (Exception e) {

            // SQLite has already committed.
            // Preserve best-effort notification semantics.
            System.err.println(
                    "Leaderboard notification failed for event "
                            + event.getEventId() + ": " + e
            );
        }
    }
}
