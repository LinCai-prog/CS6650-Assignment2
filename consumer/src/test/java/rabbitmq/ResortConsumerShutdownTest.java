
package rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.Envelope;

import concurrency.ResortSerialExecutor;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import storage.LeaderboardStore;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class ResortConsumerShutdownTest {

    @Test
    void shouldNackMessageWhenShutdownHasStarted()
            throws Exception {

        // 1. Mock RabbitMQ Channel and SQLite Store.
        Channel channel = mock(Channel.class);
        LeaderboardStore store = mock(LeaderboardStore.class);

        when(channel.isOpen()).thenReturn(true);

        when(channel.basicConsume(
                eq("resort.3"),
                eq(false),
                any(DeliverCallback.class),
                any(CancelCallback.class)
        )).thenReturn("test-consumer-tag");

        // 2. Create a real worker pool and serial executor.
        ExecutorService workerPool =
                Executors.newFixedThreadPool(1);

        try {
            ResortSerialExecutor executor =
                    new ResortSerialExecutor(workerPool, 10);

            ResortConsumer consumer =
                    new ResortConsumer(
                            channel,
                            store,
                            executor
                    );

            // 3. Start consumer and capture its callback.
            consumer.start(3);

            ArgumentCaptor<DeliverCallback> callbackCaptor =
                    ArgumentCaptor.forClass(
                            DeliverCallback.class
                    );

            verify(channel).basicConsume(
                    eq("resort.3"),
                    eq(false),
                    callbackCaptor.capture(),
                    any(CancelCallback.class)
            );

            DeliverCallback callback =
                    callbackCaptor.getValue();

            // 4. Simulate shutdown before message delivery.
            consumer.stopAccepting();

            // 5. Create a simulated RabbitMQ delivery.
            String json = "{"
                    + "\"eventId\":\"shutdown-test-001\","
                    + "\"resortID\":\"3\","
                    + "\"dayID\":\"1\","
                    + "\"skierID\":\"1234\","
                    + "\"liftID\":\"17\","
                    + "\"vertical\":213,"
                    + "\"time\":217,"
                    + "\"timestamp\":\"2026-11-07T21:00:00\","
                    + "\"serverId\":\"shutdown-test\""
                    + "}";

            byte[] body =
                    json.getBytes(StandardCharsets.UTF_8);

            long deliveryTag = 42L;

            Envelope envelope = new Envelope(
                    deliveryTag,
                    false,
                    "skitrak.exchange",
                    "resort.3"
            );

            Delivery delivery = new Delivery(
                    envelope,
                    new AMQP.BasicProperties(),
                    body
            );

            // 6. Simulate RabbitMQ invoking the callback.
            callback.handle("test-consumer-tag", delivery);

            // 7. Verify NACK with requeue=true.
            verify(channel).basicNack(
                    42L,
                    false,
                    true
            );

            // 8. No ACK should be sent.
            verify(channel, never()).basicAck(
                    anyLong(),
                    anyBoolean()
            );

            // 9. SQLite must not process this message.
            verifyNoInteractions(store);

        } finally {
            workerPool.shutdownNow();
        }
    }
}
