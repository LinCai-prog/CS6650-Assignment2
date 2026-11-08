
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
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class ResortConsumerPublisherTest {

    @Test
    void shouldPublishUpdateAfterSuccessfulCommit()
            throws Exception {

        Channel channel = mock(Channel.class);
        LeaderboardStore store = mock(LeaderboardStore.class);

        LeaderboardUpdatePublisher publisher =
                mock(LeaderboardUpdatePublisher.class);

        when(channel.isOpen()).thenReturn(true);

        when(channel.basicConsume(
                eq("resort.3"),
                eq(false),
                any(DeliverCallback.class),
                any(CancelCallback.class)
        )).thenReturn("test-consumer-tag");

        when(store.processEvent(any())).thenReturn(true);

        when(store.getTopN(
                anyString(),
                anyString(),
                anyInt()
        )).thenReturn(Collections.emptyList());

        ExecutorService workerPool =
                Executors.newSingleThreadExecutor();

        CountDownLatch ackObserved = new CountDownLatch(1);

        doAnswer(invocation -> {
            ackObserved.countDown();
            return null;
        }).when(channel).basicAck(42L, false);

        try {
            ResortSerialExecutor executor =
                    new ResortSerialExecutor(workerPool, 10);

            ResortConsumer consumer =
                    new ResortConsumer(
                            channel,
                            store,
                            executor,
                            publisher
                    );

            consumer.start(3);

            ArgumentCaptor<DeliverCallback> captor =
                    ArgumentCaptor.forClass(DeliverCallback.class);

            verify(channel).basicConsume(
                    eq("resort.3"),
                    eq(false),
                    captor.capture(),
                    any(CancelCallback.class)
            );

            String json = "{"
                    + "\"eventId\":\"publisher-test-001\","
                    + "\"resortID\":\"3\","
                    + "\"dayID\":\"1\","
                    + "\"skierID\":\"1234\","
                    + "\"liftID\":\"17\","
                    + "\"vertical\":300,"
                    + "\"time\":220,"
                    + "\"timestamp\":\"2026-11-07T23:30:00\","
                    + "\"serverId\":\"publisher-test\""
                    + "}";

            Delivery delivery = new Delivery(
                    new Envelope(
                            42L,
                            false,
                            "skitrak.exchange",
                            "resort.3"
                    ),
                    new AMQP.BasicProperties(),
                    json.getBytes(StandardCharsets.UTF_8)
            );

            captor.getValue().handle(
                    "test-consumer-tag",
                    delivery
            );

            assertTrue(
                    ackObserved.await(5, TimeUnit.SECONDS),
                    "ACK was not observed"
            );

            // Both operations must have occurred.
            verify(store).processEvent(any());

            verify(publisher).publish(
                    "publisher-test-001",
                    "3",
                    "1"
            );

            // Check ordering: commit processing before publish.
            org.mockito.InOrder order = inOrder(store, publisher);

            order.verify(store).processEvent(any());
            order.verify(publisher).publish(
                    "publisher-test-001",
                    "3",
                    "1"
            );

            verify(channel).basicAck(42L, false);

            verify(channel, never()).basicNack(
                    anyLong(),
                    anyBoolean(),
                    anyBoolean()
            );

        } finally {
            workerPool.shutdownNow();
        }
    }
}
