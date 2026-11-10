package rabbitmq;

import com.rabbitmq.client.*;
import concurrency.ResortSerialExecutor;
import model.SwipeEvent;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import service.BatchingSwipeProcessor;
import storage.LeaderboardStore;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;
import static org.junit.jupiter.api.Assertions.*;

class ReceiptTimingTest {
    @Test void capturesReceiptBeforeTheBlockedWorkerExecutorRuns() throws Exception {
        var channel = mock(Channel.class); when(channel.isOpen()).thenReturn(true);
        var store = mock(LeaderboardStore.class);
        var eventSeen = new CompletableFuture<SwipeEvent>();
        when(store.processBatch(anyList())).thenAnswer(inv -> {
            List<SwipeEvent> events = inv.getArgument(0);
            eventSeen.complete(events.get(0));
            return Collections.nCopies(events.size(), true);
        });
        var pool = Executors.newSingleThreadExecutor();
        var occupied = new CountDownLatch(1); var release = new CountDownLatch(1);
        pool.submit(() -> { occupied.countDown(); release.await(); return null; });
        assertTrue(occupied.await(2, TimeUnit.SECONDS));
        try (var writer = new BatchingSwipeProcessor(store, 1, 5, 10);
             var consumer = new ResortConsumer(channel, writer, new ResortSerialExecutor(pool, 10))) {
            consumer.start(1);
            var callback = ArgumentCaptor.forClass(DeliverCallback.class);
            verify(channel).basicConsume(eq("resort.1"), eq(false), callback.capture(), any(CancelCallback.class));
            String json = "{\"eventId\":\"receipt\",\"resortID\":\"1\",\"dayID\":\"1\",\"skierID\":\"42\",\"liftID\":\"1\",\"vertical\":100,\"time\":100,\"timestamp\":\"2026-11-10T00:00:00Z\",\"serverId\":\"test\"}";
            long beforeCallback = System.nanoTime();
            callback.getValue().handle("test", new Delivery(new Envelope(1, false, "skitrak.exchange", "resort.1"),
                    new AMQP.BasicProperties(), json.getBytes(StandardCharsets.UTF_8)));
            long afterCallback = System.nanoTime();
            assertFalse(eventSeen.isDone());
            release.countDown();
            var event = eventSeen.get(2, TimeUnit.SECONDS);
            assertNotNull(event.getReceivedAt());
            assertTrue(event.getReceivedNanos() >= beforeCallback);
            assertTrue(event.getReceivedNanos() <= afterCallback);
            verify(channel, timeout(2000)).basicAck(1, true);
        } finally {
            release.countDown(); pool.shutdownNow();
        }
    }
}
