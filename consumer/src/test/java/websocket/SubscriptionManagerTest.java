
package websocket;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class SubscriptionManagerTest {

    @Test
    void shouldTreatEquivalentKeysAsEqual() {
        SubscriptionKey key1 = new SubscriptionKey("3", "1");
        SubscriptionKey key2 = new SubscriptionKey("3", "1");

        assertEquals(key1, key2);
        assertEquals(key1.hashCode(), key2.hashCode());
    }

    @Test
    void shouldSubscribeMultipleClientsToSameLeaderboard() {
        SubscriptionManager manager = new SubscriptionManager();
        SubscriptionKey key = new SubscriptionKey("3", "1");

        manager.subscribe("alice", key);
        manager.subscribe("bob", key);

        assertEquals(
                Set.of("alice", "bob"),
                manager.getSubscribers(key)
        );
    }

    @Test
    void shouldAllowOneSessionToSubscribeToMultipleLeaderboards() {
        SubscriptionManager manager = new SubscriptionManager();

        SubscriptionKey key1 = new SubscriptionKey("3", "1");
        SubscriptionKey key2 = new SubscriptionKey("5", "2");

        manager.subscribe("bob", key1);
        manager.subscribe("bob", key2);

        assertEquals(
                Set.of(key1, key2),
                manager.getSubscriptions("bob")
        );
    }

    @Test
    void shouldRemoveAllSubscriptionsWhenSessionDisconnects() {
        SubscriptionManager manager = new SubscriptionManager();

        SubscriptionKey key1 = new SubscriptionKey("3", "1");
        SubscriptionKey key2 = new SubscriptionKey("5", "2");

        manager.subscribe("alice", key1);
        manager.subscribe("bob", key1);
        manager.subscribe("bob", key2);

        manager.removeSession("bob");

        assertEquals(
                Set.of("alice"),
                manager.getSubscribers(key1)
        );

        assertTrue(manager.getSubscribers(key2).isEmpty());
        assertTrue(manager.getSubscriptions("bob").isEmpty());
    }

    @Test
    void shouldSupportConcurrentSubscriptions() throws Exception {
        SubscriptionManager manager = new SubscriptionManager();
        SubscriptionKey key = new SubscriptionKey("3", "1");

        int clientCount = 100;
        ExecutorService pool = Executors.newFixedThreadPool(10);
        CountDownLatch startLatch = new CountDownLatch(1);

        try {
            List<Future<?>> futures = new ArrayList<>();

            for (int i = 0; i < clientCount; i++) {
                final String sessionId = "client-" + i;

                futures.add(pool.submit(() -> {
                    startLatch.await();
                    manager.subscribe(sessionId, key);
                    return null;
                }));
            }

            startLatch.countDown();

            for (Future<?> future : futures) {
                future.get(10, TimeUnit.SECONDS);
            }

            assertEquals(
                    clientCount,
                    manager.getSubscribers(key).size()
            );

        } finally {
            startLatch.countDown();
            pool.shutdownNow();
        }
    }
}
