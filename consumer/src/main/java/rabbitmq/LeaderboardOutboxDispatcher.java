package rabbitmq;

import storage.LeaderboardStore;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** One dispatcher per aggregation database; failures leave the update pending. */
public final class LeaderboardOutboxDispatcher implements AutoCloseable {
    private final LeaderboardStore store;
    private final LeaderboardUpdatePublisher publisher;
    private final ScheduledExecutorService worker = Executors.newSingleThreadScheduledExecutor();

    public LeaderboardOutboxDispatcher(LeaderboardStore store, LeaderboardUpdatePublisher publisher) {
        this.store = Objects.requireNonNull(store);
        this.publisher = Objects.requireNonNull(publisher);
    }

    public void start() {
        worker.scheduleWithFixedDelay(() -> {
            try {
                dispatchPending();
            } catch (Exception e) {
                System.err.println("Leaderboard publication deferred for retry: " + e);
            }
        }, 0, 25, TimeUnit.MILLISECONDS);
    }

    public synchronized void dispatchPending() throws Exception {
        for (var update : store.getPendingUpdates(100)) {
            publisher.publish(update);
            // Only remove after the broker confirms. A crash here can cause a
            // duplicate publication, which the WebSocket version check suppresses.
            store.markUpdatePublished(update);
        }
    }

    @Override
    public void close() throws InterruptedException {
        worker.shutdown();
        if (!worker.awaitTermination(10, TimeUnit.SECONDS)) {
            worker.shutdownNow();
            if (!worker.awaitTermination(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Leaderboard dispatcher is still stopping");
            }
        }
    }
}
