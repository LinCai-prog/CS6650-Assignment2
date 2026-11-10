package service;

import model.SwipeEvent;
import storage.LeaderboardStore;
import java.util.*;
import java.util.concurrent.*;

/** Bounded round-robin writer. A failed batch is retried before any later batch. */
public final class BatchingSwipeProcessor implements AutoCloseable {
    private static final class Pending {
        final SwipeEvent event;
        final long submitted = System.nanoTime();
        final CompletableFuture<Boolean> committed = new CompletableFuture<>();
        Pending(SwipeEvent event) { this.event = event; }
    }
    private final LeaderboardStore store;
    private final int capacity, batchSize;
    private final long waitNanos;
    private final Map<String, ArrayDeque<Pending>> queues = new LinkedHashMap<>();
    private final Thread writer;
    private boolean accepting = true;
    private int pending;
    private String previousResort;

    public BatchingSwipeProcessor(LeaderboardStore store, int batchSize, int waitMs, int capacity) {
        if (batchSize < 1 || waitMs < 1 || capacity < 1) throw new IllegalArgumentException("positive batch limits required");
        this.store = Objects.requireNonNull(store);
        this.batchSize = batchSize;
        this.waitNanos = TimeUnit.MILLISECONDS.toNanos(waitMs);
        this.capacity = capacity;
        writer = new Thread(this::run, "leaderboard-db-writer");
        writer.start();
    }

    public synchronized CompletableFuture<Boolean> submit(SwipeEvent event) {
        if (!accepting || pending >= capacity) throw new RejectedExecutionException("database writer admission closed/full");
        Pending item = new Pending(Objects.requireNonNull(event));
        queues.computeIfAbsent(event.getResortID(), ignored -> new ArrayDeque<>()).add(item);
        pending++;
        notifyAll();
        return item.committed;
    }

    private synchronized List<Pending> nextBatch() throws InterruptedException {
        while (pending == 0 && accepting) wait();
        if (pending == 0) return Collections.emptyList();
        long oldest = queues.values().stream().filter(q -> !q.isEmpty()).mapToLong(q -> q.peek().submitted).min().orElseThrow();
        long remaining;
        while (accepting && pending < batchSize && (remaining = waitNanos - (System.nanoTime() - oldest)) > 0) {
            TimeUnit.NANOSECONDS.timedWait(this, remaining);
        }
        List<Pending> batch = new ArrayList<>();
        List<String> resorts = new ArrayList<>(queues.keySet());
        int cursor = previousResort == null ? 0 : (resorts.indexOf(previousResort) + 1) % resorts.size();
        while (pending > 0 && batch.size() < batchSize) {
            String resort = resorts.get(cursor);
            Pending item = queues.get(resort).poll();
            if (item != null) { batch.add(item); pending--; previousResort = resort; }
            cursor = (cursor + 1) % resorts.size();
        }
        return batch;
    }

    private void run() {
        List<Pending> batch = Collections.emptyList();
        try {
            while (true) {
                batch = nextBatch();
                if (batch.isEmpty()) return;
                List<SwipeEvent> events = new ArrayList<>();
                for (Pending item : batch) events.add(item.event);
                int failures = 0;
                while (true) {
                    try {
                        List<Boolean> result = store.processBatch(events);
                        if (result.size() != batch.size()) throw new IllegalStateException("batch result size mismatch");
                        for (int i = 0; i < batch.size(); i++) batch.get(i).committed.complete(result.get(i));
                        break;
                    } catch (Exception failure) {
                        if (!isAccepting()) throw failure;
                        failures++;
                        if (failures == 1 || failures % 10 == 0) System.err.println("Database batch retry " + failures + ": " + failure.getClass().getSimpleName());
                        Thread.sleep(Math.min(1000, 50L * failures));
                    }
                }
            }
        } catch (Exception stopped) {
            if (stopped instanceof InterruptedException) Thread.currentThread().interrupt();
            for (Pending item : batch) item.committed.completeExceptionally(stopped);
            synchronized (this) {
                accepting = false;
                for (var queue : queues.values()) for (Pending item : queue) item.committed.completeExceptionally(stopped);
                queues.clear(); pending = 0;
            }
        }
    }

    private synchronized boolean isAccepting() { return accepting; }

    @Override public void close() throws InterruptedException {
        synchronized (this) { accepting = false; notifyAll(); }
        writer.join(60000);
        if (writer.isAlive()) { writer.interrupt(); writer.join(5000); }
        if (writer.isAlive()) throw new IllegalStateException("database writer did not stop");
    }
}
