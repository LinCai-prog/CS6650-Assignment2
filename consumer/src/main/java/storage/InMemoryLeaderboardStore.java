package storage;

import model.LeaderboardUpdate;
import model.SwipeEvent;
import service.AggregationMetrics;

import java.time.Instant;
import java.util.*;

/** Process-local standings, event IDs and pending publications. */
public final class InMemoryLeaderboardStore implements LeaderboardStore {
    private static final int OUTBOX_BACKPRESSURE_THRESHOLD = 10_000;

    private static final Comparator<Map.Entry<String, Long>> RANK =
            Comparator.<Map.Entry<String, Long>>comparingLong(Map.Entry::getValue).reversed()
                    .thenComparing(Map.Entry::getKey);

    private static final class Standings {
        final Map<String, Long> totals = new HashMap<>();
        final NavigableSet<Map.Entry<String, Long>> ranking = new TreeSet<>(RANK);
        List<Map.Entry<String, Long>> top = List.of();
        long version;
    }

    private final Map<List<String>, Standings> boards = new HashMap<>();
    private final Set<String> processed = new HashSet<>();
    private final Queue<LeaderboardUpdate> pending = new ArrayDeque<>();
    private final AggregationMetrics metrics = new AggregationMetrics();

    @Override public boolean processEvent(SwipeEvent event) {
        return processBatch(List.of(event)).get(0);
    }

    @Override public synchronized List<Boolean> processBatch(List<SwipeEvent> events) {
        // Validate the entire batch before changing any standings or event IDs.
        for (SwipeEvent event : events) {
            Objects.requireNonNull(event, "event");
            if (event.getEventId() == null || event.getEventId().isBlank()
                    || event.getResortID() == null || !event.getResortID().matches("[0-9]+")
                    || event.getDayID() == null || !event.getDayID().matches("[0-9]+")
                    || event.getSkierID() == null || event.getSkierID().isBlank()
                    || event.getVertical() <= 0) {
                throw new IllegalArgumentException("Invalid swipe for aggregation");
            }
        }
        // The batcher retries without ACK until the dispatcher makes room.
        if (pending.size() >= OUTBOX_BACKPRESSURE_THRESHOLD) {
            throw new IllegalStateException("In-memory leaderboard outbox is full");
        }
        long batchStarted = System.nanoTime();
        List<Boolean> fresh = new ArrayList<>(events.size());
        for (var event : events) {
            Instant started = Instant.now();
            long startedNanos = System.nanoTime();
            if (!processed.add(event.getEventId())) {
                fresh.add(false);
                metrics.recordDuplicate();
                continue;
            }
            fresh.add(true);
            String resort = event.getResortID(), day = event.getDayID(), skier = event.getSkierID();
            var board = boards.computeIfAbsent(List.of(resort, day), ignored -> new Standings());
            long previous = board.totals.getOrDefault(skier, 0L);
            long total = previous + event.getVertical();
            board.ranking.remove(Map.entry(skier, previous));
            board.totals.put(skier, total);
            var entry = Map.entry(skier, total);
            board.ranking.add(entry);
            boolean changed = board.top.size() < LeaderboardUpdate.TOP_N
                    || RANK.compare(entry, board.top.get(board.top.size() - 1)) <= 0;
            if (changed) {
                board.top = top(board, LeaderboardUpdate.TOP_N);
                pending.add(new LeaderboardUpdate(event.getEventId(), resort, day, ++board.version,
                        event.getTimestamp(), Instant.now().toString(), board.top));
            }
            metrics.recordProcessed(event.getTimestamp(), changed, started);
            metrics.recordStages(event, startedNanos);
        }
        metrics.recordBatch(events.size(), System.nanoTime() - batchStarted);
        return fresh;
    }

    @Override public synchronized List<LeaderboardUpdate> getPendingUpdates(int limit) {
        if (limit <= 0) throw new IllegalArgumentException("limit must be positive");
        var result = new ArrayList<LeaderboardUpdate>();
        for (var update : pending) {
            if (result.size() == limit) break;
            result.add(update);
        }
        return result;
    }

    @Override public synchronized void markUpdatePublished(LeaderboardUpdate update) {
        if (pending.peek() != update) throw new IllegalStateException("Outbox publication was out of order");
        pending.remove();
    }

    @Override public synchronized LeaderboardUpdate getSnapshot(String resort, String day) {
        var board = boards.get(List.of(resort, day));
        return new LeaderboardUpdate("snapshot", resort, day, board == null ? 0 : board.version,
                null, null, board == null ? List.of() : board.top);
    }

    @Override public synchronized long getTotalVertical(String resort, String day, String skier) {
        var board = boards.get(List.of(resort, day));
        return board == null ? 0 : board.totals.getOrDefault(skier, 0L);
    }

    @Override public synchronized List<Map.Entry<String, Long>> getTopN(String resort, String day, int limit) {
        if (limit <= 0) throw new IllegalArgumentException("limit must be positive");
        var board = boards.get(List.of(resort, day));
        return board == null ? List.of() : top(board, limit);
    }

    private static List<Map.Entry<String, Long>> top(Standings board, int limit) {
        var result = new ArrayList<Map.Entry<String, Long>>();
        for (var entry : board.ranking) {
            if (result.size() == limit) break;
            result.add(entry);
        }
        return result;
    }

    @Override public String getMetricsSummary() {
        return metrics.summary();
    }

    @Override public void close() {}
}
