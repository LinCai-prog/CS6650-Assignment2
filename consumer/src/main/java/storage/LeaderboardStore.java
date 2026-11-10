
package storage;

import model.SwipeEvent;
import model.LeaderboardUpdate;

import java.util.List;
import java.util.Map;

public interface LeaderboardStore extends AutoCloseable {

    // Atomically record an event and update vertical totals.
    // Returns false if eventId has already been processed.
    boolean processEvent(SwipeEvent event) throws Exception;

    // All events, per-event standings changes and outbox entries commit atomically.
    List<Boolean> processBatch(List<SwipeEvent> events) throws Exception;

    // Updates are committed atomically with aggregation, only if the top-N changed.
    List<LeaderboardUpdate> getPendingUpdates(int limit) throws Exception;

    void markUpdatePublished(LeaderboardUpdate update) throws Exception;

    // Read standings and their version from a single database snapshot.
    LeaderboardUpdate getSnapshot(String resortId, String dayId) throws Exception;

    // Return the total vertical for one skier.
    long getTotalVertical(
            String resortId,
            String dayId,
            String skierId
    ) throws Exception;

    // Return the top N skiers ordered by vertical descending.
    List<Map.Entry<String, Long>> getTopN(
            String resortId,
            String dayId,
            int limit
    ) throws Exception;

    @Override
    void close() throws Exception;
}
