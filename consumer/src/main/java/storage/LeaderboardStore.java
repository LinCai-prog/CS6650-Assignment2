
package storage;

import model.SwipeEvent;

import java.util.List;
import java.util.Map;

public interface LeaderboardStore extends AutoCloseable {

    // Atomically record an event and update vertical totals.
    // Returns false if eventId has already been processed.
    boolean processEvent(SwipeEvent event) throws Exception;

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
