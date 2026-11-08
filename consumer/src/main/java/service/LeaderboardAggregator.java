
package service;

import model.SwipeEvent;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class LeaderboardAggregator {

    // resortID:dayID -> (skierID -> total vertical)
    private final Map<String, Map<String, Long>> totals =
            new ConcurrentHashMap<>();

    // Keep track of processed events for deduplication.
    private final Set<String> processedEventIds = new HashSet<>();

    /**
     * Atomically checks event ID and updates totals.
     *
     * @return true if this is a new event,
     *         false if it was already processed
     */
    public synchronized boolean processEvent(SwipeEvent event) {

        Objects.requireNonNull(event, "event");

        String eventId = Objects.requireNonNull(
                event.getEventId(), "eventId"
        );

        String resortId = Objects.requireNonNull(
                event.getResortID(), "resortID"
        );

        String dayId = Objects.requireNonNull(
                event.getDayID(), "dayID"
        );

        String skierId = Objects.requireNonNull(
                event.getSkierID(), "skierID"
        );

        if (event.getVertical() <= 0) {
            throw new IllegalArgumentException(
                    "Vertical must be positive"
            );
        }

        if (processedEventIds.contains(eventId)) {
            return false;
        }

        String key = resortId + ":" + dayId;

        Map<String, Long> skierTotals =
                totals.computeIfAbsent(
                        key,
                        ignored -> new HashMap<>()
                );

        skierTotals.merge(
                skierId,
                (long) event.getVertical(),
                Math::addExact
        );

        // Add eventId only after successful aggregation.
        processedEventIds.add(eventId);

        return true;
    }

    public synchronized long getTotalVertical(
            String resortId,
            String dayId,
            String skierId) {

        String key = resortId + ":" + dayId;

        Map<String, Long> skierTotals = totals.get(key);

        if (skierTotals == null) {
            return 0L;
        }

        return skierTotals.getOrDefault(skierId, 0L);
    }

    public synchronized List<Map.Entry<String, Long>> getTopN(
            String resortId, String dayId, int n) {

        if (n <= 0) {
            return Collections.emptyList();
        }

        String key = resortId + ":" + dayId;

        Map<String, Long> skierTotals = totals.get(key);

        if (skierTotals == null) {
            return Collections.emptyList();
        }

        List<Map.Entry<String, Long>> ranking =
                new ArrayList<>();

        for (Map.Entry<String, Long> entry : skierTotals.entrySet()) {
            ranking.add(
                    new AbstractMap.SimpleImmutableEntry<>(
                            entry.getKey(), entry.getValue()
                    )
            );
        }

        ranking.sort(
                Comparator
                        .<Map.Entry<String, Long>>comparingLong(
                                Map.Entry::getValue
                        )
                        .reversed()
                        .thenComparing(Map.Entry::getKey)
        );

        return new ArrayList<>(
                ranking.subList(
                        0, Math.min(n, ranking.size())
                )
        );
    }
}
