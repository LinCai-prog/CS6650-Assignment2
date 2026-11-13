package storage;

import com.google.gson.Gson;
import model.SwipeEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class InMemoryLeaderboardStoreTest {
    @TempDir Path directory;

    private static SwipeEvent swipe(String id, String resort, String day, String skier, int vertical) {
        var data = Map.of("eventId", id, "resortID", resort, "dayID", day, "skierID", skier, "vertical", vertical);
        Gson gson = new Gson();
        return gson.fromJson(gson.toJsonTree(data), SwipeEvent.class);
    }

    @Test void matchesSqliteForTiesDuplicatesAndEveryIntermediateUpdate() throws Exception {
        List<SwipeEvent> events = new ArrayList<>();
        for (int i = 0; i < 10; i++) events.add(swipe("seed-" + i, "1", "1", "s" + i, 100));
        var entersAtTie = swipe("tie", "1", "1", "a", 100);
        events.addAll(List.of(swipe("outsider", "1", "1", "z", 100), entersAtTie, entersAtTie,
                swipe("lead", "1", "1", "a", 90), swipe("different-day", "1", "2", "s0", 80),
                swipe("different-resort", "2", "1", "s0", 60), swipe("tie-after", "1", "1", "z", 100)));
        try (var memory = new InMemoryLeaderboardStore();
             var sqlite = new SqliteLeaderboardStore(directory.resolve("comparison.db").toString())) {
            assertEquals(sqlite.processBatch(events), memory.processBatch(events));
            var expected = sqlite.getPendingUpdates(100);
            var actual = memory.getPendingUpdates(100);
            assertEquals(expected.size(), actual.size());
            for (int i = 0; i < expected.size(); i++) {
                assertEquals(expected.get(i).getEventId(), actual.get(i).getEventId());
                assertEquals(expected.get(i).getVersion(), actual.get(i).getVersion());
                assertEquals(expected.get(i).getStandings(), actual.get(i).getStandings());
                memory.markUpdatePublished(actual.get(i));
            }
            assertTrue(memory.getPendingUpdates(1).isEmpty());
            assertEquals(sqlite.getTopN("1","1",100), memory.getTopN("1","1",100));
            assertEquals(sqlite.getSnapshot("1","1").getVersion(), memory.getSnapshot("1","1").getVersion());
            assertEquals(200, memory.getTotalVertical("1","1","z"));
        }
    }

    @Test void invalidBatchLeavesNoPartialStateAndValidBatchCanRetry() {
        try (var memory = new InMemoryLeaderboardStore()) {
            var valid = swipe("valid", "1", "1", "skier", 100);
            var invalid = swipe("invalid", "1", "malformed", "skier", 100);
            assertThrows(IllegalArgumentException.class, () -> memory.processBatch(List.of(valid, invalid)));
            assertEquals(0, memory.getSnapshot("1", "1").getVersion());
            assertTrue(memory.getPendingUpdates(1).isEmpty());
            assertTrue(memory.processEvent(valid));
            assertFalse(memory.processEvent(valid));
        }
    }
}
