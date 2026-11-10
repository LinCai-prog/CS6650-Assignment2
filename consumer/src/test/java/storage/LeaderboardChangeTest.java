package storage;

import com.google.gson.JsonObject;
import com.google.gson.Gson;
import model.SwipeEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import rabbitmq.LeaderboardOutboxDispatcher;
import rabbitmq.LeaderboardUpdatePublisher;
import java.nio.file.Path;
import java.sql.DriverManager;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class LeaderboardChangeTest {
    @TempDir Path temp;

    private SwipeEvent swipe(String id, String resort, String day, String skier, int vertical) {
        var json = new JsonObject();
        json.addProperty("eventId", id);
        json.addProperty("resortID", resort);
        json.addProperty("dayID", day);
        json.addProperty("skierID", skier);
        json.addProperty("vertical", vertical);
        json.addProperty("timestamp", "2026-11-09T20:00:00Z");
        return new Gson().fromJson(json, SwipeEvent.class);
    }

    @Test
    void shouldNotifyOnlyWhenTopTenChangesIncludingScoreOnlyChanges() throws Exception {
        try (var store = new SqliteLeaderboardStore(temp.resolve("changes.db").toString())) {
            for (int i = 0; i < 10; i++) {
                assertTrue(store.processEvent(swipe("seed-" + i, "3", "1", "s" + i, 1000 + i * 100)));
            }
            assertEquals(10, store.getPendingUpdates(100).size());
            var outsider = swipe("outside", "3", "1", "outsider", 100);
            assertTrue(store.processEvent(outsider));
            assertEquals(100, store.getTotalVertical("3", "1", "outsider"));
            assertEquals(10, store.getPendingUpdates(100).size());
            assertFalse(store.processEvent(outsider));
            assertEquals(10, store.getPendingUpdates(100).size());

            store.processEvent(swipe("enter", "3", "1", "outsider", 2000));
            var entered = store.getPendingUpdates(100).get(10);
            assertEquals("outsider", entered.getStandings().get(0).getKey());
            assertEquals(2100L, entered.getStandings().get(0).getValue());
            store.processEvent(swipe("leader-score", "3", "1", "outsider", 100));
            var changed = store.getPendingUpdates(100).get(11);
            assertEquals(2200L, changed.getStandings().get(0).getValue());
            assertEquals(12, changed.getVersion());
            // Earlier notifications retain historical standings, not the latest DB value.
            assertEquals(2100L, entered.getStandings().get(0).getValue());
            assertThrows(UnsupportedOperationException.class,
                    () -> entered.getStandings().get(0).setValue(1L));
        }
    }

    @Test
    void shouldHandleTieMembershipAndIsolateDaysAndResorts() throws Exception {
        try (var store = new SqliteLeaderboardStore(temp.resolve("ties.db").toString())) {
            for (int i = 0; i < 10; i++) store.processEvent(swipe("e" + i, "3", "1", "s" + i, 100));
            store.processEvent(swipe("tie-out", "3", "1", "z", 100));
            assertEquals(10, store.getPendingUpdates(100).size());
            store.processEvent(swipe("tie-in", "3", "1", "a", 100));
            assertEquals("a", store.getSnapshot("3", "1").getStandings().get(0).getKey());
            assertEquals(11, store.getSnapshot("3", "1").getVersion());
            store.processEvent(swipe("other-day", "3", "2", "s0", 100));
            store.processEvent(swipe("other-resort", "5", "1", "s0", 200));
            assertEquals(1, store.getSnapshot("3", "2").getVersion());
            assertEquals(1, store.getSnapshot("5", "1").getVersion());
        }
    }

    @Test
    void shouldPersistPendingUpdatesAcrossRestartAndRetryWithoutReaggregating() throws Exception {
        var path = temp.resolve("restart.db").toString();
        var event = swipe("event", "3", "1", "skier", 300);
        try (var store = new SqliteLeaderboardStore(path)) {
            store.processEvent(event);
        }
        try (var store = new SqliteLeaderboardStore(path)) {
            var publisher = mock(LeaderboardUpdatePublisher.class);
            doThrow(new java.io.IOException("broker unavailable")).doNothing().when(publisher).publish(any());
            try (var dispatcher = new LeaderboardOutboxDispatcher(store, publisher)) {
                assertThrows(java.io.IOException.class, dispatcher::dispatchPending);
                assertEquals(1, store.getPendingUpdates(100).size());
                assertFalse(store.processEvent(event));
                dispatcher.dispatchPending();
                assertTrue(store.getPendingUpdates(100).isEmpty());
                assertEquals(300, store.getTotalVertical("3", "1", "skier"));
                assertEquals(1, store.getSnapshot("3", "1").getVersion());
                store.processEvent(swipe("next", "3", "1", "skier", 100));
                assertEquals(2, store.getPendingUpdates(100).get(0).getVersion());
            }
        }
    }

    @Test
    void shouldRollbackTotalsDedupAndVersionWhenOutboxWriteFails() throws Exception {
        var path = temp.resolve("rollback.db").toString();
        try (var store = new SqliteLeaderboardStore(path);
             var db = DriverManager.getConnection("jdbc:sqlite:" + path)) {
            db.createStatement().execute("CREATE TRIGGER fail_outbox BEFORE INSERT ON leaderboard_outbox "
                    + "BEGIN SELECT RAISE(ABORT, 'test failure'); END");
            var event = swipe("retry-me", "3", "1", "skier", 300);
            assertThrows(java.sql.SQLException.class, () -> store.processEvent(event));
            assertEquals(0, store.getTotalVertical("3", "1", "skier"));
            assertEquals(0, store.getSnapshot("3", "1").getVersion());
            db.createStatement().execute("DROP TRIGGER fail_outbox");
            assertTrue(store.processEvent(event));
            assertEquals(1, store.getPendingUpdates(100).size());
        }
    }

    @Test
    void shouldReadConsistentSnapshotFromSecondConnection() throws Exception {
        var path = temp.resolve("shared.db").toString();
        try (var writer = new SqliteLeaderboardStore(path); var reader = new SqliteLeaderboardStore(path)) {
            writer.processEvent(swipe("first", "3", "1", "skier", 300));
            var first = reader.getSnapshot("3", "1");
            writer.processEvent(swipe("second", "3", "1", "skier", 100));
            assertEquals(1, first.getVersion());
            assertEquals(300L, first.getStandings().get(0).getValue());
            assertEquals(2, reader.getSnapshot("3", "1").getVersion());
            assertEquals(400L, reader.getSnapshot("3", "1").getStandings().get(0).getValue());
        }
    }
}
