
package storage;

import model.SwipeEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import com.google.gson.Gson;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;


class SqliteLeaderboardStoreTest {


    private SwipeEvent createEvent(
            String eventId,
            int vertical) {

        return createEvent(
                eventId, "3", "1", "1234", vertical);
    }

    private SwipeEvent createEvent(
            String eventId,
            String resortId,
            String dayId,
            String skierId,
            int vertical) {

        String json = "{"
                + "\"eventId\":\"" + eventId + "\","
                + "\"resortID\":\"" + resortId + "\","
                + "\"dayID\":\"" + dayId + "\","
                + "\"skierID\":\"" + skierId + "\","
                + "\"liftID\":\"17\","
                + "\"vertical\":" + vertical + ","
                + "\"time\":217,"
                + "\"timestamp\":\"2026-11-07T17:35:00\","
                + "\"serverId\":\"test-server\""
                + "}";

        return new Gson().fromJson(json, SwipeEvent.class);
    }



    @TempDir
    Path tempDir;

    @Test
    void shouldCreateDatabaseFile() throws Exception {

        Path dbPath = tempDir.resolve("leaderboard.db");

        assertFalse(dbPath.toFile().exists());

        try (SqliteLeaderboardStore store =
                     new SqliteLeaderboardStore(dbPath.toString())) {

            assertTrue(dbPath.toFile().exists());
        }
    }

    @Test
    void shouldReopenExistingDatabase() throws Exception {

        Path dbPath = tempDir.resolve("leaderboard.db");

        try (SqliteLeaderboardStore store =
                     new SqliteLeaderboardStore(dbPath.toString())) {
            // First connection initializes the database.
        }

        try (SqliteLeaderboardStore store =
                     new SqliteLeaderboardStore(dbPath.toString())) {
            // Second connection must not fail.
        }

        assertTrue(dbPath.toFile().exists());
    }


    @Test
    void shouldInsertNewEvent() throws Exception {
        Path dbPath = tempDir.resolve("new-event.db");

        try (SqliteLeaderboardStore store =
                     new SqliteLeaderboardStore(dbPath.toString())) {

            SwipeEvent event = createEvent("event-A", 213);

            assertTrue(store.processEvent(event));
        }
    }

    @Test
    void shouldIgnoreDuplicateEvent() throws Exception {
        Path dbPath = tempDir.resolve("duplicate.db");

        try (SqliteLeaderboardStore store =
                     new SqliteLeaderboardStore(dbPath.toString())) {

            SwipeEvent event = createEvent("event-A", 213);

            assertTrue(store.processEvent(event));
            assertFalse(store.processEvent(event));
        }
    }

    @Test
    void shouldRestoreDuplicateDetectionAfterRestart()
            throws Exception {

        Path dbPath = tempDir.resolve("restart.db");

        SwipeEvent event = createEvent("event-A", 213);

        try (SqliteLeaderboardStore store =
                     new SqliteLeaderboardStore(dbPath.toString())) {

            assertTrue(store.processEvent(event));
        }

        try (SqliteLeaderboardStore store =
                     new SqliteLeaderboardStore(dbPath.toString())) {

            assertFalse(store.processEvent(event));
        }
    }


    @Test
    void shouldAccumulateVerticalForSameSkier() throws Exception {
        Path dbPath = tempDir.resolve("accumulate.db");

        try (SqliteLeaderboardStore store =
                     new SqliteLeaderboardStore(dbPath.toString())) {

            store.processEvent(createEvent("event-A", 213));
            store.processEvent(createEvent("event-B", 167));

            assertEquals(
                    380,
                    store.getTotalVertical("3", "1", "1234")
            );
        }
    }

    @Test
    void shouldNotDoubleCountDuplicateEvent() throws Exception {
        Path dbPath = tempDir.resolve("dedup-total.db");

        try (SqliteLeaderboardStore store =
                     new SqliteLeaderboardStore(dbPath.toString())) {

            SwipeEvent event = createEvent("event-A", 213);

            assertTrue(store.processEvent(event));
            assertFalse(store.processEvent(event));

            assertEquals(
                    213,
                    store.getTotalVertical("3", "1", "1234")
            );
        }
    }

    @Test
    void shouldRestoreTotalAfterRestart() throws Exception {
        Path dbPath = tempDir.resolve("restore-total.db");

        try (SqliteLeaderboardStore store =
                     new SqliteLeaderboardStore(dbPath.toString())) {

            store.processEvent(createEvent("event-A", 213));
            store.processEvent(createEvent("event-B", 167));
        }

        try (SqliteLeaderboardStore store =
                     new SqliteLeaderboardStore(dbPath.toString())) {

            assertEquals(
                    380,
                    store.getTotalVertical("3", "1", "1234")
            );
        }
    }

    @Test
    void shouldReturnTopN() throws Exception {
        Path dbPath = tempDir.resolve("topn.db");

        try (SqliteLeaderboardStore store =
                     new SqliteLeaderboardStore(dbPath.toString())) {

            store.processEvent(createEvent("event-A", 213));
            store.processEvent(createEvent("event-B", 167));

            List<Map.Entry<String, Long>> top =
                    store.getTopN("3", "1", 10);

            assertEquals(1, top.size());
            assertEquals("1234", top.get(0).getKey());
            assertEquals(380L, top.get(0).getValue());
        }
    }


    @Test
    void shouldRollbackEventIdWhenTotalUpdateFails()
            throws Exception {

        Path dbPath = tempDir.resolve("rollback.db");

        // First, create the database and its tables.
        try (SqliteLeaderboardStore store =
                     new SqliteLeaderboardStore(dbPath.toString())) {
            // Database initialized.
        }

        // Create a trigger that intentionally rejects total updates.
        try (java.sql.Connection conn =
                     java.sql.DriverManager.getConnection(
                             "jdbc:sqlite:" + dbPath);
             java.sql.Statement stmt = conn.createStatement()) {

            stmt.execute(
                    "CREATE TRIGGER fail_total_insert " +
                            "BEFORE INSERT ON skier_totals " +
                            "BEGIN " +
                            "SELECT RAISE(ABORT, 'Simulated failure'); " +
                            "END"
            );
        }

        SwipeEvent event = createEvent("event-rollback", 213);

        // The transaction must fail.
        try (SqliteLeaderboardStore store =
                     new SqliteLeaderboardStore(dbPath.toString())) {

            assertThrows(
                    java.sql.SQLException.class,
                    () -> store.processEvent(event)
            );
        }

        // Remove the artificial failure and retry.
        try (java.sql.Connection conn =
                     java.sql.DriverManager.getConnection(
                             "jdbc:sqlite:" + dbPath);
             java.sql.Statement stmt = conn.createStatement()) {

            stmt.execute("DROP TRIGGER fail_total_insert");
        }

        try (SqliteLeaderboardStore store =
                     new SqliteLeaderboardStore(dbPath.toString())) {

            // Must be treated as a new event.
            assertTrue(store.processEvent(event));

            assertEquals(
                    213,
                    store.getTotalVertical("3", "1", "1234")
            );
        }
    }


    @Test
    void shouldRankMultipleSkiersCorrectly() throws Exception {
        Path dbPath = tempDir.resolve("ranking.db");

        try (SqliteLeaderboardStore store =
                     new SqliteLeaderboardStore(dbPath.toString())) {

            store.processEvent(
                    createEvent("A", "3", "1", "1234", 213));
            store.processEvent(
                    createEvent("B", "3", "1", "1234", 167));
            store.processEvent(
                    createEvent("C", "3", "1", "5678", 426));
            store.processEvent(
                    createEvent("D", "3", "1", "9999", 100));

            List<Map.Entry<String, Long>> top =
                    store.getTopN("3", "1", 2);

            assertEquals(2, top.size());

            assertEquals("5678", top.get(0).getKey());
            assertEquals(426L, top.get(0).getValue());

            assertEquals("1234", top.get(1).getKey());
            assertEquals(380L, top.get(1).getValue());
        }
    }


    @Test
    void shouldIsolateResortsAndDays() throws Exception {
        Path dbPath = tempDir.resolve("isolation.db");

        try (SqliteLeaderboardStore store =
                     new SqliteLeaderboardStore(dbPath.toString())) {

            store.processEvent(
                    createEvent("A", "3", "1", "1234", 213));

            store.processEvent(
                    createEvent("B", "5", "1", "1234", 500));

            store.processEvent(
                    createEvent("C", "3", "2", "1234", 300));

            assertEquals(213,
                    store.getTotalVertical("3", "1", "1234"));

            assertEquals(500,
                    store.getTotalVertical("5", "1", "1234"));

            assertEquals(300,
                    store.getTotalVertical("3", "2", "1234"));
        }
    }


    @Test
    void shouldProcessConcurrentEventsSafely() throws Exception {

        Path dbPath = tempDir.resolve("concurrent.db");

        int threadCount = 10;
        int eventCount = 100;

        ExecutorService pool =
                Executors.newFixedThreadPool(threadCount);

        try (SqliteLeaderboardStore store =
                     new SqliteLeaderboardStore(dbPath.toString())) {

            CountDownLatch startLatch = new CountDownLatch(1);

            List<Future<Boolean>> futures =
                    new java.util.ArrayList<>();

            // Submit 100 unique events.
            for (int i = 0; i < eventCount; i++) {
                final int index = i;

                futures.add(pool.submit(() -> {
                    startLatch.await();

                    SwipeEvent event = createEvent(
                            "concurrent-" + index,
                            10
                    );

                    return store.processEvent(event);
                }));
            }

            // Release all worker threads.
            startLatch.countDown();

            // Every unique event must be processed.
            for (Future<Boolean> future : futures) {
                assertTrue(future.get(30, TimeUnit.SECONDS));
            }

            // 100 events × 10 vertical each = 1000.
            assertEquals(
                    1000,
                    store.getTotalVertical("3", "1", "1234")
            );

        } finally {
            pool.shutdownNow();
        }
    }


    @Test
    void shouldDeduplicateConcurrentDuplicateEvents()
            throws Exception {

        Path dbPath = tempDir.resolve("concurrent-duplicates.db");

        ExecutorService pool = Executors.newFixedThreadPool(10);

        try (SqliteLeaderboardStore store =
                     new SqliteLeaderboardStore(dbPath.toString())) {

            CountDownLatch startLatch = new CountDownLatch(1);
            List<Future<Boolean>> futures =
                    new java.util.ArrayList<>();

            for (int i = 0; i < 20; i++) {
                futures.add(pool.submit(() -> {
                    startLatch.await();

                    return store.processEvent(
                            createEvent("same-event", 213)
                    );
                }));
            }

            startLatch.countDown();

            int insertedCount = 0;

            for (Future<Boolean> future : futures) {
                if (future.get(30, TimeUnit.SECONDS)) {
                    insertedCount++;
                }
            }

            // Only one worker inserts the event.
            assertEquals(1, insertedCount);

            // Vertical is counted exactly once.
            assertEquals(
                    213,
                    store.getTotalVertical("3", "1", "1234")
            );

        } finally {
            pool.shutdownNow();
        }
    }


}
