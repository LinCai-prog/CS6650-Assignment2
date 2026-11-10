
package storage;

import model.SwipeEvent;
import model.LeaderboardUpdate;
import com.google.gson.JsonParser;
import java.time.Instant;
import java.util.ArrayList;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;


public class SqliteLeaderboardStore implements LeaderboardStore {

    private final Connection connection;
    private final WalCheckpointer checkpointer;
    private final service.AggregationMetrics metrics = new service.AggregationMetrics();

    public String getMetricsSummary() { return metrics.summary() + checkpointer.summary(); }

    public SqliteLeaderboardStore(String databasePath)
            throws SQLException {

        this(databasePath, config.ConsumerConfig.SQLITE_CACHE_KIB,
                config.ConsumerConfig.SQLITE_CHECKPOINT_PAGES, config.ConsumerConfig.SQLITE_CHECKPOINT_MS);
    }

    SqliteLeaderboardStore(String databasePath, int cacheKiB, int checkpointPages, int checkpointMs) throws SQLException {
        if (cacheKiB < 1 || checkpointPages < 1 || checkpointMs < 1) throw new IllegalArgumentException("positive SQLite tuning required");
        String url = "jdbc:sqlite:" + databasePath;
        this.connection = DriverManager.getConnection(url);

        try (Statement statement = connection.createStatement()) {
            statement.execute("PRAGMA busy_timeout = 5000");
            statement.execute("PRAGMA journal_mode = WAL");
            statement.execute("PRAGMA synchronous = FULL");
            statement.execute("PRAGMA cache_size = -" + cacheKiB);
            // Background checkpoints normally keep WAL short; retain an automatic fallback.
            statement.execute("PRAGMA wal_autocheckpoint = " + checkpointPages);
        }

        try {
            initializeTables();
            checkpointer = new WalCheckpointer(url, checkpointMs);
        } catch (SQLException failure) {
            connection.close(); throw failure;
        }
    }


    private void initializeTables() throws SQLException {

        // Table 1: Store processed Event IDs
        String createProcessedEvents =
                "CREATE TABLE IF NOT EXISTS processed_events (" +
                        "event_id TEXT PRIMARY KEY, " +
                        "resort_id TEXT NOT NULL, " +
                        "day_id TEXT NOT NULL, " +
                        "skier_id TEXT NOT NULL" +
                        ")";

        // Table 2: Store accumulated vertical
        String createSkierTotals =
                "CREATE TABLE IF NOT EXISTS skier_totals (" +
                        "resort_id TEXT NOT NULL, " +
                        "day_id TEXT NOT NULL, " +
                        "skier_id TEXT NOT NULL, " +
                        "total_vertical INTEGER NOT NULL DEFAULT 0, " +
                        "PRIMARY KEY (resort_id, day_id, skier_id)" +
                        ")";

        // Index: Optimize Top-N leaderboard queries
        String createLeaderboardIndex =
                "CREATE INDEX IF NOT EXISTS idx_skier_totals_rank " +
                        "ON skier_totals " +
                        "(resort_id, day_id, total_vertical DESC, skier_id ASC)";

        try (Statement statement = connection.createStatement()) {

            // 1. Create processed_events table
            statement.execute(createProcessedEvents);

            // 2. Create skier_totals table
            statement.execute(createSkierTotals);

            // 3. Create leaderboard ranking index
            statement.execute(createLeaderboardIndex);
            statement.execute("CREATE TABLE IF NOT EXISTS leaderboard_versions ("
                    + "resort_id TEXT NOT NULL, day_id TEXT NOT NULL, version INTEGER NOT NULL, "
                    + "PRIMARY KEY (resort_id, day_id))");
            statement.execute("CREATE TABLE IF NOT EXISTS leaderboard_outbox ("
                    + "resort_id TEXT NOT NULL, day_id TEXT NOT NULL, version INTEGER NOT NULL, "
                    + "payload TEXT NOT NULL, PRIMARY KEY (resort_id, day_id, version))");
        }
    }



    @Override
    public boolean processEvent(SwipeEvent event) throws SQLException {
        return processBatch(java.util.Collections.singletonList(event)).get(0);
    }

    @Override
    public synchronized List<Boolean> processBatch(List<SwipeEvent> events) throws SQLException {
        if (events == null || events.stream().anyMatch(java.util.Objects::isNull)) {
            throw new IllegalArgumentException("events cannot contain null");
        }
        if (events.isEmpty()) return java.util.Collections.emptyList();
        List<Boolean> inserted = new ArrayList<>();
        List<Boolean> changed = new ArrayList<>();
        List<Instant> started = new ArrayList<>();
        List<Long> startedNanos = new ArrayList<>();
        long batchStarted = System.nanoTime();
        connection.setAutoCommit(false);
        try (var record = connection.prepareStatement(
                "INSERT INTO processed_events (event_id, resort_id, day_id, skier_id) "
                        + "VALUES (?, ?, ?, ?) ON CONFLICT(event_id) DO NOTHING");
             var total = connection.prepareStatement(
                "INSERT INTO skier_totals (resort_id, day_id, skier_id, total_vertical) "
                        + "VALUES (?, ?, ?, ?) ON CONFLICT(resort_id, day_id, skier_id) "
                        + "DO UPDATE SET total_vertical = total_vertical + excluded.total_vertical")) {
            for (SwipeEvent event : events) {
                started.add(Instant.now());
                startedNanos.add(System.nanoTime());
                record.setString(1, event.getEventId());
                record.setString(2, event.getResortID());
                record.setString(3, event.getDayID());
                record.setString(4, event.getSkierID());
                boolean fresh = record.executeUpdate() == 1;
                inserted.add(fresh);
                if (!fresh) {
                    changed.add(false);
                    continue;
                }
                var before = getTopN(event.getResortID(), event.getDayID(), LeaderboardUpdate.TOP_N);
                total.setString(1, event.getResortID());
                total.setString(2, event.getDayID());
                total.setString(3, event.getSkierID());
                total.setLong(4, event.getVertical());
                total.executeUpdate();
                var after = getTopN(event.getResortID(), event.getDayID(), LeaderboardUpdate.TOP_N);
                boolean different = !before.equals(after);
                changed.add(different);
                // Capture every intermediate change, even within the same transaction.
                if (different) enqueueLeaderboardChange(event, after);
            }
            connection.commit();
        } catch (SQLException | RuntimeException e) {
            try { connection.rollback(); }
            catch (SQLException rollbackError) { e.addSuppressed(rollbackError); }
            throw e;
        } finally {
            connection.setAutoCommit(true);
        }
        // Failed transactions must not advance metrics or be acknowledged.
        metrics.recordBatch(events.size(), System.nanoTime() - batchStarted);
        for (int i = 0; i < events.size(); i++) {
            if (inserted.get(i)) {
                metrics.recordProcessed(events.get(i).getTimestamp(), changed.get(i), started.get(i));
                metrics.recordStages(events.get(i), startedNanos.get(i));
            }
            else metrics.recordDuplicate();
        }
        return inserted;
    }

    private void enqueueLeaderboardChange(SwipeEvent event,
            List<Map.Entry<String, Long>> standings) throws SQLException {
        String resort = event.getResortID();
        String day = event.getDayID();
        try (var ps = connection.prepareStatement(
                "INSERT INTO leaderboard_versions (resort_id, day_id, version) VALUES (?, ?, 1) "
                        + "ON CONFLICT(resort_id, day_id) DO UPDATE SET version = version + 1")) {
            ps.setString(1, resort);
            ps.setString(2, day);
            ps.executeUpdate();
        }
        LeaderboardUpdate update = new LeaderboardUpdate(event.getEventId(), resort, day,
                getVersion(resort, day), event.getTimestamp(), Instant.now().toString(), standings);
        try (var ps = connection.prepareStatement(
                "INSERT INTO leaderboard_outbox (resort_id, day_id, version, payload) VALUES (?, ?, ?, ?)")) {
            ps.setString(1, resort);
            ps.setString(2, day);
            ps.setLong(3, update.getVersion());
            ps.setString(4, update.toJson().toString());
            ps.executeUpdate();
        }
    }

    private long getVersion(String resort, String day) throws SQLException {
        try (var ps = connection.prepareStatement(
                "SELECT version FROM leaderboard_versions WHERE resort_id = ? AND day_id = ?")) {
            ps.setString(1, resort);
            ps.setString(2, day);
            try (var rs = ps.executeQuery()) {
                return rs.next() ? rs.getLong(1) : 0L;
            }
        }
    }

    @Override
    public synchronized LeaderboardUpdate getSnapshot(String resort, String day) throws SQLException {
        connection.setAutoCommit(false);
        try {
            long version = getVersion(resort, day);
            var standings = getTopN(resort, day, LeaderboardUpdate.TOP_N);
            connection.commit();
            return new LeaderboardUpdate("snapshot", resort, day, version, null, null, standings);
        } catch (SQLException | RuntimeException e) {
            connection.rollback();
            throw e;
        } finally {
            connection.setAutoCommit(true);
        }
    }

    @Override
    public synchronized List<LeaderboardUpdate> getPendingUpdates(int limit) throws SQLException {
        if (limit <= 0) throw new IllegalArgumentException("limit must be positive");
        List<LeaderboardUpdate> updates = new ArrayList<>();
        try (var ps = connection.prepareStatement(
                "SELECT payload FROM leaderboard_outbox ORDER BY rowid LIMIT ?")) {
            ps.setInt(1, limit);
            try (var rs = ps.executeQuery()) {
                while (rs.next()) {
                    updates.add(LeaderboardUpdate.fromJson(
                            JsonParser.parseString(rs.getString(1)).getAsJsonObject()));
                }
            }
        }
        return updates;
    }

    @Override
    public synchronized void markUpdatePublished(LeaderboardUpdate update) throws SQLException {
        try (var ps = connection.prepareStatement(
                "DELETE FROM leaderboard_outbox WHERE resort_id = ? AND day_id = ? AND version = ?")) {
            ps.setString(1, update.getResortId());
            ps.setString(2, update.getDayId());
            ps.setLong(3, update.getVersion());
            ps.executeUpdate();
        }
    }



    @Override
    public synchronized long getTotalVertical(
            String resortId,
            String dayId,
            String skierId) throws SQLException {

        String sql =
                "SELECT total_vertical " +
                        "FROM skier_totals " +
                        "WHERE resort_id = ? " +
                        "AND day_id = ? " +
                        "AND skier_id = ?";

        try (java.sql.PreparedStatement ps =
                     connection.prepareStatement(sql)) {

            ps.setString(1, resortId);
            ps.setString(2, dayId);
            ps.setString(3, skierId);

            try (java.sql.ResultSet rs = ps.executeQuery()) {

                if (rs.next()) {
                    return rs.getLong("total_vertical");
                }

                return 0L;
            }
        }
    }



    @Override
    public synchronized List<Map.Entry<String, Long>> getTopN(
            String resortId,
            String dayId,
            int limit) throws SQLException {

        if (limit <= 0) {
            throw new IllegalArgumentException(
                    "limit must be positive");
        }

        String sql =
                "SELECT skier_id, total_vertical " +
                        "FROM skier_totals " +
                        "WHERE resort_id = ? AND day_id = ? " +
                        "ORDER BY total_vertical DESC, skier_id ASC " +
                        "LIMIT ?";

        List<Map.Entry<String, Long>> results =
                new java.util.ArrayList<>();

        try (java.sql.PreparedStatement ps =
                     connection.prepareStatement(sql)) {

            ps.setString(1, resortId);
            ps.setString(2, dayId);
            ps.setInt(3, limit);

            try (java.sql.ResultSet rs = ps.executeQuery()) {

                while (rs.next()) {
                    String skierId = rs.getString("skier_id");
                    long total = rs.getLong("total_vertical");

                    results.add(
                            new java.util.AbstractMap.SimpleEntry<>(
                                    skierId, total));
                }
            }
        }

        return results;
    }


    @Override
    public synchronized void close() throws SQLException {
        checkpointer.close();
        connection.close();
    }
}
