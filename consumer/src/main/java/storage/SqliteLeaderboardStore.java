
package storage;

import model.SwipeEvent;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;


public class SqliteLeaderboardStore implements LeaderboardStore {

    private final Connection connection;

    public SqliteLeaderboardStore(String databasePath)
            throws SQLException {

        String url = "jdbc:sqlite:" + databasePath;
        this.connection = DriverManager.getConnection(url);

        try (Statement statement = connection.createStatement()) {
            statement.execute("PRAGMA busy_timeout = 5000");
            statement.execute("PRAGMA journal_mode = WAL");
            statement.execute("PRAGMA synchronous = FULL");
        }

        initializeTables();
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
        }
    }



    @Override
    public synchronized boolean processEvent(SwipeEvent event)
            throws SQLException {

        if (event == null) {
            throw new IllegalArgumentException("event cannot be null");
        }

        String insertEventSql =
                "INSERT INTO processed_events " +
                        "(event_id, resort_id, day_id, skier_id) " +
                        "VALUES (?, ?, ?, ?) " +
                        "ON CONFLICT(event_id) DO NOTHING";

        String updateTotalSql =
                "INSERT INTO skier_totals " +
                        "(resort_id, day_id, skier_id, total_vertical) " +
                        "VALUES (?, ?, ?, ?) " +
                        "ON CONFLICT(resort_id, day_id, skier_id) " +
                        "DO UPDATE SET total_vertical = " +
                        "total_vertical + excluded.total_vertical";

        connection.setAutoCommit(false);

        try {
            // Step 1: Try to record the event ID.
            int inserted;

            try (java.sql.PreparedStatement ps =
                         connection.prepareStatement(insertEventSql)) {

                ps.setString(1, event.getEventId());
                ps.setString(2, event.getResortID());
                ps.setString(3, event.getDayID());
                ps.setString(4, event.getSkierID());

                inserted = ps.executeUpdate();
            }

            // Step 2: Duplicate event; do not update totals.
            if (inserted == 0) {
                connection.commit();
                return false;
            }

            // Step 3: New event; update total vertical.
            try (java.sql.PreparedStatement ps =
                         connection.prepareStatement(updateTotalSql)) {

                ps.setString(1, event.getResortID());
                ps.setString(2, event.getDayID());
                ps.setString(3, event.getSkierID());
                ps.setLong(4, event.getVertical());

                ps.executeUpdate();
            }

            // Step 4: Both operations succeeded.
            connection.commit();
            return true;

        } catch (SQLException | RuntimeException e) {

            try {
                connection.rollback();
            } catch (SQLException rollbackError) {
                e.addSuppressed(rollbackError);
            }

            throw e;

        } finally {
            connection.setAutoCommit(true);
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
        connection.close();
    }
}
