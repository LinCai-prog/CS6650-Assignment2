package storage;

import java.sql.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/** Checkpoint on a separate connection; the writer retains FULL commit durability. */
final class WalCheckpointer implements AutoCloseable {
    private final Connection connection;
    private final ScheduledExecutorService executor;
    private final AtomicLong runs = new AtomicLong(), errors = new AtomicLong(), maxNanos = new AtomicLong();
    private volatile int walFrames, checkpointedFrames;

    WalCheckpointer(String url, int intervalMs) throws SQLException {
        connection = DriverManager.getConnection(url);
        try (var statement = connection.createStatement()) {
            statement.execute("PRAGMA busy_timeout = 0");
            statement.execute("PRAGMA synchronous = FULL");
            statement.execute("PRAGMA wal_autocheckpoint = 0");
        } catch (SQLException failure) {
            connection.close(); throw failure;
        }
        executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "leaderboard-wal-checkpoint"); thread.setDaemon(true); return thread;
        });
        executor.scheduleWithFixedDelay(this::checkpoint, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    private void checkpoint() {
        long started = System.nanoTime();
        try (var statement = connection.createStatement(); var result = statement.executeQuery("PRAGMA wal_checkpoint(PASSIVE)")) {
            if (result.next()) { walFrames = result.getInt(2); checkpointedFrames = result.getInt(3); }
            runs.incrementAndGet();
        } catch (SQLException failure) {
            long count = errors.incrementAndGet();
            if (count == 1 || count % 10 == 0) System.err.println("Background checkpoint error count=" + count + ": " + failure.getClass().getSimpleName());
        } finally { maxNanos.accumulateAndGet(System.nanoTime() - started, Math::max); }
    }

    String summary() {
        return ", checkpoints=" + runs.get() + ", checkpoint errors=" + errors.get()
                + ", wal frames=" + walFrames + ", checkpointed frames=" + checkpointedFrames
                + ", max checkpoint ms=" + maxNanos.get() / 1_000_000.0;
    }

    @Override public void close() throws SQLException {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) throw new SQLException("Checkpoint worker did not stop");
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt(); throw new SQLException("Interrupted stopping checkpoint worker", interrupted);
        }
        connection.close();
    }
}
