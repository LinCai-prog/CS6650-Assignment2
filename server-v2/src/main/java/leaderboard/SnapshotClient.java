package leaderboard;
import model.LeaderboardUpdate;
import java.util.concurrent.CompletableFuture;
@FunctionalInterface
public interface SnapshotClient {
    CompletableFuture<LeaderboardUpdate> fetch(String resortId, String dayId);
}
