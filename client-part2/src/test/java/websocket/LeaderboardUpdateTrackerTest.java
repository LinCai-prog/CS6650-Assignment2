package websocket;

/** Run with java -ea; the load client intentionally has no external dependencies. */
public final class LeaderboardUpdateTrackerTest {
    private static String message(long version, long total) {
        return "{\"version\":" + version + ",\"top10\":[{\"rank\":1,\"skierId\":\"1234\",\"totalVertical\":" + total + "}]}";
    }

    public static void main(String[] args) {
        var tracker = new LeaderboardUpdateTracker();
        assert tracker.accept(message(0, 100), true) == LeaderboardUpdateTracker.Result.CHANGED;
        assert tracker.accept(message(1, 100), false) == LeaderboardUpdateTracker.Result.UNCHANGED;
        assert tracker.accept(message(2, 200), false) == LeaderboardUpdateTracker.Result.CHANGED;
        assert tracker.accept(message(2, 200), false) == LeaderboardUpdateTracker.Result.STALE;
        assert tracker.accept(message(1, 100), false) == LeaderboardUpdateTracker.Result.STALE;
        invalid(tracker, "{\"version\":3}");
        invalid(tracker, "{\"version\":3,\"top10\":[{\"rank\":2,\"skierId\":\"1234\",\"totalVertical\":200}]}");
        invalid(tracker, "{\"version\":3,\"top10\":[{\"rank\":1,\"skierId\":\"1234\",\"totalVertical\":200},"
                + "{\"rank\":2,\"skierId\":\"1234\",\"totalVertical\":100}]}");
        invalid(tracker, "{\"version\":3,\"top10\":[{\"rank\":1,\"skierId\":\"1234\",\"totalVertical\":100},"
                + "{\"rank\":2,\"skierId\":\"5678\",\"totalVertical\":200}]}");
        invalid(tracker, message(3, 0));
        invalid(tracker, message(3, 200).replace("]", ",]"));
        assert tracker.accept(message(3, 300), false) == LeaderboardUpdateTracker.Result.CHANGED;
        tracker.reset();
        assert tracker.accept(message(1, 100), false) == LeaderboardUpdateTracker.Result.CHANGED;
        System.out.println("LeaderboardUpdateTracker checks passed");
    }

    private static void invalid(LeaderboardUpdateTracker tracker, String message) {
        try {
            tracker.accept(message, false);
            throw new AssertionError("Accepted invalid leaderboard: " + message);
        } catch (IllegalArgumentException expected) {
            // Invalid messages must not advance the last accepted version.
        }
    }
}
