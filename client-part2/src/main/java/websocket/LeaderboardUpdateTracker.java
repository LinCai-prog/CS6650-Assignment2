package websocket;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/** Validates the leaderboard schema emitted by the server without extra dependencies. */
final class LeaderboardUpdateTracker {
    enum Result { CHANGED, UNCHANGED, STALE }
    private static final Pattern VERSION = Pattern.compile("\"version\"\\s*:\\s*(\\d+)");
    private static final Pattern TOP = Pattern.compile("\"top10\"\\s*:\\s*\\[([^\\]]*)\\]");
    private static final Pattern ENTRY = Pattern.compile(
            "\\{\\s*\"rank\"\\s*:\\s*(\\d+)\\s*,\\s*\"skierId\"\\s*:\\s*\"([^\"\\\\]+)\""
                    + "\\s*,\\s*\"totalVertical\"\\s*:\\s*(\\d+)\\s*\\}");
    private long lastVersion = -1;
    private List<Map.Entry<String, Long>> previous;

    synchronized void reset() {
        lastVersion = -1;
        previous = null;
    }

    synchronized Result accept(String message, boolean snapshot) {
        var versionMatch = VERSION.matcher(message);
        var topMatch = TOP.matcher(message);
        if (!versionMatch.find() || !topMatch.find()) {
            throw new IllegalArgumentException("Missing leaderboard version or top10");
        }
        long version = Long.parseLong(versionMatch.group(1));
        if (!snapshot && version == 0) throw new IllegalArgumentException("Invalid update version");
        var entries = new ArrayList<Map.Entry<String, Long>>();
        var skiers = new HashSet<String>();
        String array = topMatch.group(1);
        var matcher = ENTRY.matcher(array);
        int end = 0;
        while (matcher.find()) {
            String separator = array.substring(end, matcher.start()).trim();
            if (!separator.equals(entries.isEmpty() ? "" : ",")
                    || Integer.parseInt(matcher.group(1)) != entries.size() + 1
                    || entries.size() >= 10 || !skiers.add(matcher.group(2))) {
                throw new IllegalArgumentException("Invalid leaderboard entries");
            }
            long total = Long.parseLong(matcher.group(3));
            if (total <= 0) throw new IllegalArgumentException("Invalid vertical total");
            if (!entries.isEmpty()) {
                var previousEntry = entries.get(entries.size() - 1);
                if (previousEntry.getValue() < total
                        || (previousEntry.getValue() == total
                        && previousEntry.getKey().compareTo(matcher.group(2)) > 0)) {
                    throw new IllegalArgumentException("Standings are out of order");
                }
            }
            entries.add(new AbstractMap.SimpleImmutableEntry<>(matcher.group(2), total));
            end = matcher.end();
        }
        if (!array.substring(end).trim().isEmpty()) throw new IllegalArgumentException("Invalid top10 content");
        if (version <= lastVersion) return Result.STALE;
        boolean changed = !entries.equals(previous);
        lastVersion = version;
        previous = entries;
        return changed ? Result.CHANGED : Result.UNCHANGED;
    }
}
