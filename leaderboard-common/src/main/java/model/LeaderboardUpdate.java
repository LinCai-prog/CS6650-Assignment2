package model;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** An immutable, versioned leaderboard captured by the aggregation transaction. */
public final class LeaderboardUpdate {
    public static final int TOP_N = 10;
    private final String eventId;
    private final String resortId;
    private final String dayId;
    private final long version;
    private final String sourceTimestamp;
    private final String aggregatedTimestamp;
    private final List<Map.Entry<String, Long>> standings;

    public LeaderboardUpdate(String eventId, String resortId, String dayId,
                             long version, String sourceTimestamp,
                             String aggregatedTimestamp,
                             List<Map.Entry<String, Long>> standings) {
        if (eventId == null || eventId.isBlank() || resortId == null
                || !resortId.matches("\\d+") || dayId == null
                || !dayId.matches("\\d+") || version < 0
                || standings == null || standings.size() > TOP_N) {
            throw new IllegalArgumentException("Invalid leaderboard update");
        }
        List<Map.Entry<String, Long>> copy = new ArrayList<>();
        Set<String> skiers = new HashSet<>();
        Map.Entry<String, Long> previous = null;
        for (Map.Entry<String, Long> entry : standings) {
            if (entry.getKey() == null || entry.getKey().isBlank()
                    || entry.getValue() == null || entry.getValue() <= 0
                    || !skiers.add(entry.getKey())
                    || (previous != null && (previous.getValue() < entry.getValue()
                    || (previous.getValue().equals(entry.getValue())
                    && previous.getKey().compareTo(entry.getKey()) > 0)))) {
                throw new IllegalArgumentException("Invalid leaderboard standings");
            }
            previous = new AbstractMap.SimpleImmutableEntry<>(entry);
            copy.add(previous);
        }
        this.eventId = eventId;
        this.resortId = resortId;
        this.dayId = dayId;
        this.version = version;
        this.sourceTimestamp = sourceTimestamp;
        this.aggregatedTimestamp = aggregatedTimestamp;
        this.standings = Collections.unmodifiableList(copy);
    }

    public String getEventId() { return eventId; }
    public String getResortId() { return resortId; }
    public String getDayId() { return dayId; }
    public long getVersion() { return version; }
    public List<Map.Entry<String, Long>> getStandings() { return standings; }

    public JsonObject toJson() {
        JsonObject json = new JsonObject();
        json.addProperty("type", "leaderboard_changed");
        json.addProperty("eventId", eventId);
        json.addProperty("resortId", resortId);
        json.addProperty("dayId", dayId);
        json.addProperty("version", version);
        if (sourceTimestamp != null) json.addProperty("sourceTimestamp", sourceTimestamp);
        if (aggregatedTimestamp != null) json.addProperty("aggregatedTimestamp", aggregatedTimestamp);
        JsonArray entries = new JsonArray();
        int rank = 1;
        for (Map.Entry<String, Long> entry : standings) {
            JsonObject skier = new JsonObject();
            skier.addProperty("rank", rank++);
            skier.addProperty("skierId", entry.getKey());
            skier.addProperty("totalVertical", entry.getValue());
            entries.add(skier);
        }
        json.add("top10", entries);
        return json;
    }

    public static LeaderboardUpdate fromJson(JsonObject json) {
        return parse(json, false);
    }

    public static LeaderboardUpdate fromSnapshot(JsonObject json) {
        return parse(json, true);
    }

    public JsonObject toSnapshotJson() {
        JsonObject json = toJson();
        json.addProperty("type", "leaderboard_snapshot");
        return json;
    }

    private static LeaderboardUpdate parse(JsonObject json, boolean snapshot) {
        if (!(snapshot ? "leaderboard_snapshot" : "leaderboard_changed").equals(json.get("type").getAsString())) {
            throw new IllegalArgumentException("Unexpected notification type");
        }
        List<Map.Entry<String, Long>> entries = new ArrayList<>();
        int rank = 1;
        for (var element : json.getAsJsonArray("top10")) {
            JsonObject skier = element.getAsJsonObject();
            if (skier.get("rank").getAsInt() != rank++) {
                throw new IllegalArgumentException("Invalid leaderboard rank");
            }
            entries.add(new AbstractMap.SimpleImmutableEntry<>(
                    skier.get("skierId").getAsString(),
                    skier.get("totalVertical").getAsLong()));
        }
        long version = json.get("version").getAsLong();
        if (version < 0 || (!snapshot && version == 0)) throw new IllegalArgumentException("Invalid leaderboard version");
        return new LeaderboardUpdate(json.get("eventId").getAsString(),
                json.get("resortId").getAsString(), json.get("dayId").getAsString(),
                version, json.has("sourceTimestamp") ? json.get("sourceTimestamp").getAsString() : null,
                json.has("aggregatedTimestamp") ? json.get("aggregatedTimestamp").getAsString() : null,
                entries);
    }
}
