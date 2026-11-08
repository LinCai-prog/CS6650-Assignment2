
package model;

import java.time.Instant;
import java.util.UUID;

public class SwipeEventMessage {

    private final String eventId;
    private final String resortID;
    private final String dayID;
    private final String skierID;
    private final String liftID;

    private final int vertical;
    private final int time;

    private final String timestamp;
    private final String serverId;

    // Backward-compatible constructor.
    // Used when the caller does not provide an eventId.
    public SwipeEventMessage(
            String resortID,
            String dayID,
            String skierID,
            LiftRide liftRide,
            int vertical,
            String serverId) {

        this(
                null,
                resortID,
                dayID,
                skierID,
                liftRide,
                vertical,
                serverId
        );
    }

    // Constructor with a stable client-provided eventId.
    public SwipeEventMessage(
            String eventId,
            String resortID,
            String dayID,
            String skierID,
            LiftRide liftRide,
            int vertical,
            String serverId) {

        // Generate a new UUID only when none is supplied.
        this.eventId =
                eventId == null || eventId.trim().isEmpty()
                        ? UUID.randomUUID().toString()
                        : eventId;

        this.resortID = resortID;
        this.dayID = dayID;
        this.skierID = skierID;

        this.liftID =
                String.valueOf(liftRide.getLiftID());

        this.vertical = vertical;
        this.time = liftRide.getTime();

        this.timestamp = Instant.now().toString();
        this.serverId = serverId;
    }

    public String getEventId() {
        return eventId;
    }

    public String getResortID() {
        return resortID;
    }

    public String getDayID() {
        return dayID;
    }

    public String getSkierID() {
        return skierID;
    }

    public String getLiftID() {
        return liftID;
    }

    public int getVertical() {
        return vertical;
    }

    public int getTime() {
        return time;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public String getServerId() {
        return serverId;
    }
}
