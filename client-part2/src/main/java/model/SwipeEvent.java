
package model;

import java.util.UUID;

public class SwipeEvent {

    private final String eventId;

    private final int skierID;
    private final int resortID;
    private final int liftID;
    private final int seasonID;
    private final int dayID;
    private final int time;

    public SwipeEvent(
            int skierID,
            int resortID,
            int liftID,
            int seasonID,
            int dayID,
            int time) {

        // Generate once per logical swipe event.
        // All HTTP retries will reuse this eventId.
        this.eventId = UUID.randomUUID().toString();

        this.skierID = skierID;
        this.resortID = resortID;
        this.liftID = liftID;
        this.seasonID = seasonID;
        this.dayID = dayID;
        this.time = time;
    }

    public String getEventId() {
        return eventId;
    }

    public int getSkierID() {
        return skierID;
    }

    public int getResortID() {
        return resortID;
    }

    public int getLiftID() {
        return liftID;
    }

    public int getSeasonID() {
        return seasonID;
    }

    public int getDayID() {
        return dayID;
    }

    public int getTime() {
        return time;
    }
}
