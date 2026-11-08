
package model;

public class SwipeEvent {

    private String eventId;
    private String resortID;
    private String dayID;
    private String skierID;
    private String liftID;

    private int vertical;
    private int time;

    private String timestamp;
    private String serverId;

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
