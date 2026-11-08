
package websocket;

import java.util.Objects;

public final class SubscriptionKey {

    private final String resortId;
    private final String dayId;

    public SubscriptionKey(String resortId, String dayId) {
        if (resortId == null || resortId.trim().isEmpty()
                || dayId == null || dayId.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "Resort ID and Day ID must not be empty"
            );
        }

        this.resortId = resortId;
        this.dayId = dayId;
    }

    public String getResortId() {
        return resortId;
    }

    public String getDayId() {
        return dayId;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (!(obj instanceof SubscriptionKey)) {
            return false;
        }

        SubscriptionKey other = (SubscriptionKey) obj;

        return resortId.equals(other.resortId)
                && dayId.equals(other.dayId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resortId, dayId);
    }

    @Override
    public String toString() {
        return "resort:" + resortId + ":day:" + dayId;
    }
}
