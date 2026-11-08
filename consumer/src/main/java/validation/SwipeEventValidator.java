
package validation;

import model.SwipeEvent;

public final class SwipeEventValidator {

    private SwipeEventValidator() {
        // Utility class.
    }

    public static void validate(
            SwipeEvent event,
            int expectedResortId) {

        if (event == null) {
            throw new InvalidSwipeEventException(
                    "SwipeEvent cannot be null"
            );
        }

        if (isBlank(event.getEventId())) {
            throw new InvalidSwipeEventException(
                    "Missing eventId"
            );
        }

        if (isBlank(event.getResortID())
                || isBlank(event.getDayID())
                || isBlank(event.getSkierID())) {
            throw new InvalidSwipeEventException(
                    "Missing resortID, dayID or skierID"
            );
        }

        if (!String.valueOf(expectedResortId)
                .equals(event.getResortID())) {
            throw new InvalidSwipeEventException(
                    "Resort ID does not match queue"
            );
        }

        if (isBlank(event.getLiftID())) {
            throw new InvalidSwipeEventException(
                    "Missing liftID"
            );
        }

        if (event.getVertical() <= 0) {
            throw new InvalidSwipeEventException(
                    "Vertical must be positive"
            );
        }

        if (event.getTime() < 0) {
            throw new InvalidSwipeEventException(
                    "Time cannot be negative"
            );
        }

        if (isBlank(event.getTimestamp())
                || isBlank(event.getServerId())) {
            throw new InvalidSwipeEventException(
                    "Missing timestamp or serverId"
            );
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
