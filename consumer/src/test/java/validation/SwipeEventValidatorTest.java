
package validation;

import com.google.gson.Gson;
import model.SwipeEvent;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SwipeEventValidatorTest {

    private final Gson gson = new Gson();

    @Test
    void validEventShouldPass() {
        SwipeEvent event = fromJson(213, "3");

        assertDoesNotThrow(
                () -> SwipeEventValidator.validate(event, 3)
        );
    }

    @Test
    void negativeVerticalShouldFail() {
        SwipeEvent event = fromJson(-100, "3");

        assertThrows(
                InvalidSwipeEventException.class,
                () -> SwipeEventValidator.validate(event, 3)
        );
    }

    @Test
    void mismatchedResortShouldFail() {
        SwipeEvent event = fromJson(213, "5");

        assertThrows(
                InvalidSwipeEventException.class,
                () -> SwipeEventValidator.validate(event, 3)
        );
    }

    @Test
    void missingEventIdShouldFail() {
        SwipeEvent event = gson.fromJson(
                "{\"resortID\":\"3\","
                        + "\"dayID\":\"1\","
                        + "\"skierID\":\"1234\","
                        + "\"liftID\":\"17\","
                        + "\"vertical\":213,"
                        + "\"time\":217,"
                        + "\"timestamp\":\"test-time\","
                        + "\"serverId\":\"test-server\"}",
                SwipeEvent.class
        );

        assertThrows(
                InvalidSwipeEventException.class,
                () -> SwipeEventValidator.validate(event, 3)
        );
    }

    private SwipeEvent fromJson(
            int vertical,
            String resortId) {

        String json = "{"
                + "\"eventId\":\"test-123\","
                + "\"resortID\":\"" + resortId + "\","
                + "\"dayID\":\"1\","
                + "\"skierID\":\"1234\","
                + "\"liftID\":\"17\","
                + "\"vertical\":" + vertical + ","
                + "\"time\":217,"
                + "\"timestamp\":\"test-time\","
                + "\"serverId\":\"test-server\""
                + "}";

        return gson.fromJson(json, SwipeEvent.class);
    }
}
