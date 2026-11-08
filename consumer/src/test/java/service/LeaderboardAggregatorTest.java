
package service;

import com.google.gson.Gson;
import model.SwipeEvent;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class LeaderboardAggregatorTest {

    private final Gson gson = new Gson();

    private SwipeEvent event(
            String eventId, String skierId, int vertical) {

        String json = String.format(
                "{\"eventId\":\"%s\","
                        + "\"resortID\":\"3\","
                        + "\"dayID\":\"1\","
                        + "\"skierID\":\"%s\","
                        + "\"liftID\":\"17\","
                        + "\"vertical\":%d,"
                        + "\"time\":217}",
                eventId, skierId, vertical
        );

        return gson.fromJson(json, SwipeEvent.class);
    }

    @Test
    void testAggregationAndDeduplication() {

        LeaderboardAggregator aggregator =
                new LeaderboardAggregator();

        assertTrue(aggregator.processEvent(
                event("A", "1234", 213)
        ));

        assertTrue(aggregator.processEvent(
                event("B", "1234", 167)
        ));

        assertTrue(aggregator.processEvent(
                event("C", "5678", 269)
        ));

        // Duplicate event B should not count again.
        assertFalse(aggregator.processEvent(
                event("B", "1234", 167)
        ));

        assertEquals(
                380,
                aggregator.getTotalVertical("3", "1", "1234")
        );

        assertEquals(
                269,
                aggregator.getTotalVertical("3", "1", "5678")
        );

        assertEquals(
                "1234",
                aggregator.getTopN("3", "1", 2)
                        .get(0).getKey()
        );
    }
}
