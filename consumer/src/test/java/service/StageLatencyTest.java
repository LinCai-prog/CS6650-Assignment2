package service;

import com.google.gson.Gson;
import model.SwipeEvent;
import org.junit.jupiter.api.Test;
import java.time.Instant;
import static org.junit.jupiter.api.Assertions.*;

class StageLatencyTest {
    private SwipeEvent event(String timestamp) {
        return new Gson().fromJson("{\"timestamp\":\"" + timestamp + "\"}", SwipeEvent.class);
    }

    @Test void separatesTransportFromLocalQueueWaitWithSubMillisecondPrecision() {
        var metrics = new AggregationMetrics();
        var event = event("2026-11-10T00:00:00Z");
        event.markReceived(Instant.parse("2026-11-10T00:00:00.012500Z"), 500L);
        metrics.recordStages(event, 200_000_500L);
        String out = metrics.summary();
        assertTrue(out.contains("mean source-to-receive ms=12.5"), out);
        assertTrue(out.contains("max receive-to-sql ms=200.0"), out);
        assertTrue(out.contains("receive-to-sql >=100ms=1"), out);
        assertTrue(out.contains("source-to-receive samples=1"), out);
    }

    @Test void flagsNegativeCrossHostTimeWithoutDiscardingLocalElapsedTime() {
        var metrics = new AggregationMetrics();
        var event = event("2026-11-10T00:00:01Z");
        event.markReceived(Instant.parse("2026-11-10T00:00:00Z"), 0L);
        metrics.recordStages(event, 6_000_000L);
        String out = metrics.summary();
        assertTrue(out.contains("source-to-receive samples=0"), out);
        assertTrue(out.contains("source-to-receive invalid=1"), out);
        assertTrue(out.contains("mean receive-to-sql ms=6.0"), out);
    }

    @Test void reportsMissingOrInvalidTimestampsAndKeepsReceiptOffTheWire() {
        var metrics = new AggregationMetrics();
        var event = event("invalid");
        metrics.recordStages(event, 1L);
        assertTrue(metrics.summary().contains("stage timings missing receipt=1"));
        event.markReceived(Instant.now(), 0L);
        metrics.recordStages(event, 99_999_999L);
        assertTrue(metrics.summary().contains("source-to-receive invalid=1"));
        assertTrue(metrics.summary().contains("receive-to-sql >=100ms=0"));
        String json = new Gson().toJson(event);
        assertFalse(json.contains("receivedAt"));
        assertFalse(json.contains("receivedNanos"));
    }
}
