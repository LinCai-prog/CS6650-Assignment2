package service;

import org.junit.jupiter.api.Test;
import java.time.Instant;
import static org.junit.jupiter.api.Assertions.*;

class AggregationMetricsTest {
    @Test void reportsConservativePercentilesAndSubtractableHistogramBuckets() {
        var metrics=new AggregationMetrics();Instant started=Instant.parse("2026-11-10T00:00:00Z");
        for(int i=0;i<95;i++)metrics.recordProcessed(started.minusMillis(10).toString(),false,started);
        for(int i=0;i<4;i++)metrics.recordProcessed(started.minusMillis(10001).toString(),false,started);
        metrics.recordProcessed(started.minusMillis(610001).toString(),false,started);
        String output=metrics.summary();
        assertTrue(output.contains("lag p95 upper ms=10"));
        assertTrue(output.contains("lag p99 upper ms=11000"));
        assertTrue(output.contains("lag histogram=[10:95;10001:4;10601:1]"));
        assertTrue(output.contains("lag >=100ms=5"));
    }
}
