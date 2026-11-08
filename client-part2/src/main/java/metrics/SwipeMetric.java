package metrics;

import java.time.Instant;

public class SwipeMetric {

    private final Instant sendTimestamp;
    private final Instant responseTimestamp;
    private final double latencyMs;
    private final int statusCode;
    private final int resortID;

    public SwipeMetric(
            Instant sendTimestamp,
            Instant responseTimestamp,
            double latencyMs,
            int statusCode,
            int resortID) {

        this.sendTimestamp = sendTimestamp;
        this.responseTimestamp = responseTimestamp;
        this.latencyMs = latencyMs;
        this.statusCode = statusCode;
        this.resortID = resortID;
    }

    public Instant getSendTimestamp() {
        return sendTimestamp;
    }

    public Instant getResponseTimestamp() {
        return responseTimestamp;
    }

    public double getLatencyMs() {
        return latencyMs;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public int getResortID() {
        return resortID;
    }
}