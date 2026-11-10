package service;

import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.atomic.AtomicLong;

/** Consumer-side measurements; timestamps require synchronized host clocks. */
public final class AggregationMetrics {
    private static final class StageLatency {
        final LongAdder samples = new LongAdder(), totalNanos = new LongAdder();
        final LongAdder over100 = new LongAdder(), invalid = new LongAdder();
        final AtomicLong maxNanos = new AtomicLong();

        void record(long nanos) {
            if (nanos < 0) { invalid.increment(); return; }
            samples.increment(); totalNanos.add(nanos);
            maxNanos.accumulateAndGet(nanos, Math::max);
            if (nanos >= 100_000_000L) over100.increment();
        }

        String summary(String name) {
            long count = samples.sum();
            return ", " + name + " samples=" + count
                    + ", mean " + name + " ms=" + (count == 0 ? "n/a" : totalNanos.sum() / 1_000_000.0 / count)
                    + ", max " + name + " ms=" + maxNanos.get() / 1_000_000.0
                    + ", " + name + " >=100ms=" + over100.sum()
                    + ", " + name + " invalid=" + invalid.sum();
        }
    }
    private final StageLatency sourceToReceive = new StageLatency();
    private final StageLatency receiveToSql = new StageLatency();
    private final LongAdder stageMissingReceipt = new LongAdder();

    /** Called only after commit for a unique event, on the same cohort as lag. */
    public void recordStages(model.SwipeEvent event, long sqlStartedNanos) {
        if (event.getReceivedAt() == null) {
            stageMissingReceipt.increment();
            return;
        }
        // Local elapsed time remains valid even if either host's wall clock moves.
        receiveToSql.record(sqlStartedNanos - event.getReceivedNanos());
        try {
            sourceToReceive.record(Duration.between(Instant.parse(event.getTimestamp()), event.getReceivedAt()).toNanos());
        } catch (DateTimeParseException | NullPointerException | ArithmeticException invalidTimestamp) {
            sourceToReceive.invalid.increment();
        }
    }
    private final LongAdder batches = new LongAdder();
    private final LongAdder batchEvents = new LongAdder();
    private final LongAdder batchNanos = new LongAdder();
    private final java.util.concurrent.atomic.AtomicLongArray lagHistogram = new java.util.concurrent.atomic.AtomicLongArray(10602);
    public void recordBatch(int size, long nanos) {
        batches.increment(); batchEvents.add(size); batchNanos.add(nanos);
    }
    private long percentileUpper(double fraction, long samples) {
        long target = (long) Math.ceil(samples * fraction), seen = 0;
        if (target == 0) return 0;
        for (int i = 0; i < lagHistogram.length(); i++) {
            seen += lagHistogram.get(i);
            if (seen >= target) return i <= 10000 ? i : i == 10601 ? Long.MAX_VALUE : 10000L + (i - 10000L) * 1000;
        }
        return 0;
    }
    private final LongAdder processed = new LongAdder();
    private final LongAdder duplicates = new LongAdder();
    private final LongAdder changes = new LongAdder();
    private final LongAdder lagSamples = new LongAdder();
    private final LongAdder lagTotal = new LongAdder();
    private final LongAdder lagOver100 = new LongAdder();
    private final AtomicLong lagMax = new AtomicLong();

    public void recordDuplicate() { duplicates.increment(); }

    public void recordProcessed(String sourceTimestamp, boolean changed, Instant aggregationStarted) {
        processed.increment();
        if (changed) changes.increment();
        try {
            long lag = Duration.between(Instant.parse(sourceTimestamp), aggregationStarted).toMillis();
            if (lag >= 0) {
                int bucket = lag <= 10000 ? (int) lag : lag <= 610000 ? 10000 + (int) ((lag - 10000 + 999) / 1000) : 10601;
                lagHistogram.incrementAndGet(bucket);
                lagSamples.increment();
                lagTotal.add(lag);
                lagMax.accumulateAndGet(lag, Math::max);
                if (lag >= 100) lagOver100.increment();
            }
        } catch (DateTimeParseException | NullPointerException ignored) {
            // Old event formats without a timezone cannot supply a valid lag sample.
        }
    }

    private String histogram() {
        StringBuilder value = new StringBuilder("[");
        for (int i = 0; i < lagHistogram.length(); i++) {
            long count = lagHistogram.get(i);
            if (count > 0) {
                if (value.length() > 1) value.append(';');
                value.append(i).append(':').append(count);
            }
        }
        return value.append(']').toString();
    }

    public String summary() {
        long samples = lagSamples.sum();
        return "Processed swipes=" + processed.sum() + ", duplicates=" + duplicates.sum()
                + ", leaderboard changes=" + changes.sum() + ", consumer lag samples=" + samples
                + ", mean lag ms=" + (samples == 0 ? "n/a" : lagTotal.sum() / (double) samples)
                + ", max lag ms=" + lagMax.get() + ", lag >=100ms=" + lagOver100.sum()
                + ", lag p95 upper ms=" + percentileUpper(.95, samples)
                + ", lag p99 upper ms=" + percentileUpper(.99, samples)
                + ", db batches=" + batches.sum() + ", db batch events=" + batchEvents.sum()
                + ", mean db batch ms=" + (batches.sum() == 0 ? 0 : batchNanos.sum() / 1_000_000.0 / batches.sum())
                + ", lag histogram=" + histogram()
                + sourceToReceive.summary("source-to-receive")
                + receiveToSql.summary("receive-to-sql")
                + ", stage timings missing receipt=" + stageMissingReceipt.sum();
    }
}
