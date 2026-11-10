package rabbitmq;

import com.rabbitmq.client.Channel;
import java.util.*;
import java.util.concurrent.*;

/** Only a contiguous prefix of registered, committed deliveries may be ACKed. */
public final class BatchAcknowledgements implements AutoCloseable {
    private final Channel channel;
    private final int batchSize;
    private final NavigableMap<Long, Boolean> deliveries = new TreeMap<>();
    private final ScheduledExecutorService timer;

    public BatchAcknowledgements(Channel channel, int batchSize, int flushMs) {
        if (batchSize < 1 || flushMs < 1) throw new IllegalArgumentException("positive ACK limits required");
        this.channel = channel;
        this.batchSize = batchSize;
        channel.addShutdownListener(cause -> reset());
        timer = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "leaderboard-ack-flush"));
        timer.scheduleWithFixedDelay(this::flush, flushMs, flushMs, TimeUnit.MILLISECONDS);
    }

    private synchronized void reset() { deliveries.clear(); }

    public synchronized void register(long tag) { deliveries.put(tag, false); }
    public synchronized void discarded(long tag) { deliveries.remove(tag); }
    public synchronized void committed(long tag) {
        if (!deliveries.containsKey(tag)) return;
        deliveries.put(tag, true);
        int ready = 0;
        for (boolean done : deliveries.values()) { if (!done) break; ready++; }
        if (ready >= batchSize) flush();
    }

    public synchronized void flush() {
        Long last = null;
        int count = 0;
        for (var item : deliveries.entrySet()) {
            if (!item.getValue() || count == batchSize) break;
            last = item.getKey(); count++;
        }
        if (last == null) return;
        try {
            synchronized (channel) {
                if (!channel.isOpen()) return;
                channel.basicAck(last, true);
            }
            deliveries.headMap(last, true).clear();
        } catch (Exception uncertain) {
            // Retrying an uncertain tag could double-ACK; retain broker ownership
            // until recovery. Remove only this committed prefix from our tracker.
            deliveries.headMap(last, true).clear();
            System.err.println("Batch ACK outcome uncertain: " + uncertain.getClass().getSimpleName());
        }
    }

    @Override public void close() throws InterruptedException {
        timer.shutdown();
        if (!timer.awaitTermination(5, TimeUnit.SECONDS)) timer.shutdownNow();
        synchronized (this) { while (!deliveries.isEmpty() && deliveries.firstEntry().getValue()) {
            int before = deliveries.size(); flush(); if (deliveries.size() == before) break;
        } }
    }
}
