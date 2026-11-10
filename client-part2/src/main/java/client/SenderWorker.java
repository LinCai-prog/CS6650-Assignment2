package client;

import metrics.SwipeMetric;
import metrics.SwipeMetricsCollector;
import model.SwipeEvent;

import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class SenderWorker implements Runnable {

    private final BlockingQueue<SwipeEvent> queue;
    private final SwipeSender sender;

    private final AtomicInteger successCount;
    private final AtomicInteger failureCount;

    private final int requestsPerThread;

    private final SwipeMetricsCollector metricsCollector;
    private final RequestPacer pacer;
    private final String phase;

    public SenderWorker(
            BlockingQueue<SwipeEvent> queue,
            SwipeSender sender,
            AtomicInteger successCount,
            AtomicInteger failureCount,
            int requestsPerThread,
            SwipeMetricsCollector metricsCollector, RequestPacer pacer, String phase) {

        this.queue = queue;
        this.sender = sender;
        this.successCount = successCount;
        this.failureCount = failureCount;
        this.requestsPerThread = requestsPerThread;

        // Save shared metrics collector
        this.metricsCollector = metricsCollector;
        this.pacer = pacer; this.phase = phase;
    }

    @Override
    public void run() {

        for (int i = 0; requestsPerThread < 0 || i < requestsPerThread; i++) {

            SwipeEvent event = queue.poll();
            if (event == null) {
                if (requestsPerThread < 0) return;
                throw new IllegalStateException("Queue unexpectedly empty before worker completed its quota");
            }
            try { pacer.acquire(); }
            catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); return; }

            // Record timestamp before sending
            Instant sendTimestamp = Instant.now();

            // Start latency measurement
            long startNano = System.nanoTime();

            // 0 means no HTTP response was received
            int statusCode = 0;

            try {

                // Existing retry logic stays inside SwipeSender
                statusCode = sender.sendSwipe(event);

                if (statusCode == 201) {

                    successCount.incrementAndGet();

                } else {

                    failureCount.incrementAndGet();
                }

            } catch (InterruptedException e) {

                Thread.currentThread().interrupt();

                failureCount.incrementAndGet();

                System.err.println(
                        "SenderWorker interrupted"
                );

                // Record metric before stopping
                recordMetric(
                        event,
                        sendTimestamp,
                        startNano,
                        statusCode
                );

                return;

            } catch (IOException e) {

                failureCount.incrementAndGet();

                System.err.println(
                        "Request permanently failed after retries: "
                                + e.getMessage()
                );

            }

            // Record metric for success and failure
            recordMetric(
                    event,
                    sendTimestamp,
                    startNano,
                    statusCode
            );
        }
    }

    private void recordMetric(
            SwipeEvent event,
            Instant sendTimestamp,
            long startNano,
            int statusCode) {

        long endNano = System.nanoTime();

        Instant responseTimestamp = Instant.now();

        double latencyMs =
                (endNano - startNano) / 1_000_000.0;

        SwipeMetric metric = new SwipeMetric(
                sendTimestamp,
                responseTimestamp,
                latencyMs,
                statusCode,
                event.getResortID(), event.getEventId(), phase
        );

        metricsCollector.record(metric);
    }
}