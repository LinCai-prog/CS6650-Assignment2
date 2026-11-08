
import client.SenderWorker;
import client.SwipeSender;
import generator.SwipeGenerator;
import metrics.SwipeMetricsCollector;
import model.SwipeEvent;
import websocket.SubscriptionManager;

import java.io.IOException;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class LoadClient {

    private static final int RESORT_COUNT = 10;

    // Smoke: 1,000 events
    private static final int SMOKE_WARMUP_EVENTS = 100;
    private static final int SMOKE_WARMUP_THREADS = 10;
    private static final int SMOKE_MAIN_EVENTS = 900;
    private static final int SMOKE_MAIN_THREADS = 20;

    // Medium: 10,000 events
    private static final int MEDIUM_WARMUP_EVENTS = 1000;
    private static final int MEDIUM_WARMUP_THREADS = 20;
    private static final int MEDIUM_MAIN_EVENTS = 9000;
    private static final int MEDIUM_MAIN_THREADS = 50;

    // Full: 500,000 events
    private static final int FULL_WARMUP_EVENTS = 32000;
    private static final int FULL_WARMUP_THREADS = 32;
    private static final int FULL_MAIN_EVENTS = 468000;
    private static final int FULL_MAIN_THREADS = 100;

    private static final int DEFAULT_WS_DRAIN_SECONDS = 30;

    private static final String BASE_URL =
            "http://cs6650-ingest-alb-972956563.us-west-2.elb.amazonaws.com/server-v2";

    // Requires an active SSH tunnel on the Mac.
    private static final String WS_BASE_URL =
            "ws://127.0.0.1:18081";

    public static void main(String[] args)
            throws InterruptedException, IOException {

        String mode = args.length > 0 ? args[0] : "smoke";

        if (!mode.equals("smoke")
                && !mode.equals("medium")
                && !mode.equals("full")) {
            throw new IllegalArgumentException(
                    "Usage: LoadClient [smoke|medium|full] [wsDrainSeconds]"
            );
        }

        int wsDrainSeconds = DEFAULT_WS_DRAIN_SECONDS;

        if (args.length > 1) {
            try {
                wsDrainSeconds = Integer.parseInt(args[1]);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        "wsDrainSeconds must be an integer",
                        e
                );
            }
        }

        if (wsDrainSeconds < 0 || wsDrainSeconds > 600) {
            throw new IllegalArgumentException(
                    "wsDrainSeconds must be between 0 and 600"
            );
        }

        int warmupEvents;
        int warmupThreads;
        int mainEvents;
        int mainThreads;
        String csvFilename;

        switch (mode) {
            case "full":
                warmupEvents = FULL_WARMUP_EVENTS;
                warmupThreads = FULL_WARMUP_THREADS;
                mainEvents = FULL_MAIN_EVENTS;
                mainThreads = FULL_MAIN_THREADS;
                csvFilename = "swipe_metrics_full.csv";
                break;

            case "medium":
                warmupEvents = MEDIUM_WARMUP_EVENTS;
                warmupThreads = MEDIUM_WARMUP_THREADS;
                mainEvents = MEDIUM_MAIN_EVENTS;
                mainThreads = MEDIUM_MAIN_THREADS;
                csvFilename = "swipe_metrics_medium.csv";
                break;

            default:
                warmupEvents = SMOKE_WARMUP_EVENTS;
                warmupThreads = SMOKE_WARMUP_THREADS;
                mainEvents = SMOKE_MAIN_EVENTS;
                mainThreads = SMOKE_MAIN_THREADS;
                csvFilename = "swipe_metrics_smoke.csv";
                break;
        }

        int expectedTotal = warmupEvents + mainEvents;

        HttpClient httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .version(HttpClient.Version.HTTP_1_1)
                .build();

        SwipeSender sender =
                new SwipeSender(httpClient, BASE_URL);

        AtomicInteger successCount = new AtomicInteger();
        AtomicInteger failureCount = new AtomicInteger();

        SwipeMetricsCollector metricsCollector =
                new SwipeMetricsCollector();

        SubscriptionManager subscriptionManager =
                new SubscriptionManager(
                        httpClient,
                        WS_BASE_URL,
                        RESORT_COUNT
                );

        System.out.println(
                "========== CS6650 LOAD TEST =========="
        );

        System.out.println("Mode: " + mode);
        System.out.println("Expected Requests: " + expectedTotal);
        System.out.println("HTTP Endpoint: " + BASE_URL);
        System.out.println("WebSocket Endpoint: " + WS_BASE_URL);
        System.out.println(
                "WebSocket Drain Period: "
                        + wsDrainSeconds + " seconds"
        );

        double warmupSeconds;
        double mainSeconds;

        boolean subscribersStarted = false;

        try {
            // Start WebSocket subscribers BEFORE HTTP traffic.
            subscriptionManager.startAll();
            subscribersStarted = true;

            System.out.println(
                    "\n========== WARMUP PHASE =========="
            );

            int successBeforeWarmup = successCount.get();
            int failureBeforeWarmup = failureCount.get();

            long warmupStart = System.nanoTime();

            runPhase(
                    warmupEvents,
                    warmupThreads,
                    sender,
                    successCount,
                    failureCount,
                    metricsCollector
            );

            warmupSeconds =
                    (System.nanoTime() - warmupStart)
                            / 1_000_000_000.0;

            printPhaseMetrics(
                    "WARMUP",
                    warmupEvents,
                    successCount.get() - successBeforeWarmup,
                    failureCount.get() - failureBeforeWarmup,
                    warmupSeconds
            );

            System.out.println(
                    "\n========== MAIN PHASE =========="
            );

            int successBeforeMain = successCount.get();
            int failureBeforeMain = failureCount.get();

            long mainStart = System.nanoTime();

            runPhase(
                    mainEvents,
                    mainThreads,
                    sender,
                    successCount,
                    failureCount,
                    metricsCollector
            );

            mainSeconds =
                    (System.nanoTime() - mainStart)
                            / 1_000_000_000.0;

            printPhaseMetrics(
                    "MAIN",
                    mainEvents,
                    successCount.get() - successBeforeMain,
                    failureCount.get() - failureBeforeMain,
                    mainSeconds
            );

            // HTTP measurement ends HERE.
            // WebSocket drain time is NOT part of HTTP throughput.
            double totalSeconds = warmupSeconds + mainSeconds;

            int totalSuccess = successCount.get();
            int totalFailure = failureCount.get();
            int completedRequests = totalSuccess + totalFailure;

            double overallThroughput =
                    completedRequests / totalSeconds;

            System.out.println(
                    "\n========== OVERALL PERFORMANCE =========="
            );

            System.out.println(
                    "Expected Requests: " + expectedTotal
            );

            System.out.println(
                    "Completed Requests: " + completedRequests
            );

            System.out.println(
                    "Successful Swipes: " + totalSuccess
            );

            System.out.println(
                    "Failed Swipes: " + totalFailure
            );

            System.out.printf(
                    "HTTP Runtime: %.2f seconds%n",
                    totalSeconds
            );

            System.out.printf(
                    "Overall HTTP Throughput: %.2f req/sec%n",
                    overallThroughput
            );

            int recordedMetrics = metricsCollector.getCount();

            System.out.println(
                    "\n========== VALIDATION =========="
            );

            System.out.println(
                    "Completed Requests: " + completedRequests
            );

            System.out.println(
                    "Recorded Metrics: " + recordedMetrics
            );

            if (completedRequests != expectedTotal) {
                throw new IllegalStateException(
                        "Expected " + expectedTotal
                                + " requests, but completed "
                                + completedRequests
                );
            }

            if (recordedMetrics != completedRequests) {
                throw new IllegalStateException(
                        "Metrics count mismatch: "
                                + recordedMetrics
                                + " metrics for "
                                + completedRequests
                                + " requests"
                );
            }

            System.out.println("Validation PASSED!");

            // Snapshot BEFORE the drain period.
            System.out.println(
                    "\n========== WEBSOCKET: BEFORE DRAIN =========="
            );
            subscriptionManager.printStatistics();

            // Keep subscribers connected so that delayed messages
            // have additional time to arrive.
            if (wsDrainSeconds > 0) {
                System.out.println(
                        "\n========== WEBSOCKET DRAIN =========="
                );

                System.out.println(
                        "HTTP traffic has finished."
                );

                System.out.println(
                        "Keeping subscribers connected for "
                                + wsDrainSeconds
                                + " seconds."
                );

                Thread.sleep(
                        TimeUnit.SECONDS.toMillis(wsDrainSeconds)
                );
            }

            // Snapshot AFTER the drain period.
            System.out.println(
                    "\n========== WEBSOCKET: AFTER DRAIN =========="
            );
            subscriptionManager.printStatistics();

            System.out.println(
                    "\n========== STATISTICAL ANALYSIS =========="
            );

            metricsCollector.printLatencyStatistics();
            metricsCollector.printResortThroughput(totalSeconds);

            System.out.println(
                    "\n========== EXPORTING CSV =========="
            );

            metricsCollector.writeToCsv(csvFilename);

            System.out.println(
                    "\n========== LOAD TEST FINISHED =========="
            );

        } finally {
            if (subscribersStarted) {
                System.out.println(
                        "\nStopping WebSocket subscribers."
                );

                subscriptionManager.stopAll();
            }
        }
    }

    private static void runPhase(
            int totalEvents,
            int threadCount,
            SwipeSender sender,
            AtomicInteger successCount,
            AtomicInteger failureCount,
            SwipeMetricsCollector metricsCollector
    ) throws InterruptedException {

        if (totalEvents % threadCount != 0) {
            throw new IllegalArgumentException(
                    "Events must be divisible by thread count"
            );
        }

        int requestsPerThread =
                totalEvents / threadCount;

        BlockingQueue<SwipeEvent> queue =
                new ArrayBlockingQueue<>(totalEvents);

        SwipeGenerator generator =
                new SwipeGenerator(
                        queue,
                        totalEvents,
                        RESORT_COUNT
                );

        Thread generatorThread = new Thread(generator);
        generatorThread.start();
        generatorThread.join();

        if (queue.size() != totalEvents) {
            throw new IllegalStateException(
                    "Generator produced "
                            + queue.size()
                            + " events instead of "
                            + totalEvents
            );
        }

        System.out.println(
                "Generated Events: " + queue.size()
        );

        ExecutorService executor =
                Executors.newFixedThreadPool(threadCount);

        List<Future<?>> futures = new ArrayList<>();

        for (int i = 0; i < threadCount; i++) {
            futures.add(
                    executor.submit(
                            new SenderWorker(
                                    queue,
                                    sender,
                                    successCount,
                                    failureCount,
                                    requestsPerThread,
                                    metricsCollector
                            )
                    )
            );
        }

        executor.shutdown();

        boolean finished = executor.awaitTermination(
                30,
                TimeUnit.MINUTES
        );

        if (!finished) {
            executor.shutdownNow();
            throw new IllegalStateException(
                    "Workers did not finish within 30 minutes"
            );
        }

        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (ExecutionException e) {
                throw new IllegalStateException(
                        "Worker failed",
                        e.getCause()
                );
            }
        }

        if (!queue.isEmpty()) {
            throw new IllegalStateException(
                    "Unprocessed events: " + queue.size()
            );
        }

        System.out.println(
                "Phase completed successfully."
        );
    }

    private static void printPhaseMetrics(
            String phase,
            int expectedRequests,
            int success,
            int failure,
            double seconds) {

        int completed = success + failure;

        System.out.println(
                "\n----- " + phase + " RESULTS -----"
        );

        System.out.println(
                "Expected Requests: " + expectedRequests
        );

        System.out.println(
                "Completed Requests: " + completed
        );

        System.out.println(
                "Successful Swipes: " + success
        );

        System.out.println(
                "Failed Swipes: " + failure
        );

        System.out.printf(
                "Runtime: %.2f seconds%n",
                seconds
        );

        System.out.printf(
                "Throughput: %.2f req/sec%n",
                completed / seconds
        );
    }
}
