
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
    private static final int DAY_ID = Integer.parseInt(System.getenv().getOrDefault("LOAD_DAY_ID", "1"));

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
            System.getenv().getOrDefault("HTTP_BASE_URL",
                    "http://cs6650-ingest-alb-972956563.us-west-2.elb.amazonaws.com/server-v2");

    // The ALB routes upgrades to the real WebSocket endpoint on each ingest host.
    private static final String WS_BASE_URL =
            System.getenv().getOrDefault("WS_BASE_URL",
                    "ws://cs6650-ingest-alb-972956563.us-west-2.elb.amazonaws.com/server-v2/leaderboard");

    public static void main(String[] args)
            throws InterruptedException, IOException {

        String mode = args.length > 0 ? args[0] : "smoke";

        if (!mode.equals("smoke")
                && !mode.equals("medium")
                && !mode.equals("full")) {
            throw new IllegalArgumentException(
                    "Usage: LoadClient [smoke|medium|full] [wsDrainSeconds] [mainThreads] [warmupThreads]"
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

        if (DAY_ID < 1 || DAY_ID > 999_999_999) {
            throw new IllegalArgumentException("LOAD_DAY_ID must be in 1..999999999");
        }
        if (args.length > 4) throw new IllegalArgumentException("Too many arguments");
        mainThreads = positiveOption(args, 2, "INGEST_THREADS", mainThreads);
        warmupThreads = positiveOption(args, 3, "WARMUP_THREADS", warmupThreads);
        String totalOverride = System.getenv("LOAD_TOTAL_EVENTS");
        if (totalOverride != null) {
            int total = Integer.parseInt(totalOverride);
            if (total <= warmupEvents || total > 2000000) throw new IllegalArgumentException("LOAD_TOTAL_EVENTS must exceed warmup and be <=2000000");
            mainEvents = total - warmupEvents;
        }
        int targetRps = Integer.parseInt(System.getenv().getOrDefault("TARGET_RPS", "0"));
        int warmupTargetRps = Integer.parseInt(System.getenv().getOrDefault("WARMUP_TARGET_RPS", Integer.toString(targetRps)));
        new client.RequestPacer(targetRps); // Validate before opening connections.
        new client.RequestPacer(warmupTargetRps);
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

        // Preserve completed requests when a queue guard stops this process.
        java.util.concurrent.atomic.AtomicBoolean metricsExported =
                new java.util.concurrent.atomic.AtomicBoolean(false);
        final String partialCsvFilename = csvFilename.replace(".csv", "_partial.csv");
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (!metricsExported.get() && metricsCollector.getCount() > 0) {
                try {
                    metricsCollector.writeToCsv(partialCsvFilename);
                } catch (IOException error) {
                    System.err.println("Unable to save partial request metrics: " + error.getMessage());
                }
            }
        }, "partial-request-metrics"));

        SubscriptionManager subscriptionManager =
                new SubscriptionManager(
                        WS_BASE_URL,
                        RESORT_COUNT, DAY_ID
                );

        System.out.println(
                "========== CS6650 LOAD TEST =========="
        );

        System.out.println("Mode: " + mode);
        System.out.println("Target RPS: " + (targetRps == 0 ? "unpaced" : targetRps));
        System.out.println("Warmup Target RPS: " + (warmupTargetRps == 0 ? "unpaced" : warmupTargetRps));
        System.out.println("Warmup Threads: " + warmupThreads);
        System.out.println("Main Threads: " + mainThreads);
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
            subscribersStarted = true;
            subscriptionManager.startAll();

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
                    metricsCollector, warmupTargetRps, "WARMUP"
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
                    metricsCollector, targetRps, "MAIN"
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
            // Persist request evidence before waiting for WebSocket drain.
            System.out.println("\n========== EXPORTING CSV ==========");
            metricsCollector.writeToCsv(csvFilename);
            metricsExported.set(true);

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

    private static int positiveOption(String[] args, int index, String env, int fallback) {
        String raw = args.length > index ? args[index] : System.getenv(env);
        int value = raw == null ? fallback : Integer.parseInt(raw);
        if (value < 1 || value > 4096) throw new IllegalArgumentException(env + " must be in 1..4096");
        return value;
    }

    private static void runPhase(
            int totalEvents,
            int threadCount,
            SwipeSender sender,
            AtomicInteger successCount,
            AtomicInteger failureCount,
            SwipeMetricsCollector metricsCollector, int targetRps, String phase
    ) throws InterruptedException {

        if (threadCount < 1 || totalEvents < 1) throw new IllegalArgumentException("Positive events and threads required");

        BlockingQueue<SwipeEvent> queue =
                new ArrayBlockingQueue<>(totalEvents);

        SwipeGenerator generator =
                new SwipeGenerator(
                        queue,
                        totalEvents,
                        RESORT_COUNT, DAY_ID
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
        client.RequestPacer pacer = new client.RequestPacer(targetRps);

        for (int i = 0; i < threadCount; i++) {
            futures.add(
                    executor.submit(
                            new SenderWorker(
                                    queue,
                                    sender,
                                    successCount,
                                    failureCount,
                                    targetRps > 0 ? -1 : RequestAllocation.requestsForWorker(totalEvents, threadCount, i),
                                    metricsCollector, pacer, phase
                            )
                    )
            );
        }

        executor.shutdown();

        // Allow each load-test phase to run for up to sixty minutes.
        long phaseTimeoutSeconds = TimeUnit.MINUTES.toSeconds(60);
        System.out.println("Phase worker timeout: " + phaseTimeoutSeconds + " seconds");
        boolean finished = executor.awaitTermination(
                phaseTimeoutSeconds,
                TimeUnit.SECONDS
        );

        if (!finished) {
            executor.shutdownNow();
            throw new IllegalStateException(
                    "Workers did not finish within " + phaseTimeoutSeconds + " seconds"
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
