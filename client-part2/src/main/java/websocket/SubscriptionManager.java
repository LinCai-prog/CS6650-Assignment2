
package websocket;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.net.http.HttpClient;

public class SubscriptionManager {

    private final List<LeaderboardSubscriber> subscribers =
            new ArrayList<>();

    public SubscriptionManager(
            String baseUrl,
            int resortCount) {
        this(baseUrl, resortCount, 1);
    }

    public SubscriptionManager(String baseUrl, int resortCount, int dayId) {

        for (int i = 1; i <= resortCount; i++) {
            subscribers.add(
                    new LeaderboardSubscriber(
                            HttpClient.newBuilder()
                                    .cookieHandler(new java.net.CookieManager(null, java.net.CookiePolicy.ACCEPT_ALL))
                                    .connectTimeout(java.time.Duration.ofSeconds(10))
                                    .version(HttpClient.Version.HTTP_1_1).build(),
                            baseUrl,
                            i, dayId
                    )
            );
        }
    }

    public void startAll() {
        for (LeaderboardSubscriber subscriber : subscribers) {
            subscriber.start();
        }

        System.out.println(
                "Waiting for all WebSocket subscriptions..."
        );

        for (LeaderboardSubscriber subscriber : subscribers) {
            try {
                subscriber.awaitSubscribed(Duration.ofSeconds(20));
            } catch (Exception e) {
                stopAll();
                if (e instanceof InterruptedException) Thread.currentThread().interrupt();
                throw new IllegalStateException(
                        "WebSocket subscription failed for resort "
                                + subscriber.getResortID(),
                        e
                );
            }
        }

        System.out.println(
                "All WebSocket subscriptions confirmed."
        );
    }

    public void stopAll() {
        for (LeaderboardSubscriber subscriber : subscribers) {
            subscriber.stop();
        }
    }

    public void printStatistics() {

        System.out.println(
                "\n========== WEBSOCKET PERFORMANCE METRICS =========="
        );

        int totalMessages = 0;
        int distinctChanges = 0;
        int invalidUpdates = 0;
        int staleUpdates = 0;
        int totalConnections = 0;
        int totalReconnections = 0;

        List<Double> allConnectionTimes = new ArrayList<>();
        List<Long> allPushLatencies = new ArrayList<>();

        System.out.println(
                "\n[Leaderboard Updates per Subscriber]"
        );

        for (LeaderboardSubscriber subscriber : subscribers) {
            int resortID = subscriber.getResortID();
            int messageCount = subscriber.getMessageCount();

            System.out.printf(
                    "Resort %d: %d updates%n",
                    resortID,
                    messageCount
            );

            totalMessages += messageCount;
            distinctChanges += subscriber.getDistinctChangeCount();
            invalidUpdates += subscriber.getInvalidUpdateCount();
            staleUpdates += subscriber.getStaleUpdateCount();
            totalConnections += subscriber.getConnectionCount();
            totalReconnections += subscriber.getReconnectionCount();

            allConnectionTimes.addAll(
                    subscriber.getConnectionTimes()
            );

            allPushLatencies.addAll(
                    subscriber.getPushLatencies()
            );
        }

        System.out.println("\n[Connection Statistics]");

        System.out.println(
                "Successful Connections: " + totalConnections
        );

        System.out.println(
                "Reconnections: " + totalReconnections
        );

        if (!allConnectionTimes.isEmpty()) {
            System.out.printf(
                    "Average Connection Time: %.2f ms%n",
                    allConnectionTimes.stream()
                            .mapToDouble(Double::doubleValue)
                            .average()
                            .orElse(0.0)
            );

            System.out.printf(
                    "Min Connection Time: %.2f ms%n",
                    Collections.min(allConnectionTimes)
            );

            System.out.printf(
                    "Max Connection Time: %.2f ms%n",
                    Collections.max(allConnectionTimes)
            );
        }

        System.out.println(
                "\n[Leaderboard Delivery Statistics]"
        );

        System.out.println(
                "Total Leaderboard Updates Delivered: "
                        + totalMessages
        );

        System.out.println("Distinct standings changes received: " + distinctChanges);
        System.out.println("Invalid leaderboard payloads: " + invalidUpdates);
        System.out.println("Duplicate or older versions received: " + staleUpdates);

        System.out.println(
                "\n========== PUSH LATENCY STATISTICS =========="
        );

        if (allPushLatencies.isEmpty()) {
            System.out.println(
                    "No valid push latency samples received."
            );
            return;
        }

        Collections.sort(allPushLatencies);

        int count = allPushLatencies.size();

        double mean = allPushLatencies.stream()
                .mapToDouble(Long::doubleValue)
                .average()
                .orElse(0.0);

        double median;

        if (count % 2 == 0) {
            median =
                    (allPushLatencies.get(count / 2 - 1)
                            + allPushLatencies.get(count / 2))
                            / 2.0;
        } else {
            median = allPushLatencies.get(count / 2);
        }

        double p95 = percentile(allPushLatencies, 0.95);
        double p99 = percentile(allPushLatencies, 0.99);

        System.out.println(
                "Total Push Latency Samples: " + count
        );

        System.out.printf("Mean:   %.2f ms%n", mean);
        System.out.printf("Median: %.2f ms%n", median);
        System.out.printf("P95:    %.2f ms%n", p95);
        System.out.printf("P99:    %.2f ms%n", p99);
    }

    private static double percentile(
            List<Long> sortedValues,
            double p) {

        int index =
                (int) Math.ceil(p * sortedValues.size()) - 1;

        index = Math.max(
                0,
                Math.min(index, sortedValues.size() - 1)
        );

        return sortedValues.get(index);
    }
}
