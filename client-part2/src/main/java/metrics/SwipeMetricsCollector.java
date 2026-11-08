
package metrics;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class SwipeMetricsCollector {

    private final Queue<SwipeMetric> metrics =
            new ConcurrentLinkedQueue<>();

    public void record(SwipeMetric metric) {
        if (metric == null) {
            throw new IllegalArgumentException(
                    "SwipeMetric cannot be null"
            );
        }
        metrics.add(metric);
    }

    public int getCount() {
        return metrics.size();
    }

    public void writeToCsv(String filename)
            throws IOException {

        try (BufferedWriter writer =
                     Files.newBufferedWriter(Path.of(filename))) {

            writer.write(
                    "timestamp,requestType,latency,statusCode,resortID"
            );
            writer.newLine();

            for (SwipeMetric metric : metrics) {

                writer.write(
                        metric.getSendTimestamp()
                                + ",SWIPE,"
                                + metric.getLatencyMs()
                                + ","
                                + metric.getStatusCode()
                                + ","
                                + metric.getResortID()
                );

                writer.newLine();
            }
        }

        System.out.println(
                "CSV saved: " + filename
                        + " (" + metrics.size() + " records)"
        );
    }

    public void printLatencyStatistics() {

        List<Double> allLatencies = new ArrayList<>();
        List<Double> successLatencies = new ArrayList<>();

        Map<Integer, Integer> statusCounts = new HashMap<>();

        for (SwipeMetric metric : metrics) {

            double latency = metric.getLatencyMs();
            int status = metric.getStatusCode();

            allLatencies.add(latency);

            if (status == 201) {
                successLatencies.add(latency);
            }

            statusCounts.merge(status, 1, Integer::sum);
        }

        System.out.println(
                "\n========== INGEST LATENCY STATISTICS =========="
        );

        printLatencyGroup(
                "ALL COMPLETED REQUESTS",
                allLatencies
        );

        printLatencyGroup(
                "SUCCESSFUL REQUESTS (HTTP 201)",
                successLatencies
        );

        System.out.println(
                "\n========== HTTP STATUS DISTRIBUTION =========="
        );

        List<Integer> codes =
                new ArrayList<>(statusCounts.keySet());

        Collections.sort(codes);

        for (int code : codes) {
            String label =
                    code == 0
                            ? "NO HTTP RESPONSE"
                            : Integer.toString(code);

            System.out.printf(
                    "Status %-17s : %d%n",
                    label,
                    statusCounts.get(code)
            );
        }
    }

    private void printLatencyGroup(
            String title,
            List<Double> latencies) {

        System.out.println("\n[" + title + "]");

        if (latencies.isEmpty()) {
            System.out.println(
                    "No latency metrics available."
            );
            return;
        }

        Collections.sort(latencies);

        int count = latencies.size();

        double sum = 0.0;

        for (double latency : latencies) {
            sum += latency;
        }

        double mean = sum / count;

        double median;

        if (count % 2 == 0) {
            median =
                    (latencies.get(count / 2 - 1)
                            + latencies.get(count / 2)) / 2.0;
        } else {
            median = latencies.get(count / 2);
        }

        double p95 = percentile(latencies, 0.95);
        double p99 = percentile(latencies, 0.99);

        System.out.println("Total Samples: " + count);
        System.out.printf("Mean:   %.2f ms%n", mean);
        System.out.printf("Median: %.2f ms%n", median);
        System.out.printf("P95:    %.2f ms%n", p95);
        System.out.printf("P99:    %.2f ms%n", p99);
        System.out.printf("Min:    %.2f ms%n", latencies.get(0));
        System.out.printf(
                "Max:    %.2f ms%n",
                latencies.get(count - 1)
        );
    }

    private static double percentile(
            List<Double> sortedValues,
            double p) {

        int index =
                (int) Math.ceil(p * sortedValues.size()) - 1;

        index = Math.max(
                0,
                Math.min(index, sortedValues.size() - 1)
        );

        return sortedValues.get(index);
    }

    public void printResortThroughput(
            double totalRuntimeSeconds) {

        if (totalRuntimeSeconds <= 0) {
            System.out.println("Invalid runtime.");
            return;
        }

        Map<Integer, Integer> resortCounts =
                new HashMap<>();

        for (SwipeMetric metric : metrics) {

            int resortID = metric.getResortID();

            resortCounts.merge(resortID, 1, Integer::sum);
        }

        System.out.println(
                "\n========== THROUGHPUT PER RESORT =========="
        );

        System.out.printf(
                "%-12s %-15s %-15s%n",
                "Resort ID",
                "Requests",
                "Throughput"
        );

        for (int resortID = 1; resortID <= 10; resortID++) {

            int count =
                    resortCounts.getOrDefault(resortID, 0);

            double throughput =
                    count / totalRuntimeSeconds;

            System.out.printf(
                    "%-12d %-15d %.2f req/sec%n",
                    resortID,
                    count,
                    throughput
            );
        }
    }
}
