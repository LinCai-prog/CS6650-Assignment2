
package client;

import model.SwipeEvent;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

public class SwipeSender {

    private static final int MAX_RETRIES = 5;
    private static final long BASE_DELAY_MS = 100;

    private final HttpClient httpClient;
    private final String baseUrl;

    public SwipeSender(HttpClient httpClient, String baseUrl) {
        this.httpClient = httpClient;
        this.baseUrl = baseUrl;
    }

    public int sendSwipe(SwipeEvent event)
            throws IOException, InterruptedException {

        // Build URL.
        String url = String.format(
                "%s/skiers/%d/seasons/%d/days/%d/skiers/%d",
                baseUrl,
                event.getResortID(),
                event.getSeasonID(),
                event.getDayID(),
                event.getSkierID()
        );

        // Build JSON body.
        String jsonBody = String.format(
                "{\"time\":%d,\"liftID\":%d}",
                event.getTime(),
                event.getLiftID()
        );

        // Build the request once.
        // All retry attempts reuse the same X-Event-ID.
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
                .header("X-Event-ID", event.getEventId())
                .timeout(Duration.ofSeconds(10))
                .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                .build();

        IOException lastException = null;
        int lastStatusCode = -1;

        // Initial attempt + up to five retries.
        for (int attempt = 0; attempt <= MAX_RETRIES; attempt++) {

            try {
                HttpResponse<String> response =
                        httpClient.send(
                                request,
                                HttpResponse.BodyHandlers.ofString()
                        );

                int statusCode = response.statusCode();

                lastStatusCode = statusCode;
                lastException = null;

                if (statusCode == 201) {
                    return statusCode;
                }

                // Do not retry ordinary client errors.
                // Retry 429 and 5xx server errors.
                if (statusCode != 429 && statusCode < 500) {
                    return statusCode;
                }

                System.err.println(
                        "HTTP " + statusCode
                                + " on attempt " + (attempt + 1)
                );

            } catch (IOException e) {
                lastException = e;

                System.err.println(
                        "Request attempt " + (attempt + 1)
                                + " failed: " + e.getMessage()
                );
            }

            if (attempt == MAX_RETRIES) {
                break;
            }

            // Exponential backoff.
            long delay =
                    BASE_DELAY_MS * (1L << attempt);

            // Jitter reduces synchronized retry bursts.
            long jitter =
                    ThreadLocalRandom.current()
                            .nextLong(0, 51);

            Thread.sleep(delay + jitter);
        }

        if (lastException != null) {
            throw lastException;
        }

        return lastStatusCode;
    }
}
