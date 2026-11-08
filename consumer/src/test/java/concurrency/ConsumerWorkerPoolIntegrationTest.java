
package concurrency;

import model.SwipeEvent;
import org.junit.jupiter.api.Test;
import service.LeaderboardAggregator;

import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;
import com.google.gson.Gson;

class ConsumerWorkerPoolIntegrationTest {

    @Test
    void multipleResortsAggregateCorrectly()
            throws Exception {

        ExecutorService workerPool =
                Executors.newFixedThreadPool(3);

        try {
            ResortSerialExecutor resort1 =
                    new ResortSerialExecutor(workerPool);

            ResortSerialExecutor resort2 =
                    new ResortSerialExecutor(workerPool);

            LeaderboardAggregator aggregator =
                    new LeaderboardAggregator();

            CountDownLatch completed =
                    new CountDownLatch(5);

            // Resort 1: 3 events, including a duplicate.
            submit(resort1, aggregator,
                    event("A", "1", "1234", 213),
                    completed);

            submit(resort1, aggregator,
                    event("B", "1", "1234", 167),
                    completed);

            submit(resort1, aggregator,
                    event("A", "1", "1234", 213),
                    completed);

            // Resort 2: 2 independent events.
            submit(resort2, aggregator,
                    event("C", "2", "5678", 269),
                    completed);

            submit(resort2, aggregator,
                    event("D", "2", "5678", 100),
                    completed);

            assertTrue(
                    completed.await(5, TimeUnit.SECONDS),
                    "Worker tasks did not finish"
            );

            // Resort 1: 213 + 167, duplicate A ignored.
            assertEquals(
                    380,
                    aggregator.getTotalVertical(
                            "1", "1", "1234"
                    )
            );

            // Resort 2: 269 + 100.
            assertEquals(
                    369,
                    aggregator.getTotalVertical(
                            "2", "1", "5678"
                    )
            );

        } finally {
            workerPool.shutdownNow();
        }
    }

    private void submit(
            ResortSerialExecutor executor,
            LeaderboardAggregator aggregator,
            SwipeEvent event,
            CountDownLatch completed) {

        executor.execute(() -> {
            try {
                aggregator.processEvent(event);
            } finally {
                completed.countDown();
            }
        });
    }


    private SwipeEvent event(
            String eventId,
            String resortId,
            String skierId,
            int vertical) {

        String json = String.format(
                "{"
                        + "\"eventId\":\"%s\","
                        + "\"resortID\":\"%s\","
                        + "\"dayID\":\"1\","
                        + "\"skierID\":\"%s\","
                        + "\"liftID\":\"17\","
                        + "\"vertical\":%d,"
                        + "\"time\":217,"
                        + "\"timestamp\":\"2026-11-06T16:00:00\","
                        + "\"serverId\":\"test-server\""
                        + "}",
                eventId,
                resortId,
                skierId,
                vertical
        );

        return new Gson().fromJson(json, SwipeEvent.class);
    }
}
