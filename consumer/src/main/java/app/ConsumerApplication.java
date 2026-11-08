
package app;

import com.rabbitmq.client.Channel;
import config.ConsumerConfig;
import rabbitmq.RabbitMQConsumerManager;
import rabbitmq.ResortConsumer;
import rabbitmq.LeaderboardUpdatePublisher;

import java.util.concurrent.CountDownLatch;
import storage.LeaderboardStore;
import storage.SqliteLeaderboardStore;

import java.nio.file.Files;
import java.nio.file.Path;

import java.util.List;
import java.util.ArrayList;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import concurrency.ResortSerialExecutor;
import java.util.Map;
import java.util.HashMap;

import java.util.concurrent.TimeUnit;

public class ConsumerApplication {

    public static void main(String[] args) throws Exception {

        RabbitMQConsumerManager manager =
                new RabbitMQConsumerManager();

        manager.connect();


        Channel publisherChannel = manager.createChannel();

        LeaderboardUpdatePublisher updatePublisher =
                new LeaderboardUpdatePublisher(publisherChannel);

        System.out.println(
                "Leaderboard Update Publisher initialized."
        );


        ExecutorService workerPool =
                Executors.newFixedThreadPool(
                        ConsumerConfig.WORKER_THREADS
                );


        Map<Integer, ResortSerialExecutor> resortExecutors =
                new HashMap<>();

        for (int resortId = 1;
             resortId <= ConsumerConfig.RESORT_COUNT;
             resortId++) {

            ResortSerialExecutor serialExecutor =
                    new ResortSerialExecutor(workerPool);

            resortExecutors.put(resortId, serialExecutor);
        }

        System.out.println(
                "Created " + resortExecutors.size()
                        + " ResortSerialExecutors sharing one worker pool."
        );


        System.out.println(
                "Consumer worker pool initialized with "
                        + ConsumerConfig.WORKER_THREADS
                        + " threads."
        );


        Path dataDirectory = Path.of("data");
        Files.createDirectories(dataDirectory);

        Path databasePath =
                dataDirectory.resolve("leaderboard.db");

        LeaderboardStore store =
                new SqliteLeaderboardStore(databasePath.toString());

        System.out.println(
                "SQLite leaderboard initialized at: "
                        + databasePath.toAbsolutePath()
        );


        List<Channel> channels = new ArrayList<>();
        Map<Channel, String> consumerTags = new HashMap<>();
        List<ResortConsumer> consumers = new ArrayList<>();

        for (int resortId = 1;
             resortId <= ConsumerConfig.RESORT_COUNT;
             resortId++) {
            Channel channel = manager.createChannel();
            channels.add(channel);


            ResortConsumer consumer =
                    new ResortConsumer(
                            channel,
                            store,
                            resortExecutors.get(resortId),
                            updatePublisher
                    );

            consumers.add(consumer);

            String consumerTag = consumer.start(resortId);
            consumerTags.put(channel, consumerTag);
        }





        Runtime.getRuntime().addShutdownHook(new Thread(() -> {

            System.out.println("Shutting down ConsumerApplication...");

            // Step 0: Prevent callbacks from admitting new tasks.
            for (ResortConsumer consumer : consumers) {
                consumer.stopAccepting();
            }

            // Step 1: Stop receiving new messages.
            for (Channel channel : channels) {
                try {
                    if (channel.isOpen()) {
                        String consumerTag = consumerTags.get(channel);

                        if (consumerTag != null) {
                            channel.basicCancel(consumerTag);
                        }
                    }
                } catch (Exception e) {
                    System.err.println(
                            "Failed to cancel consumer: " + e.getMessage()
                    );
                }
            }



            // Step 2: Stop accepting new worker tasks.
            workerPool.shutdown();

            // Step 3: Wait for submitted tasks to finish.
            boolean workersTerminated = false;

            try {
                workersTerminated =
                        workerPool.awaitTermination(
                                60, TimeUnit.SECONDS
                        );

                if (!workersTerminated) {
                    System.err.println(
                            "Worker pool did not finish within 60 seconds. "
                                    + "Unacked messages may need redelivery."
                    );
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                System.err.println(
                        "Shutdown interrupted while waiting for workers."
                );
            }

            // Step 4: Close SQLite after workers terminate.
            if (workersTerminated) {
                try {
                    store.close();
                    System.out.println(
                            "SQLite leaderboard closed."
                    );
                } catch (Exception e) {
                    System.err.println(
                            "Failed to close SQLite leaderboard: "
                                    + e.getMessage()
                    );
                }
            } else {
                System.err.println(
                        "Skipping explicit SQLite close: "
                                + "worker tasks may still be active."
                );
            }


            // Step 5: Close the dedicated publisher channel.
            // Workers should have finished before this point.
            try {
                if (publisherChannel.isOpen()) {
                    publisherChannel.close();
                }

                System.out.println(
                        "Leaderboard Publisher Channel closed."
                );

            } catch (Exception e) {
                System.err.println(
                        "Failed to close Publisher Channel: "
                                + e.getMessage()
                );
            }

            for (Channel channel : channels) {
                try {
                    if (channel.isOpen()) {
                        channel.close();
                    }
                } catch (Exception e) {
                    System.err.println(
                            "Failed to close channel: " + e.getMessage()
                    );
                }
            }

            // Step 6: Close RabbitMQ connection.
            try {
                manager.close();
            } catch (Exception e) {
                System.err.println(
                        "Failed to close connection: " + e.getMessage()
                );
            }

            System.out.println("ConsumerApplication shutdown complete.");

        }));


        // Keep the consumer process alive.
        new CountDownLatch(1).await();
    }
}
