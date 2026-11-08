
package app;

import rabbitmq.LeaderboardUpdateSubscriber;
import rabbitmq.RabbitMQConsumerManager;
import storage.LeaderboardStore;
import storage.SqliteLeaderboardStore;
import websocket.LeaderboardWebSocketServer;

import com.rabbitmq.client.Channel;

import java.net.InetSocketAddress;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class WebSocketApplication {

    public static void main(String[] args) throws Exception {

        int port = 8081;

        String databasePath = Paths.get(
                "data",
                "leaderboard.db"
        ).toAbsolutePath().toString();

        LeaderboardStore store =
                new SqliteLeaderboardStore(databasePath);

        System.out.println(
                "WebSocket SQLite database: " + databasePath
        );

        LeaderboardWebSocketServer server =
                new LeaderboardWebSocketServer(
                        new InetSocketAddress("127.0.0.1", port),
                        store
                );

        RabbitMQConsumerManager rabbitManager =
                new RabbitMQConsumerManager();

        AtomicBoolean cleanedUp = new AtomicBoolean(false);

        Runnable cleanup = () -> {
            if (!cleanedUp.compareAndSet(false, true)) {
                return;
            }

            // Stop RabbitMQ first so no new update callbacks arrive.
            try {
                rabbitManager.close();
                System.out.println(
                        "WebSocket RabbitMQ connection closed."
                );
            } catch (Exception e) {
                System.err.println(
                        "Failed to close RabbitMQ: " + e
                );
            }

            // Then stop WebSocket connections.
            try {
                server.stop(3000);
                System.out.println(
                        "WebSocket Server stopped."
                );
            } catch (Exception e) {
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                System.err.println(
                        "Failed to stop WebSocket: " + e
                );
            }

            // Close SQLite after callbacks and sockets are stopped.
            try {
                store.close();
                System.out.println(
                        "SQLite store closed."
                );
            } catch (Exception e) {
                System.err.println(
                        "Failed to close SQLite: " + e
                );
            }
        };

        Runtime.getRuntime().addShutdownHook(
                new Thread(cleanup, "websocket-shutdown")
        );

        try {
            // RabbitMQ connection initialization.
            rabbitManager.connect();

            Channel updateChannel = rabbitManager.createChannel();

            LeaderboardUpdateSubscriber updateSubscriber =
                    new LeaderboardUpdateSubscriber(
                            updateChannel,
                            server
                    );

            // Asynchronous WebSocket startup.
            server.start();

            // Wait until the listening socket is ready.
            // Throws on bind failure or startup timeout.
            server.awaitStartup();

            System.out.println(
                    "WebSocket startup confirmed. "
                            + "Starting RabbitMQ update subscriber..."
            );

            // Only consume notifications after WebSocket is ready.
            updateSubscriber.start();

            // Keep the application alive.
            new CountDownLatch(1).await();

        } catch (Exception e) {

            System.err.println(
                    "WebSocketApplication failed: " + e
            );

            cleanup.run();

            throw e;
        }
    }
}
