
package websocket;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;

import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;

import storage.LeaderboardStore;

import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class LeaderboardWebSocketServer extends WebSocketServer {

    private final CompletableFuture<Void> startupResult =
            new CompletableFuture<>();

    private final Gson gson = new Gson();

    private final SubscriptionManager subscriptionManager =
            new SubscriptionManager();

    private final LeaderboardStore store;

    private final ConcurrentHashMap<WebSocket, String> sessions =
            new ConcurrentHashMap<>();

    public LeaderboardWebSocketServer(
            InetSocketAddress address,
            LeaderboardStore store) {

        super(address);
        this.store = Objects.requireNonNull(store);
    }

    private JsonArray buildLeaderboardEntries(
            SubscriptionKey key) throws Exception {

        List<Map.Entry<String, Long>> top10 =
                store.getTopN(
                        key.getResortId(),
                        key.getDayId(),
                        10
                );

        JsonArray entries = new JsonArray();
        int rank = 1;

        for (Map.Entry<String, Long> entry : top10) {
            JsonObject skier = new JsonObject();

            skier.addProperty("rank", rank++);
            skier.addProperty("skierId", entry.getKey());
            skier.addProperty("totalVertical", entry.getValue());

            entries.add(skier);
        }

        return entries;
    }

    private void sendInitialSnapshot(
            WebSocket conn,
            SubscriptionKey key) throws Exception {

        JsonObject snapshot = new JsonObject();

        snapshot.addProperty(
                "type",
                "leaderboard_snapshot"
        );

        snapshot.addProperty(
                "resortId",
                key.getResortId()
        );

        snapshot.addProperty(
                "dayId",
                key.getDayId()
        );

        snapshot.add(
                "top10",
                buildLeaderboardEntries(key)
        );

        if (conn.isOpen()) {
            conn.send(gson.toJson(snapshot));
        }
    }

    public void broadcastLeaderboardUpdate(
            SubscriptionKey key) throws Exception {

        Set<String> subscribers =
                subscriptionManager.getSubscribers(key);

        if (subscribers.isEmpty()) {
            return;
        }

        JsonArray entries = buildLeaderboardEntries(key);

        JsonObject update = new JsonObject();

        update.addProperty(
                "type",
                "leaderboard_update"
        );

        update.addProperty(
                "resortId",
                key.getResortId()
        );

        update.addProperty(
                "dayId",
                key.getDayId()
        );

        update.add("top10", entries);

        // Timestamp is created after reading the leaderboard
        // and immediately before broadcasting the update.
        update.addProperty(
                "serverTimestamp",
                Instant.now().toString()
        );

        String json = gson.toJson(update);

        for (Map.Entry<WebSocket, String> session
                : sessions.entrySet()) {

            WebSocket conn = session.getKey();
            String sessionId = session.getValue();

            if (!subscribers.contains(sessionId)) {
                continue;
            }

            synchronized (conn) {
                if (conn.isOpen()
                        && sessionId.equals(sessions.get(conn))
                        && subscriptionManager
                        .getSubscriptions(sessionId)
                        .contains(key)) {

                    try {
                        conn.send(json);
                    } catch (Exception e) {
                        System.err.println(
                                "Failed to send leaderboard update to "
                                        + sessionId + ": " + e
                        );
                    }
                }
            }
        }

        System.out.println(
                "Leaderboard update broadcast for " + key
        );
    }

    @Override
    public void onOpen(
            WebSocket conn,
            ClientHandshake handshake) {

        synchronized (conn) {
            String sessionId = UUID.randomUUID().toString();

            sessions.put(conn, sessionId);

            System.out.println(
                    "WebSocket connected: " + sessionId
            );
        }
    }

    @Override
    public void onMessage(
            WebSocket conn,
            String message) {

        synchronized (conn) {
            String sessionId = sessions.get(conn);

            if (sessionId == null || !conn.isOpen()) {
                return;
            }

            try {
                JsonObject json =
                        JsonParser.parseString(message)
                                .getAsJsonObject();

                String action =
                        getRequiredString(json, "action");

                String resortId =
                        getRequiredString(json, "resortId");

                String dayId =
                        getRequiredString(json, "dayId");

                SubscriptionKey key =
                        new SubscriptionKey(resortId, dayId);

                JsonObject response = new JsonObject();

                if ("subscribe".equals(action)) {

                    subscriptionManager.subscribe(sessionId, key);

                    response.addProperty(
                            "type",
                            "subscribed"
                    );

                    response.addProperty(
                            "resortId",
                            resortId
                    );

                    response.addProperty(
                            "dayId",
                            dayId
                    );

                    conn.send(gson.toJson(response));

                    try {
                        sendInitialSnapshot(conn, key);
                    } catch (Exception e) {
                        System.err.println(
                                "Failed to send initial snapshot: "
                                        + e
                        );

                        sendError(
                                conn,
                                "Failed to load initial leaderboard"
                        );
                    }

                    return;

                } else if ("unsubscribe".equals(action)) {

                    subscriptionManager.unsubscribe(
                            sessionId,
                            key
                    );

                    response.addProperty(
                            "type",
                            "unsubscribed"
                    );

                } else {
                    sendError(
                            conn,
                            "Unsupported action: " + action
                    );
                    return;
                }

                response.addProperty(
                        "resortId",
                        resortId
                );

                response.addProperty(
                        "dayId",
                        dayId
                );

                conn.send(gson.toJson(response));

            } catch (JsonParseException
                     | IllegalArgumentException
                     | IllegalStateException e) {

                sendError(
                        conn,
                        "Invalid request: " + e.getMessage()
                );
            }
        }
    }

    @Override
    public void onClose(
            WebSocket conn,
            int code,
            String reason,
            boolean remote) {

        synchronized (conn) {
            String sessionId = sessions.remove(conn);

            if (sessionId != null) {
                subscriptionManager.removeSession(sessionId);

                System.out.println(
                        "WebSocket disconnected: " + sessionId
                );
            }
        }
    }

    @Override
    public void onError(
            WebSocket conn,
            Exception ex) {

        System.err.println(
                "WebSocket error: " + ex.getMessage()
        );

        if (conn == null && !startupResult.isDone()) {
            startupResult.completeExceptionally(ex);
        }
    }

    @Override
    public void onStart() {

        System.out.println(
                "WebSocket Server listening on port "
                        + getPort()
        );

        startupResult.complete(null);
    }

    private String getRequiredString(
            JsonObject json,
            String field) {

        if (!json.has(field)
                || json.get(field).isJsonNull()
                || !json.get(field).isJsonPrimitive()
                || !json.get(field)
                .getAsJsonPrimitive()
                .isString()) {

            throw new IllegalArgumentException(
                    "Missing or invalid field: " + field
            );
        }

        String value =
                json.get(field).getAsString().trim();

        if (value.isEmpty()) {
            throw new IllegalArgumentException(
                    "Empty field: " + field
            );
        }

        return value;
    }

    private void sendError(
            WebSocket conn,
            String message) {

        JsonObject error = new JsonObject();

        error.addProperty(
                "type",
                "error"
        );

        error.addProperty(
                "message",
                message
        );

        if (conn.isOpen()) {
            conn.send(gson.toJson(error));
        }
    }

    public SubscriptionManager getSubscriptionManager() {
        return subscriptionManager;
    }

    public void awaitStartup() throws Exception {
        try {
            startupResult.get(
                    10,
                    TimeUnit.SECONDS
            );

        } catch (ExecutionException e) {
            Throwable cause = e.getCause();

            if (cause instanceof Exception) {
                throw (Exception) cause;
            }

            throw new IllegalStateException(
                    "WebSocket startup failed",
                    cause
            );
        }
    }
}
