package leaderboard;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import model.LeaderboardUpdate;
import javax.websocket.Session;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Only manages connections accepted by this Tomcat application. */
public final class LeaderboardSessions {
    private static final class Subscription {
        long version = -1;
        boolean initialized;
        LeaderboardUpdate pending;
    }
    private static final class Client {
        final Session session;
        final SessionSender sender;
        final Map<String, Subscription> subscriptions = new HashMap<>();
        Client(Session session) { this.session = session; this.sender = new SessionSender(session); }
    }
    private final Map<Session, Client> clients = new ConcurrentHashMap<>();
    private final SnapshotClient snapshots;
    private final String serverId;

    public LeaderboardSessions(SnapshotClient snapshots, String serverId) {
        this.snapshots = snapshots;
        this.serverId = serverId;
    }
    public void open(Session session) {
        session.setMaxIdleTimeout(180_000);
        session.getAsyncRemote().setSendTimeout(5_000);
        clients.put(session, new Client(session));
    }
    public void close(Session session) { clients.remove(session); }

    public void message(Session session, String text) {
        Client client = clients.get(session);
        if (client == null) return;
        try {
            JsonObject json = JsonParser.parseString(text).getAsJsonObject();
            String action = json.get("action").getAsString();
            String resort = identifier(json, "resortId");
            String day = identifier(json, "dayId");
            if (Integer.parseInt(resort) < 1 || Integer.parseInt(resort) > 10) throw new IllegalArgumentException();
            synchronized (client) {
                if (action.equals("subscribe")) {
                    if (client.subscriptions.size() >= 32 && !client.subscriptions.containsKey(resort + ":" + day)) {
                        throw new IllegalArgumentException("Too many subscriptions");
                    }
                    subscribe(client, resort, day);
                } else if (action.equals("unsubscribe")) {
                    client.subscriptions.remove(resort + ":" + day);
                    client.sender.send(response("unsubscribed", resort, day).toString());
                } else throw new IllegalArgumentException("Unknown action");
            }
        } catch (RuntimeException e) { error(client, "Invalid subscription request"); }
    }
    private String identifier(JsonObject json, String field) {
        String value = json.get(field).getAsString();
        if (!value.matches("[1-9][0-9]{0,8}")) throw new IllegalArgumentException("Invalid identifier");
        return value;
    }
    private JsonObject response(String type, String resort, String day) {
        JsonObject json = new JsonObject();
        json.addProperty("type", type); json.addProperty("resortId", resort); json.addProperty("dayId", day);
        json.addProperty("serverId", serverId);
        return json;
    }
    private void error(Client client, String message) {
        JsonObject json = new JsonObject(); json.addProperty("type", "error"); json.addProperty("message", message);
        client.sender.send(json.toString());
    }
    private void subscribe(Client client, String resort, String day) {
        String key = resort + ":" + day;
        Subscription subscription = new Subscription();
        client.subscriptions.put(key, subscription);
        snapshots.fetch(resort, day).whenComplete((snapshot, failure) -> {
            synchronized (client) {
                if (clients.get(client.session) != client || client.subscriptions.get(key) != subscription) return;
                if (failure != null || snapshot == null || !snapshot.getResortId().equals(resort) || !snapshot.getDayId().equals(day)) {
                    error(client, "Leaderboard snapshot unavailable; retrying");
                    java.util.concurrent.CompletableFuture.delayedExecutor(2, java.util.concurrent.TimeUnit.SECONDS).execute(() -> {
                        synchronized (client) {
                            if (clients.get(client.session) == client && client.subscriptions.get(key) == subscription) {
                                subscribe(client, resort, day);
                            }
                        }
                    });
                    return;
                }
                client.sender.send(response("subscribed", resort, day).toString());
                JsonObject json = snapshot.toSnapshotJson(); json.addProperty("serverId", serverId);
                client.sender.send(json.toString());
                subscription.version = snapshot.getVersion();
                subscription.initialized = true;
                if (subscription.pending != null) sendChange(client, subscription, subscription.pending);
                subscription.pending = null;
            }
        });
    }
    public void broadcast(LeaderboardUpdate update) {
        String key = update.getResortId() + ":" + update.getDayId();
        for (Client client : clients.values()) {
            synchronized (client) {
                Subscription subscription = client.subscriptions.get(key);
                if (subscription == null) continue;
                if (!subscription.initialized) {
                    if (subscription.pending == null || update.getVersion() > subscription.pending.getVersion()) subscription.pending = update;
                } else sendChange(client, subscription, update);
            }
        }
    }
    private void sendChange(Client client, Subscription subscription, LeaderboardUpdate update) {
        if (update.getVersion() <= subscription.version) return;
        JsonObject json = update.toJson(); json.addProperty("type", "leaderboard_update");
        json.addProperty("serverTimestamp", Instant.now().toString()); json.addProperty("serverId", serverId);
        client.sender.send(json.toString()); subscription.version = update.getVersion();
    }
    public void resynchronize() {
        for (Client client : clients.values()) {
            synchronized (client) {
                for (String key : new java.util.ArrayList<>(client.subscriptions.keySet())) {
                    String[] parts = key.split(":"); subscribe(client, parts[0], parts[1]);
                }
            }
        }
    }
    public void heartbeat() {
        for (Client client : clients.values()) {
            if (!client.session.isOpen()) { close(client.session); continue; }
            try { client.session.getAsyncRemote().sendPing(ByteBuffer.allocate(0)); }
            catch (Exception e) { client.sender.close("Connection failed"); close(client.session); }
        }
    }
    public void shutdown() {
        for (Client client : clients.values()) client.sender.close("Server restarting; reconnect");
        clients.clear();
    }
}
