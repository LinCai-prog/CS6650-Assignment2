import com.google.gson.JsonParser;

import javax.websocket.*;
import javax.websocket.server.PathParam;
import javax.websocket.server.ServerEndpoint;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

@ServerEndpoint("/leaderboard/{resortID}")
public class LeaderboardEndpoint {

    // resortID -> sessions subscribed to that resort
    private static final Map<String, Set<Session>> subscribers = new ConcurrentHashMap<>();
    private static final ScheduledExecutorService pusher =
            Executors.newSingleThreadScheduledExecutor();

    static {
        // Dummy: every 500ms, push a synthetic top-N to each resort's subscribers.
        pusher.scheduleAtFixedRate(LeaderboardEndpoint::pushAll, 500, 500, TimeUnit.MILLISECONDS);
    }

    @OnOpen
    public void onOpen(Session session, @PathParam("resortID") String resortID) {
        subscribers.computeIfAbsent(resortID, k -> ConcurrentHashMap.newKeySet()).add(session);
    }

    @OnClose
    public void onClose(Session session, @PathParam("resortID") String resortID) {
        Optional.ofNullable(subscribers.get(resortID)).ifPresent(s -> s.remove(session));
    }

    @OnError
    public void onError(Session session, Throwable t) {
        // log, then clean up the session
    }

    private static void pushAll() {
        for (Map.Entry<String, Set<Session>> e : subscribers.entrySet()) {
            String json = syntheticLeaderboard(e.getKey()); // canned/random top-N + serverTimestamp
            for (Session s : e.getValue()) {
                if (s.isOpen()) s.getAsyncRemote().sendText(json);
            }
        }
    }

    private static String syntheticLeaderboard(String resortID){

        String serverTimestamp = Instant.now().toString();

        return JsonParser.parseString("{\n" +
                "  \"resortID\": \"" + resortID + "\",\n" +
                "  \"serverTimestamp\": \""+ serverTimestamp + "\",\n" +
                "  \"top\": [\n" +
                "    { \"rank\": 1, \"skierID\": 40213, \"vertical\": 44120 },\n" +
                "    { \"rank\": 2, \"skierID\": 11872, \"vertical\": 43880 }\n" +
                "  ]\n" +
                "}").toString();
    }
}