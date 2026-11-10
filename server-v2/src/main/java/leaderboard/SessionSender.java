package leaderboard;

import javax.websocket.CloseReason;
import javax.websocket.Session;
import java.util.ArrayDeque;

/** Serialize asynchronous sends and disconnect a client that cannot keep up. */
final class SessionSender {
    private final Session session;
    private final ArrayDeque<String> queue = new ArrayDeque<>();
    private boolean sending;
    private boolean closed;

    SessionSender(Session session) { this.session = session; }

    synchronized void send(String json) {
        if (closed || !session.isOpen()) return;
        if (queue.size() >= 256) { close("Subscriber is too slow; reconnect for a snapshot"); return; }
        queue.add(json);
        if (!sending) { sending = true; sendHead(); }
    }

    private void sendHead() {
        try {
            session.getAsyncRemote().sendText(queue.peek(), result -> {
                synchronized (SessionSender.this) {
                    if (closed) return;
                    if (!result.isOK()) { close("Delivery failed; reconnect for a snapshot"); return; }
                    queue.poll();
                    if (queue.isEmpty()) sending = false;
                    else sendHead();
                }
            });
        } catch (RuntimeException e) { close("Delivery failed; reconnect for a snapshot"); }
    }

    synchronized void close(String reason) {
        closed = true;
        queue.clear();
        try { session.close(new CloseReason(CloseReason.CloseCodes.TRY_AGAIN_LATER, reason)); }
        catch (Exception ignored) { }
    }
}
