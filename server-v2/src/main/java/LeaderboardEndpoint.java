import leaderboard.LeaderboardLifecycle;
import leaderboard.LeaderboardSessions;
import javax.websocket.*;
import javax.websocket.server.ServerEndpoint;

/** The ALB routes this upgrade to an ingest host; subscriptions stay local there. */
@ServerEndpoint("/leaderboard")
public class LeaderboardEndpoint {
    private LeaderboardSessions sessions;
    @OnOpen
    public void onOpen(Session session) throws java.io.IOException {
        try { sessions = LeaderboardLifecycle.sessions(); sessions.open(session); }
        catch (IllegalStateException unavailable) {
            session.close(new CloseReason(CloseReason.CloseCodes.TRY_AGAIN_LATER, "Leaderboard service not ready"));
        }
    }
    @OnMessage
    public void onMessage(Session session, String message) { if (sessions != null) sessions.message(session, message); }
    @OnClose
    public void onClose(Session session) { if (sessions != null) sessions.close(session); }
    @OnError
    public void onError(Session session, Throwable error) {
        if (sessions != null) sessions.close(session);
        try { if (session.isOpen()) session.close(); } catch (Exception ignored) { }
    }
}
