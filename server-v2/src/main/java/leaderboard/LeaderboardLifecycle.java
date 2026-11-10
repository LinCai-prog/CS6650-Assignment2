package leaderboard;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;
import java.net.InetAddress;
import java.util.concurrent.*;

@WebListener
public final class LeaderboardLifecycle implements ServletContextListener {
    private static volatile LeaderboardLifecycle current;
    private RabbitLeaderboardBackplane backplane;
    private LeaderboardSessions sessions;
    private ScheduledExecutorService heartbeat;
    private String serverId;

    @Override
    public void contextInitialized(ServletContextEvent event) {
        try { serverId = System.getenv().getOrDefault("SERVER_ID", "ingest") + "@" + InetAddress.getLocalHost().getHostName(); }
        catch (Exception e) { serverId = "ingest"; }
        backplane = new RabbitLeaderboardBackplane(update -> sessions.broadcast(update), () -> sessions.resynchronize());
        sessions = new LeaderboardSessions(backplane, serverId);
        heartbeat = Executors.newSingleThreadScheduledExecutor();
        current = this;
        backplane.start();
        heartbeat.scheduleWithFixedDelay(sessions::heartbeat, 25, 25, TimeUnit.SECONDS);
    }
    public static LeaderboardSessions sessions() {
        LeaderboardLifecycle value = current;
        if (value == null || !value.backplane.isReady()) throw new IllegalStateException("Leaderboard service not ready");
        return value.sessions;
    }
    public static boolean isReady() { return current != null && current.backplane.isReady(); }
    public static String serverId() { return current == null ? "starting" : current.serverId; }
    @Override
    public void contextDestroyed(ServletContextEvent event) {
        current = null;
        if (heartbeat != null) heartbeat.shutdownNow();
        if (backplane != null) backplane.close();
        if (sessions != null) sessions.shutdown();
    }
}
