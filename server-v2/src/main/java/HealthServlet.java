import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class HealthServlet extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest req,
                         HttpServletResponse res)
            throws IOException {

        res.setContentType("application/json");
        res.setCharacterEncoding("UTF-8");

        boolean ready = leaderboard.LeaderboardLifecycle.isReady();
        res.setStatus(ready ? HttpServletResponse.SC_OK : HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        com.google.gson.JsonObject json = new com.google.gson.JsonObject();
        json.addProperty("status", ready ? "ok" : "starting");
        json.addProperty("leaderboardReady", ready);
        json.addProperty("serverId", leaderboard.LeaderboardLifecycle.serverId());
        res.getWriter().write(json.toString());
    }
}
