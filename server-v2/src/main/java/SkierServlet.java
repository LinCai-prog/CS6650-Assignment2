
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

import model.LiftRide;
import model.ResponseMsg;
import model.SwipeEventMessage;

import rabbitmq.RabbitMQConfig;
import rabbitmq.RabbitMQManager;
import rabbitmq.ChannelPool;
import rabbitmq.SwipePublisher;

import util.RequestStats;
import util.LiftVerticalLookup;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SkierServlet extends HttpServlet {

    private static final Logger logger =
            Logger.getLogger(SkierServlet.class.getName());

    private static final String EVENT_ID_HEADER =
            "X-Event-ID";

    private final Gson gson = new Gson();

    private RabbitMQManager rabbitMQManager;
    private ChannelPool channelPool;
    private SwipePublisher swipePublisher;

    @Override
    public void init() throws ServletException {
        super.init();

        try {
            rabbitMQManager = new RabbitMQManager();

            logger.info(
                    "RabbitMQ Host: " + RabbitMQConfig.HOST
            );

            logger.info(
                    "RabbitMQ Port: " + RabbitMQConfig.PORT
            );

            logger.info(
                    "RabbitMQ User: " + RabbitMQConfig.USERNAME
            );

            logger.info(
                    "RabbitMQ Password configured: "
                            + (RabbitMQConfig.PASSWORD != null
                            && !RabbitMQConfig.PASSWORD.isEmpty())
            );

            channelPool = new ChannelPool(
                    rabbitMQManager,
                    RabbitMQConfig.CHANNEL_POOL_SIZE
            );

            channelPool.init();

            swipePublisher = new SwipePublisher(channelPool);

            logger.info("RabbitMQ publisher initialized.");

        } catch (Exception e) {

            logger.log(
                    Level.SEVERE,
                    "Failed to initialize RabbitMQ publisher",
                    e
            );

            if (channelPool != null) {
                channelPool.close();
            }

            if (rabbitMQManager != null) {
                rabbitMQManager.close();
            }

            throw new ServletException(
                    "RabbitMQ initialization failed",
                    e
            );
        }
    }

    @Override
    protected void doGet(
            HttpServletRequest req,
            HttpServletResponse res)
            throws ServletException, IOException {

        res.setContentType("application/json");
        res.setCharacterEncoding("UTF-8");

        String urlPath = req.getPathInfo();

        if ("/stats".equals(urlPath)) {
            res.setStatus(HttpServletResponse.SC_OK);

            res.getWriter().write(
                    gson.toJson(
                            new ResponseMsg(RequestStats.getStats())
                    )
            );

            return;
        }

        RequestStats.incrementGet();

        if (urlPath == null
                || urlPath.isEmpty()
                || urlPath.equals("/")) {

            res.setStatus(
                    HttpServletResponse.SC_NOT_FOUND
            );

            res.getWriter().write(
                    gson.toJson(
                            new ResponseMsg("missing parameters")
                    )
            );

            return;
        }

        String[] urlParts = urlPath.split("/");

        if (!isUrlValid(urlParts)) {
            res.setStatus(
                    HttpServletResponse.SC_BAD_REQUEST
            );

            res.getWriter().write(
                    gson.toJson(
                            new ResponseMsg("invalid url")
                    )
            );

            return;
        }

        res.setStatus(HttpServletResponse.SC_OK);

        String resortID = urlParts[1];
        String seasonID = urlParts[3];
        String dayID = urlParts[5];
        String skierID = urlParts[7];

        ResponseMsg response = new ResponseMsg(
                String.format(
                        "GET request processed for skier %s "
                                + "at resort %s, season %s, day %s",
                        skierID,
                        resortID,
                        seasonID,
                        dayID
                )
        );

        res.getWriter().write(gson.toJson(response));
    }

    @Override
    protected void doPost(
            HttpServletRequest req,
            HttpServletResponse res)
            throws ServletException, IOException {

        res.setContentType("application/json");
        res.setCharacterEncoding("UTF-8");

        RequestStats.incrementPost();

        String urlPath = req.getPathInfo();

        if (urlPath == null
                || urlPath.isEmpty()
                || urlPath.equals("/")) {

            res.setStatus(
                    HttpServletResponse.SC_NOT_FOUND
            );

            res.getWriter().write(
                    gson.toJson(
                            new ResponseMsg("missing parameters")
                    )
            );

            return;
        }

        String[] urlParts = urlPath.split("/");

        if (!isUrlValid(urlParts)) {

            res.setStatus(
                    HttpServletResponse.SC_BAD_REQUEST
            );

            res.getWriter().write(
                    gson.toJson(
                            new ResponseMsg("invalid url")
                    )
            );

            return;
        }

        // New: read stable event ID from HTTP header.
        String eventId = req.getHeader(EVENT_ID_HEADER);

        if (eventId != null) {

            eventId = eventId.trim();

            if (!isValidUuid(eventId)) {

                res.setStatus(
                        HttpServletResponse.SC_BAD_REQUEST
                );

                res.getWriter().write(
                        gson.toJson(
                                new ResponseMsg(
                                        "invalid X-Event-ID header"
                                )
                        )
                );

                return;
            }
        }

        try {
            StringBuilder jsonBody = new StringBuilder();

            try (BufferedReader reader = req.getReader()) {
                String line;

                while ((line = reader.readLine()) != null) {
                    jsonBody.append(line);
                }
            }

            LiftRide liftRide = gson.fromJson(
                    jsonBody.toString(),
                    LiftRide.class
            );

            if (liftRide == null || !isLiftRideValid(liftRide)) {

                res.setStatus(
                        HttpServletResponse.SC_BAD_REQUEST
                );

                res.getWriter().write(
                        gson.toJson(
                                new ResponseMsg("invalid JSON body")
                        )
                );

                return;
            }

            String resortID = urlParts[1];
            String seasonID = urlParts[3];
            String dayID = urlParts[5];
            String skierID = urlParts[7];

            int vertical = LiftVerticalLookup.getVertical(
                    liftRide.getLiftID()
            );

            // New: preserve client event ID across HTTP retries.
            // Without the header, generate a server-side UUID.
            SwipeEventMessage event = new SwipeEventMessage(
                    eventId,
                    resortID,
                    dayID,
                    skierID,
                    liftRide,
                    vertical,
                    System.getenv().getOrDefault(
                            "SERVER_ID",
                            "local-ingest-server"
                    )
            );

            try {
                swipePublisher.publish(event);

            } catch (InterruptedException e) {

                Thread.currentThread().interrupt();

                logger.log(
                        Level.WARNING,
                        "Swipe publishing interrupted",
                        e
                );

                res.setStatus(
                        HttpServletResponse.SC_SERVICE_UNAVAILABLE
                );

                res.getWriter().write(
                        gson.toJson(
                                new ResponseMsg(
                                        "message publishing interrupted"
                                )
                        )
                );

                return;

            } catch (Exception e) {

                logger.log(
                        Level.SEVERE,
                        "Failed to publish swipe event",
                        e
                );

                res.setStatus(
                        HttpServletResponse.SC_SERVICE_UNAVAILABLE
                );

                res.getWriter().write(
                        gson.toJson(
                                new ResponseMsg(
                                        "message publishing failed"
                                )
                        )
                );

                return;
            }

            res.setStatus(
                    HttpServletResponse.SC_CREATED
            );

            ResponseMsg response = new ResponseMsg(
                    String.format(
                            "Lift ride accepted: skier %s, "
                                    + "resort %s, season %s, day %s, "
                                    + "lift %d, vertical %d",
                            skierID,
                            resortID,
                            seasonID,
                            dayID,
                            liftRide.getLiftID(),
                            vertical
                    )
            );

            res.getWriter().write(gson.toJson(response));

        } catch (JsonSyntaxException e) {

            res.setStatus(
                    HttpServletResponse.SC_BAD_REQUEST
            );

            res.getWriter().write(
                    gson.toJson(
                            new ResponseMsg("invalid JSON body")
                    )
            );
        }
    }

    private boolean isValidUuid(String value) {
        try {
            UUID parsed = UUID.fromString(value);
            return parsed.toString().equalsIgnoreCase(value);

        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    private boolean isUrlValid(String[] urlParts) {

        if (urlParts.length != 8) {
            return false;
        }

        try {
            if (!urlParts[2].equals("seasons")
                    || !urlParts[4].equals("days")
                    || !urlParts[6].equals("skiers")) {
                return false;
            }

            int resortID = Integer.parseInt(urlParts[1]);
            int seasonID = Integer.parseInt(urlParts[3]);
            int dayID = Integer.parseInt(urlParts[5]);
            int skierID = Integer.parseInt(urlParts[7]);

            return resortID >= 1
                    && resortID <= RabbitMQConfig.RESORT_COUNT
                    && seasonID > 0
                    && dayID > 0
                    && skierID > 0;

        } catch (NumberFormatException e) {
            return false;
        }
    }

    private boolean isLiftRideValid(LiftRide liftRide) {
        return liftRide.getTime() >= 1
                && liftRide.getTime() <= 360
                && liftRide.getLiftID() >= 1
                && liftRide.getLiftID() <= 40;
    }

    @Override
    public void destroy() {

        logger.info("Shutting down RabbitMQ publisher.");

        if (channelPool != null) {
            channelPool.close();
        }

        if (rabbitMQManager != null) {
            rabbitMQManager.close();
        }

        super.destroy();
    }
}
