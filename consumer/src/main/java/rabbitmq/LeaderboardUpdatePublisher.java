package rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import model.LeaderboardUpdate;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.Callable;

public class LeaderboardUpdatePublisher implements AutoCloseable {
    public static final String EXCHANGE_NAME = "skitrak.leaderboard.updates";
    private final Callable<Channel> channelFactory;
    private Channel channel;

    public LeaderboardUpdatePublisher(Channel channel) throws Exception {
        this(() -> channel);
    }

    public LeaderboardUpdatePublisher(Callable<Channel> channelFactory) throws Exception {
        this.channelFactory = Objects.requireNonNull(channelFactory);
        openChannel();
    }

    private void openChannel() throws Exception {
        channel = channelFactory.call();
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC, true);
        channel.confirmSelect();
    }

    public synchronized void publish(LeaderboardUpdate update) throws Exception {
        Objects.requireNonNull(update);
        var properties = new AMQP.BasicProperties.Builder()
                .contentType("application/json").deliveryMode(2)
                .messageId(update.getEventId()).build();
        byte[] body = update.toJson().toString().getBytes(StandardCharsets.UTF_8);
        try {
            if (channel == null || !channel.isOpen()) openChannel();
            channel.basicPublish(EXCHANGE_NAME,
                    "leaderboard." + update.getResortId() + "." + update.getDayId(),
                    false, properties, body);
            channel.waitForConfirmsOrDie(5000);
        } catch (Exception e) {
            try { close(); } catch (Exception closeError) { e.addSuppressed(closeError); }
            throw e;
        }
    }

    @Override
    public synchronized void close() throws Exception {
        Channel previous = channel;
        channel = null;
        if (previous != null && previous.isOpen()) previous.close();
    }
}
