
package rabbitmq;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class LeaderboardUpdatePublisherTest {

    @Test
    void shouldPublishLeaderboardChangeNotification()
            throws Exception {

        Channel channel = mock(Channel.class);

        LeaderboardUpdatePublisher publisher =
                new LeaderboardUpdatePublisher(channel);

        verify(channel).exchangeDeclare(
                "skitrak.leaderboard.updates",
                BuiltinExchangeType.TOPIC,
                true
        );

        publisher.publish(
                "event-001",
                "3",
                "1"
        );

        ArgumentCaptor<byte[]> bodyCaptor =
                ArgumentCaptor.forClass(byte[].class);

        ArgumentCaptor<AMQP.BasicProperties> propsCaptor =
                ArgumentCaptor.forClass(
                        AMQP.BasicProperties.class
                );

        verify(channel).basicPublish(
                eq("skitrak.leaderboard.updates"),
                eq("leaderboard.3.1"),
                eq(false),
                propsCaptor.capture(),
                bodyCaptor.capture()
        );

        String json = new String(
                bodyCaptor.getValue(),
                StandardCharsets.UTF_8
        );

        JsonObject message =
                JsonParser.parseString(json).getAsJsonObject();

        assertEquals(
                "leaderboard_changed",
                message.get("type").getAsString()
        );

        assertEquals(
                "event-001",
                message.get("eventId").getAsString()
        );

        assertEquals(
                "3",
                message.get("resortId").getAsString()
        );

        assertEquals(
                "1",
                message.get("dayId").getAsString()
        );

        assertEquals(
                "application/json",
                propsCaptor.getValue().getContentType()
        );

        assertEquals(
                2,
                propsCaptor.getValue().getDeliveryMode()
        );
    }
}
