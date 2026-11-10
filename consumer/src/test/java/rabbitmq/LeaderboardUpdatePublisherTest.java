package rabbitmq;

import com.google.gson.JsonParser;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import model.LeaderboardUpdate;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class LeaderboardUpdatePublisherTest {
    private LeaderboardUpdate update() {
        return new LeaderboardUpdate("event-001", "3", "1", 2,
                "2026-11-09T20:00:00Z", "2026-11-09T20:00:00.020Z",
                List.of(new AbstractMap.SimpleImmutableEntry<>("1234", 800L)));
    }

    @Test
    void shouldPublishCapturedStandingsAndWaitForConfirmation() throws Exception {
        Channel channel = mock(Channel.class);
        when(channel.isOpen()).thenReturn(true);
        var publisher = new LeaderboardUpdatePublisher(channel);
        verify(channel).exchangeDeclare("skitrak.leaderboard.updates", BuiltinExchangeType.TOPIC, true);
        verify(channel).confirmSelect();
        publisher.publish(update());
        var body = ArgumentCaptor.forClass(byte[].class);
        var props = ArgumentCaptor.forClass(AMQP.BasicProperties.class);
        verify(channel).basicPublish(eq("skitrak.leaderboard.updates"), eq("leaderboard.3.1"),
                eq(false), props.capture(), body.capture());
        var message = JsonParser.parseString(new String(body.getValue(), StandardCharsets.UTF_8)).getAsJsonObject();
        assertEquals(update().toJson(), message);
        assertEquals(2, props.getValue().getDeliveryMode());
        assertEquals("event-001", props.getValue().getMessageId());
        verify(channel).waitForConfirmsOrDie(5000);
    }

    @Test
    void shouldRecreateChannelAfterUncertainPublication() throws Exception {
        Channel first = mock(Channel.class);
        Channel second = mock(Channel.class);
        when(first.isOpen()).thenReturn(true);
        when(second.isOpen()).thenReturn(true);
        doThrow(new java.io.IOException("confirm lost")).when(first).waitForConfirmsOrDie(5000);
        java.util.concurrent.Callable<Channel> factory = mock(java.util.concurrent.Callable.class);
        when(factory.call()).thenReturn(first, second);
        var publisher = new LeaderboardUpdatePublisher(factory);
        assertThrows(java.io.IOException.class, () -> publisher.publish(update()));
        publisher.publish(update());
        verify(first).close();
        verify(second).confirmSelect();
        verify(second).waitForConfirmsOrDie(5000);
    }
}
