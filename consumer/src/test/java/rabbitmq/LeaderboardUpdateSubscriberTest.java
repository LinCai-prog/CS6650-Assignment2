package rabbitmq;

import com.rabbitmq.client.*;
import com.rabbitmq.client.impl.AMQImpl;
import model.LeaderboardUpdate;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import websocket.LeaderboardWebSocketServer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class LeaderboardUpdateSubscriberTest {
    @Test
    void shouldCreateSeparateQueueForEachWebSocketServer() throws Exception {
        Channel first = mock(Channel.class);
        Channel second = mock(Channel.class);
        when(first.queueDeclare()).thenReturn(new AMQImpl.Queue.DeclareOk("ws-one", 0, 0));
        when(second.queueDeclare()).thenReturn(new AMQImpl.Queue.DeclareOk("ws-two", 0, 0));
        var server = mock(LeaderboardWebSocketServer.class);
        new LeaderboardUpdateSubscriber(first, server).start();
        new LeaderboardUpdateSubscriber(second, server).start();
        verify(first).queueBind("ws-one", LeaderboardUpdatePublisher.EXCHANGE_NAME, "leaderboard.*.*");
        verify(second).queueBind("ws-two", LeaderboardUpdatePublisher.EXCHANGE_NAME, "leaderboard.*.*");
    }

    @Test
    void shouldForwardCapturedUpdateAndRejectLegacyNotifications() throws Exception {
        Channel channel = mock(Channel.class);
        when(channel.queueDeclare()).thenReturn(new AMQImpl.Queue.DeclareOk("ws-one", 0, 0));
        var server = mock(LeaderboardWebSocketServer.class);
        new LeaderboardUpdateSubscriber(channel, server).start();
        var callback = ArgumentCaptor.forClass(DeliverCallback.class);
        verify(channel).basicConsume(eq("ws-one"), eq(false), callback.capture(), any(CancelCallback.class));
        var update = new LeaderboardUpdate("event", "3", "1", 1, null, null, List.of());
        callback.getValue().handle("tag", new Delivery(new Envelope(1, false, "x", "k"),
                new AMQP.BasicProperties(), update.toJson().toString().getBytes(StandardCharsets.UTF_8)));
        var captured = ArgumentCaptor.forClass(LeaderboardUpdate.class);
        verify(server).broadcastLeaderboardUpdate(captured.capture());
        assertEquals(update.toJson(), captured.getValue().toJson());
        verify(channel).basicAck(1, false);
        callback.getValue().handle("tag", new Delivery(new Envelope(2, false, "x", "k"),
                new AMQP.BasicProperties(), "{\"type\":\"leaderboard_changed\"}".getBytes(StandardCharsets.UTF_8)));
        verify(channel).basicNack(2, false, false);
        verifyNoMoreInteractions(server);
    }
}
