package websocket;

import com.google.gson.JsonParser;
import model.LeaderboardUpdate;
import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import storage.LeaderboardStore;
import java.net.InetSocketAddress;
import java.util.AbstractMap;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class LeaderboardWebSocketServerTest {
    private LeaderboardUpdate change(String resort, String day, long version, long total) {
        return new LeaderboardUpdate("event-" + version, resort, day, version, null, null,
                List.of(new AbstractMap.SimpleImmutableEntry<>("1234", total)));
    }

    private LeaderboardStore store() throws Exception {
        var store = mock(LeaderboardStore.class);
        when(store.getSnapshot(anyString(), anyString())).thenAnswer(inv ->
                new LeaderboardUpdate("snapshot", inv.getArgument(0), inv.getArgument(1),
                        0, null, null, List.of()));
        return store;
    }

    private WebSocket subscribe(LeaderboardWebSocketServer server, String resort, String day) {
        WebSocket conn = mock(WebSocket.class);
        when(conn.isOpen()).thenReturn(true);
        server.onOpen(conn, mock(ClientHandshake.class));
        server.onMessage(conn, "{\"action\":\"subscribe\",\"resortId\":\""
                + resort + "\",\"dayId\":\"" + day + "\"}");
        clearInvocations(conn);
        return conn;
    }

    @Test
    void shouldBroadcastOnlyToMatchingSubscribersWithoutReadingDatabase() throws Exception {
        var store = store();
        var server = new LeaderboardWebSocketServer(new InetSocketAddress("127.0.0.1", 0), store);
        var alice = subscribe(server, "3", "1");
        var bob = subscribe(server, "5", "2");
        clearInvocations(store);
        server.broadcastLeaderboardUpdate(change("3", "1", 1, 800));
        var message = ArgumentCaptor.forClass(String.class);
        verify(alice).send(message.capture());
        verifyNoInteractions(bob, store);
        var json = JsonParser.parseString(message.getValue()).getAsJsonObject();
        assertEquals("leaderboard_update", json.get("type").getAsString());
        assertEquals(800, json.getAsJsonArray("top10").get(0).getAsJsonObject().get("totalVertical").getAsLong());
        assertEquals(1, json.get("version").getAsLong());
        assertTrue(json.has("serverTimestamp"));
    }

    @Test
    void shouldBroadcastSameCapturedStandingsToAllMatchingSubscribers() throws Exception {
        var server = new LeaderboardWebSocketServer(new InetSocketAddress("127.0.0.1", 0), store());
        var alice = subscribe(server, "3", "1");
        var bob = subscribe(server, "3", "1");
        server.broadcastLeaderboardUpdate(change("3", "1", 1, 800));
        var a = ArgumentCaptor.forClass(String.class);
        var b = ArgumentCaptor.forClass(String.class);
        verify(alice).send(a.capture());
        verify(bob).send(b.capture());
        assertEquals(a.getValue(), b.getValue());
    }

    @Test
    void shouldPreserveDelayedStandingsAndSuppressDuplicateOrOlderVersions() throws Exception {
        var server = new LeaderboardWebSocketServer(new InetSocketAddress("127.0.0.1", 0), store());
        var conn = subscribe(server, "3", "1");
        server.broadcastLeaderboardUpdate(change("3", "1", 1, 100));
        server.broadcastLeaderboardUpdate(change("3", "1", 2, 200));
        server.broadcastLeaderboardUpdate(change("3", "1", 2, 200));
        server.broadcastLeaderboardUpdate(change("3", "1", 1, 100));
        var messages = ArgumentCaptor.forClass(String.class);
        verify(conn, times(2)).send(messages.capture());
        for (int i = 0; i < 2; i++) {
            var json = JsonParser.parseString(messages.getAllValues().get(i)).getAsJsonObject();
            assertEquals((i + 1) * 100, json.getAsJsonArray("top10").get(0).getAsJsonObject().get("totalVertical").getAsLong());
        }
    }

    @Test
    void shouldNotSendQueuedChangesOlderThanInitialSnapshot() throws Exception {
        var store = store();
        when(store.getSnapshot("3", "1")).thenReturn(change("3", "1", 5, 500));
        var server = new LeaderboardWebSocketServer(new InetSocketAddress("127.0.0.1", 0), store);
        var conn = subscribe(server, "3", "1");
        server.broadcastLeaderboardUpdate(change("3", "1", 4, 400));
        server.broadcastLeaderboardUpdate(change("3", "1", 5, 500));
        verify(conn, never()).send(anyString());
        server.broadcastLeaderboardUpdate(change("3", "1", 6, 600));
        verify(conn).send(anyString());
    }

    @Test
    void shouldNotBroadcastAfterUnsubscribe() throws Exception {
        var server = new LeaderboardWebSocketServer(new InetSocketAddress("127.0.0.1", 0), store());
        var conn = subscribe(server, "3", "1");
        server.onMessage(conn, "{\"action\":\"unsubscribe\",\"resortId\":\"3\",\"dayId\":\"1\"}");
        clearInvocations(conn);
        server.broadcastLeaderboardUpdate(change("3", "1", 1, 800));
        verifyNoInteractions(conn);
    }

    @Test
    void shouldRetryFailedSubscriberWithoutRepeatingSuccessfulDelivery() throws Exception {
        var server = new LeaderboardWebSocketServer(new InetSocketAddress("127.0.0.1", 0), store());
        var alice = subscribe(server, "3", "1");
        var bob = subscribe(server, "3", "1");
        doThrow(new IllegalStateException("temporary send failure")).doNothing().when(bob).send(anyString());
        var update = change("3", "1", 1, 800);
        assertThrows(java.io.IOException.class, () -> server.broadcastLeaderboardUpdate(update));
        server.broadcastLeaderboardUpdate(update);
        verify(alice).send(anyString());
        verify(bob, times(2)).send(anyString());
    }

    @Test
    void shouldRemoveAllSubscriptionsAfterDisconnect() throws Exception {
        var server = new LeaderboardWebSocketServer(new InetSocketAddress("127.0.0.1", 0), store());
        var conn = subscribe(server, "3", "1");
        server.onMessage(conn, "{\"action\":\"subscribe\",\"resortId\":\"5\",\"dayId\":\"2\"}");
        server.onClose(conn, 1000, "test", true);
        clearInvocations(conn);
        server.broadcastLeaderboardUpdate(change("3", "1", 1, 800));
        server.broadcastLeaderboardUpdate(change("5", "2", 1, 800));
        verifyNoInteractions(conn);
        assertTrue(server.getSubscriptionManager().getSubscribers(new SubscriptionKey("3", "1")).isEmpty());
        assertTrue(server.getSubscriptionManager().getSubscribers(new SubscriptionKey("5", "2")).isEmpty());
    }
}
