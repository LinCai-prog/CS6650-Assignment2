
package websocket;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;

import org.junit.jupiter.api.Test;

import org.mockito.ArgumentCaptor;
import storage.LeaderboardStore;

import java.net.InetSocketAddress;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class LeaderboardWebSocketServerTest {

    @Test
    void shouldBroadcastOnlyToMatchingSubscribers()
            throws Exception {

        // 1. Mock the SQLite store.
        LeaderboardStore store = mock(LeaderboardStore.class);

        when(store.getTopN("3", "1", 10))
                .thenReturn(List.of(
                        new AbstractMap.SimpleEntry<>("1234", 800L)
                ));

        when(store.getTopN("5", "2", 10))
                .thenReturn(List.of(
                        new AbstractMap.SimpleEntry<>("5678", 600L)
                ));

        // 2. Create WebSocket Server without starting a port.
        LeaderboardWebSocketServer server =
                new LeaderboardWebSocketServer(
                        new InetSocketAddress("127.0.0.1", 0),
                        store
                );

        // 3. Create two simulated WebSocket connections.
        WebSocket alice = mock(WebSocket.class);
        WebSocket bob = mock(WebSocket.class);

        when(alice.isOpen()).thenReturn(true);
        when(bob.isOpen()).thenReturn(true);

        ClientHandshake handshake = mock(ClientHandshake.class);

        // 4. Simulate both clients connecting.
        server.onOpen(alice, handshake);
        server.onOpen(bob, handshake);

        // 5. Alice subscribes to Resort 3 / Day 1.
        server.onMessage(
                alice,
                "{"
                        + "\"action\":\"subscribe\","
                        + "\"resortId\":\"3\","
                        + "\"dayId\":\"1\""
                        + "}"
        );

        // 6. Bob subscribes to Resort 5 / Day 2.
        server.onMessage(
                bob,
                "{"
                        + "\"action\":\"subscribe\","
                        + "\"resortId\":\"5\","
                        + "\"dayId\":\"2\""
                        + "}"
        );

        SubscriptionKey key3 =
                new SubscriptionKey("3", "1");

        SubscriptionKey key5 =
                new SubscriptionKey("5", "2");

        assertEquals(
                1,
                server.getSubscriptionManager()
                        .getSubscribers(key3).size()
        );

        assertEquals(
                1,
                server.getSubscriptionManager()
                        .getSubscribers(key5).size()
        );

        // 7. Ignore messages sent during subscription:
        // "subscribed" and "leaderboard_snapshot".
        clearInvocations(alice, bob);

        // 8. Broadcast a change for Resort 3 / Day 1.
        server.broadcastLeaderboardUpdate(key3);

        // 9. Alice should receive exactly one update.
        ArgumentCaptor<String> captor =
                ArgumentCaptor.forClass(String.class);

        verify(alice, times(1)).send(captor.capture());

        // Bob must receive no update.
        verify(bob, never()).send(anyString());

        // 10. Verify the actual JSON content.
        JsonObject update =
                JsonParser.parseString(
                        captor.getValue()
                ).getAsJsonObject();

        assertEquals(
                "leaderboard_update",
                update.get("type").getAsString()
        );

        assertEquals(
                "3",
                update.get("resortId").getAsString()
        );

        assertEquals(
                "1",
                update.get("dayId").getAsString()
        );

        JsonObject firstSkier =
                update.getAsJsonArray("top10")
                        .get(0).getAsJsonObject();

        assertEquals(
                "1234",
                firstSkier.get("skierId").getAsString()
        );

        assertEquals(
                800L,
                firstSkier.get("totalVertical").getAsLong()
        );

        // 11. Clean up simulated sessions.
        server.onClose(alice, 1000, "test complete", true);
        server.onClose(bob, 1000, "test complete", true);
    }

    @Test
    void shouldNotBroadcastAfterUnsubscribe()
            throws Exception {

        LeaderboardStore store = mock(LeaderboardStore.class);

        when(store.getTopN("3", "1", 10))
                .thenReturn(List.of(
                        new AbstractMap.SimpleEntry<>("1234", 800L)
                ));

        LeaderboardWebSocketServer server =
                new LeaderboardWebSocketServer(
                        new InetSocketAddress("127.0.0.1", 0),
                        store
                );

        WebSocket alice = mock(WebSocket.class);
        when(alice.isOpen()).thenReturn(true);

        ClientHandshake handshake = mock(ClientHandshake.class);

        server.onOpen(alice, handshake);

        SubscriptionKey key = new SubscriptionKey("3", "1");

        // Alice subscribes.
        server.onMessage(
                alice,
                "{\"action\":\"subscribe\","
                        + "\"resortId\":\"3\","
                        + "\"dayId\":\"1\"}"
        );

        assertEquals(
                1,
                server.getSubscriptionManager()
                        .getSubscribers(key).size()
        );

        // Alice unsubscribes without disconnecting.
        server.onMessage(
                alice,
                "{\"action\":\"unsubscribe\","
                        + "\"resortId\":\"3\","
                        + "\"dayId\":\"1\"}"
        );

        assertTrue(
                server.getSubscriptionManager()
                        .getSubscribers(key).isEmpty()
        );

        // Ignore earlier subscribed/snapshot/unsubscribed responses.
        clearInvocations(alice);

        // A new leaderboard update occurs.
        server.broadcastLeaderboardUpdate(key);

        // Alice should not receive it.
        verify(alice, never()).send(anyString());

        // Alice can still use the existing WebSocket connection.
        server.onMessage(
                alice,
                "{\"action\":\"subscribe\","
                        + "\"resortId\":\"3\","
                        + "\"dayId\":\"1\"}"
        );

        assertEquals(
                1,
                server.getSubscriptionManager()
                        .getSubscribers(key).size()
        );

        server.onClose(alice, 1000, "test complete", true);
    }


    @Test
    void shouldRemoveAllSubscriptionsAfterDisconnect()
            throws Exception {

        LeaderboardStore store = mock(LeaderboardStore.class);

        when(store.getTopN(anyString(), anyString(), eq(10)))
                .thenReturn(java.util.Collections.emptyList());

        LeaderboardWebSocketServer server =
                new LeaderboardWebSocketServer(
                        new InetSocketAddress("127.0.0.1", 0),
                        store
                );

        WebSocket alice = mock(WebSocket.class);
        when(alice.isOpen()).thenReturn(true);

        ClientHandshake handshake = mock(ClientHandshake.class);

        server.onOpen(alice, handshake);

        SubscriptionKey key3 = new SubscriptionKey("3", "1");
        SubscriptionKey key5 = new SubscriptionKey("5", "2");

        // Subscribe to two different leaderboards.
        server.onMessage(
                alice,
                "{\"action\":\"subscribe\","
                        + "\"resortId\":\"3\","
                        + "\"dayId\":\"1\"}"
        );

        server.onMessage(
                alice,
                "{\"action\":\"subscribe\","
                        + "\"resortId\":\"5\","
                        + "\"dayId\":\"2\"}"
        );

        String sessionId =
                server.getSubscriptionManager()
                        .getSubscribers(key3)
                        .iterator()
                        .next();

        assertEquals(
                2,
                server.getSubscriptionManager()
                        .getSubscriptions(sessionId).size()
        );

        // Simulate WebSocket disconnect.
        server.onClose(
                alice,
                1000,
                "Browser closed",
                true
        );

        // Both subscriptions must be removed.
        assertTrue(
                server.getSubscriptionManager()
                        .getSubscribers(key3).isEmpty()
        );

        assertTrue(
                server.getSubscriptionManager()
                        .getSubscribers(key5).isEmpty()
        );

        assertTrue(
                server.getSubscriptionManager()
                        .getSubscriptions(sessionId).isEmpty()
        );

        clearInvocations(alice);

        // Simulate leaderboard changes after disconnect.
        server.broadcastLeaderboardUpdate(key3);
        server.broadcastLeaderboardUpdate(key5);

        // No messages should be sent to the disconnected client.
        verify(alice, never()).send(anyString());
    }


    @Test
    void shouldBroadcastToAllMatchingSubscribers()
            throws Exception {

        LeaderboardStore store = mock(LeaderboardStore.class);

        when(store.getTopN("3", "1", 10))
                .thenReturn(List.of(
                        new AbstractMap.SimpleEntry<>("1234", 1000L)
                ));

        LeaderboardWebSocketServer server =
                new LeaderboardWebSocketServer(
                        new InetSocketAddress("127.0.0.1", 0),
                        store
                );

        WebSocket alice = mock(WebSocket.class);
        WebSocket bob = mock(WebSocket.class);

        when(alice.isOpen()).thenReturn(true);
        when(bob.isOpen()).thenReturn(true);

        ClientHandshake handshake = mock(ClientHandshake.class);

        // Both clients connect.
        server.onOpen(alice, handshake);
        server.onOpen(bob, handshake);

        String subscribe = "{"
                + "\"action\":\"subscribe\","
                + "\"resortId\":\"3\","
                + "\"dayId\":\"1\""
                + "}";

        // Both subscribe to the same leaderboard.
        server.onMessage(alice, subscribe);
        server.onMessage(bob, subscribe);

        SubscriptionKey key = new SubscriptionKey("3", "1");

        assertEquals(
                2,
                server.getSubscriptionManager()
                        .getSubscribers(key).size()
        );

        // Ignore subscribed and initial snapshot responses.
        clearInvocations(alice, bob);
        clearInvocations(store);

        // One leaderboard change occurs.
        server.broadcastLeaderboardUpdate(key);

        ArgumentCaptor<String> aliceCaptor =
                ArgumentCaptor.forClass(String.class);

        ArgumentCaptor<String> bobCaptor =
                ArgumentCaptor.forClass(String.class);

        // Each client receives exactly one update.
        verify(alice, times(1)).send(aliceCaptor.capture());
        verify(bob, times(1)).send(bobCaptor.capture());

        // Both clients receive the same leaderboard snapshot.
        JsonObject aliceUpdate =
                JsonParser.parseString(
                        aliceCaptor.getValue()
                ).getAsJsonObject();

        JsonObject bobUpdate =
                JsonParser.parseString(
                        bobCaptor.getValue()
                ).getAsJsonObject();

        assertEquals(aliceUpdate, bobUpdate);

        assertEquals(
                "leaderboard_update",
                aliceUpdate.get("type").getAsString()
        );

        assertEquals(
                1000L,
                aliceUpdate.getAsJsonArray("top10")
                        .get(0)
                        .getAsJsonObject()
                        .get("totalVertical")
                        .getAsLong()
        );

        // SQLite should be queried once for the broadcast.
        verify(store, times(1))
                .getTopN("3", "1", 10);

        // Clean up.
        server.onClose(alice, 1000, "test complete", true);
        server.onClose(bob, 1000, "test complete", true);
    }

}
