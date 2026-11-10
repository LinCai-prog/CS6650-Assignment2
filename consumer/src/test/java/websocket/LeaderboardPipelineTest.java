package websocket;

import com.google.gson.Gson;
import model.SwipeEvent;
import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import rabbitmq.LeaderboardOutboxDispatcher;
import rabbitmq.LeaderboardUpdatePublisher;
import storage.SqliteLeaderboardStore;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class LeaderboardPipelineTest {
    @TempDir Path temp;

    private SwipeEvent event(String id, String skier, int vertical) {
        return new Gson().fromJson("{\"eventId\":\"" + id + "\",\"resortID\":\"3\",\"dayID\":\"1\","
                + "\"skierID\":\"" + skier + "\",\"vertical\":" + vertical + "}", SwipeEvent.class);
    }

    @Test
    void shouldDeliverElevenChangedStandingsForTwelveUniqueSwipesAndOneReplay() throws Exception {
        try (var store = new SqliteLeaderboardStore(temp.resolve("pipeline.db").toString())) {
            var server = new LeaderboardWebSocketServer(new InetSocketAddress("127.0.0.1", 0), store);
            WebSocket conn = mock(WebSocket.class);
            when(conn.isOpen()).thenReturn(true);
            server.onOpen(conn, mock(ClientHandshake.class));
            server.onMessage(conn, "{\"action\":\"subscribe\",\"resortId\":\"3\",\"dayId\":\"1\"}");
            clearInvocations(conn);
            for (int i = 0; i < 10; i++) store.processEvent(event("seed" + i, "skier" + i, 1000 + i * 100));
            var outsider = event("outside", "outsider", 100);
            store.processEvent(outsider);
            assertFalse(store.processEvent(outsider));
            store.processEvent(event("leader", "skier9", 100));
            var publisher = mock(LeaderboardUpdatePublisher.class);
            doAnswer(inv -> { server.broadcastLeaderboardUpdate(inv.getArgument(0)); return null; })
                    .when(publisher).publish(any());
            try (var dispatcher = new LeaderboardOutboxDispatcher(store, publisher)) {
                dispatcher.dispatchPending();
                assertTrue(store.getPendingUpdates(100).isEmpty());
            }
            verify(conn, times(11)).send(anyString());
            assertEquals(100, store.getTotalVertical("3", "1", "outsider"));
            assertEquals(2000, store.getTotalVertical("3", "1", "skier9"));
            assertTrue(store.getMetricsSummary().contains("Processed swipes=12, duplicates=1, leaderboard changes=11"));
        }
    }
}
