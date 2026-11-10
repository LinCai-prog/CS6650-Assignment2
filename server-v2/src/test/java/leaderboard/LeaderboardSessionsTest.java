package leaderboard;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import model.LeaderboardUpdate;
import org.junit.jupiter.api.Test;
import javax.websocket.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class LeaderboardSessionsTest {
    private LeaderboardUpdate state(long version, long total) {
        return new LeaderboardUpdate("event" + version, "3", "1", version, null, null,
                total == 0 ? List.of() : List.of(new AbstractMap.SimpleImmutableEntry<>("1234", total)));
    }
    private static class Client {
        final Session session = mock(Session.class);
        final RemoteEndpoint.Async remote = mock(RemoteEndpoint.Async.class);
        final List<JsonObject> frames = new ArrayList<>();
        Client() {
            when(session.isOpen()).thenReturn(true); when(session.getAsyncRemote()).thenReturn(remote);
            doAnswer(call -> {
                frames.add(JsonParser.parseString(call.getArgument(0)).getAsJsonObject());
                ((SendHandler)call.getArgument(1)).onResult(new SendResult()); return null;
            }).when(remote).sendText(anyString(), any(SendHandler.class));
        }
    }
    private void subscribe(LeaderboardSessions hub, Client client) {
        hub.open(client.session);
        hub.message(client.session, "{\"action\":\"subscribe\",\"resortId\":\"3\",\"dayId\":\"1\"}");
    }
    @Test void shouldReadAuthoritativeSnapshotAndIdentifyAcceptingHost() {
        var hub = new LeaderboardSessions((r,d) -> CompletableFuture.completedFuture(state(4,800)), "ingest-2");
        var client = new Client(); subscribe(hub, client);
        assertEquals(2, client.frames.size());
        assertEquals("subscribed", client.frames.get(0).get("type").getAsString());
        var snapshot = client.frames.get(1);
        assertEquals("leaderboard_snapshot", snapshot.get("type").getAsString());
        assertEquals("ingest-2", snapshot.get("serverId").getAsString());
        assertEquals(800, snapshot.getAsJsonArray("top10").get(0).getAsJsonObject().get("totalVertical").getAsLong());
    }
    @Test void shouldBufferNewerChangeWhileSnapshotIsInFlight() {
        var pending = new CompletableFuture<LeaderboardUpdate>();
        var hub = new LeaderboardSessions((r,d)->pending, "one"); var client = new Client();
        subscribe(hub, client); hub.broadcast(state(2,200));
        assertTrue(client.frames.isEmpty());
        pending.complete(state(1,100));
        assertEquals(3,client.frames.size());
        assertEquals("leaderboard_update",client.frames.get(2).get("type").getAsString());
        assertEquals(2,client.frames.get(2).get("version").getAsLong());
    }
    @Test void shouldNotSendBufferedChangesOlderThanSnapshot() {
        var pending = new CompletableFuture<LeaderboardUpdate>();
        var hub = new LeaderboardSessions((r,d)->pending, "one"); var client = new Client();
        subscribe(hub, client); hub.broadcast(state(2,200)); pending.complete(state(3,300));
        assertEquals(2,client.frames.size());
        hub.broadcast(state(3,300)); hub.broadcast(state(1,100));
        assertEquals(2,client.frames.size());
        hub.broadcast(state(4,400)); assertEquals(3,client.frames.size());
    }
    @Test void shouldBroadcastToAllLocalSubscribersAndKeepResortsSeparate() {
        var hub = new LeaderboardSessions((r,d)->CompletableFuture.completedFuture(
                new LeaderboardUpdate("snapshot",r,d,0,null,null,List.of())), "one");
        var a = new Client();var b = new Client();var other = new Client();
        subscribe(hub,a);subscribe(hub,b);hub.open(other.session);
        hub.message(other.session,"{\"action\":\"subscribe\",\"resortId\":\"5\",\"dayId\":\"1\"}");
        hub.broadcast(state(1,100));
        assertEquals(3,a.frames.size());assertEquals(3,b.frames.size());assertEquals(2,other.frames.size());
    }
    @Test void shouldDiscardSnapshotAfterUnsubscribeOrDisconnect() {
        var pending = new CompletableFuture<LeaderboardUpdate>();
        var hub = new LeaderboardSessions((r,d)->pending,"one");var client=new Client();subscribe(hub,client);
        hub.message(client.session,"{\"action\":\"unsubscribe\",\"resortId\":\"3\",\"dayId\":\"1\"}");
        pending.complete(state(1,100));hub.broadcast(state(2,200));
        assertEquals(1,client.frames.size());assertEquals("unsubscribed",client.frames.get(0).get("type").getAsString());
        hub.close(client.session);hub.broadcast(state(3,300));assertEquals(1,client.frames.size());
    }
    @Test void shouldReturnErrorWhenAuthoritativeSnapshotIsUnavailable() {
        var future = new CompletableFuture<LeaderboardUpdate>();future.completeExceptionally(new java.io.IOException());
        var hub = new LeaderboardSessions((r,d)->future,"one");var client=new Client();subscribe(hub,client);
        assertEquals("error",client.frames.get(0).get("type").getAsString());
        hub.broadcast(state(1,100));assertEquals(1,client.frames.size());
        hub.close(client.session);
    }
    @Test void shouldResynchronizeExistingClientsAfterBackplaneRecovery() {
        SnapshotClient snapshots=mock(SnapshotClient.class);
        when(snapshots.fetch("3","1")).thenReturn(CompletableFuture.completedFuture(state(1,100)), CompletableFuture.completedFuture(state(5,500)));
        var hub=new LeaderboardSessions(snapshots,"one");var client=new Client();subscribe(hub,client);
        hub.resynchronize();hub.broadcast(state(4,400));
        assertEquals(4,client.frames.size());assertEquals(5,client.frames.get(3).get("version").getAsLong());
    }
    @Test void shouldSerializeAsynchronousWritesAndBoundSlowSubscriberMemory() throws Exception {
        var session=mock(Session.class);var remote=mock(RemoteEndpoint.Async.class);
        when(session.isOpen()).thenReturn(true);when(session.getAsyncRemote()).thenReturn(remote);
        var sender=new SessionSender(session);sender.send("one");sender.send("two");
        var handler=org.mockito.ArgumentCaptor.forClass(SendHandler.class);
        verify(remote).sendText(eq("one"),handler.capture());
        verify(remote,never()).sendText(eq("two"),any(SendHandler.class));
        handler.getValue().onResult(new SendResult());verify(remote).sendText(eq("two"),any(SendHandler.class));
        for(int i=0;i<260;i++)sender.send("queued");
        verify(session,atLeastOnce()).close(any(CloseReason.class));
    }
}
