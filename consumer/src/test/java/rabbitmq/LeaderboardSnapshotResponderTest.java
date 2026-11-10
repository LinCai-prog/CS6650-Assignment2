package rabbitmq;
import com.google.gson.JsonParser;
import com.rabbitmq.client.*;
import model.LeaderboardUpdate;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import storage.LeaderboardStore;
import java.nio.charset.StandardCharsets;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class LeaderboardSnapshotResponderTest {
 @Test void shouldReturnAuthoritativeSnapshotIncludingVersionZero() throws Exception {
  Channel channel=mock(Channel.class);LeaderboardStore store=mock(LeaderboardStore.class);
  when(store.getSnapshot("3","1")).thenReturn(new LeaderboardUpdate("snapshot","3","1",0,null,null,List.of()));
  new LeaderboardSnapshotResponder(channel,store).start();
  var callback=ArgumentCaptor.forClass(DeliverCallback.class);
  verify(channel).basicConsume(eq(LeaderboardSnapshotResponder.QUEUE_NAME),eq(false),callback.capture(),any(CancelCallback.class));
  var props=new AMQP.BasicProperties.Builder().replyTo("reply").correlationId("request-id").build();
  callback.getValue().handle("tag",new Delivery(new Envelope(1,false,"",""),props,
   "{\"resortId\":\"3\",\"dayId\":\"1\"}".getBytes(StandardCharsets.UTF_8)));
  var bytes=ArgumentCaptor.forClass(byte[].class);var replyProps=ArgumentCaptor.forClass(AMQP.BasicProperties.class);
  verify(channel).basicPublish(eq(""),eq("reply"),replyProps.capture(),bytes.capture());
  assertEquals("request-id",replyProps.getValue().getCorrelationId());
  var json=JsonParser.parseString(new String(bytes.getValue(),StandardCharsets.UTF_8)).getAsJsonObject();
  assertEquals(0,LeaderboardUpdate.fromSnapshot(json).getVersion());verify(channel).basicAck(1,false);
 }
 @Test void shouldRetryTransientReadFailureAndRejectMalformedRequests() throws Exception {
  Channel channel=mock(Channel.class);when(channel.isOpen()).thenReturn(true);
  LeaderboardStore store=mock(LeaderboardStore.class);when(store.getSnapshot("3","1")).thenThrow(new java.sql.SQLException("busy"));
  new LeaderboardSnapshotResponder(channel,store).start();var callback=ArgumentCaptor.forClass(DeliverCallback.class);
  verify(channel).basicConsume(anyString(),eq(false),callback.capture(),any(CancelCallback.class));
  var props=new AMQP.BasicProperties.Builder().replyTo("reply").correlationId("request-id").build();
  callback.getValue().handle("tag",new Delivery(new Envelope(1,false,"",""),props,
    "{\"resortId\":\"3\",\"dayId\":\"1\"}".getBytes(StandardCharsets.UTF_8)));
  verify(channel).basicNack(1,false,true);
  callback.getValue().handle("tag",new Delivery(new Envelope(2,false,"",""),props,"{}".getBytes(StandardCharsets.UTF_8)));
  verify(channel).basicNack(2,false,false);verify(store,times(1)).getSnapshot(anyString(),anyString());
 }
}
