package rabbitmq;

import com.rabbitmq.client.*;
import concurrency.ResortSerialExecutor;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import service.BatchingSwipeProcessor;
import storage.LeaderboardStore;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;
import static org.junit.jupiter.api.Assertions.*;

class ResortConsumerBatchTest {
    @Test void productionConsumerAcknowledgesOnlyAfterBatchCommit() throws Exception {
        var channel=mock(Channel.class);when(channel.isOpen()).thenReturn(true);
        var store=mock(LeaderboardStore.class);var entered=new CountDownLatch(1);var commit=new CountDownLatch(1);
        when(store.processBatch(anyList())).thenAnswer(inv->{entered.countDown();assertTrue(commit.await(5,TimeUnit.SECONDS));return Collections.nCopies(((List<?>)inv.getArgument(0)).size(),true);});
        var pool=Executors.newFixedThreadPool(1);
        try(var writer=new BatchingSwipeProcessor(store,2,5,100);
            var consumer=new ResortConsumer(channel,writer,new ResortSerialExecutor(pool,50))) {
            consumer.start(1);var callback=ArgumentCaptor.forClass(DeliverCallback.class);
            verify(channel).basicConsume(eq("resort.1"),eq(false),callback.capture(),any(CancelCallback.class));
            String json="{\"eventId\":\"one\",\"resortID\":\"1\",\"dayID\":\"1\",\"skierID\":\"42\",\"liftID\":\"1\",\"vertical\":100,\"time\":100,\"timestamp\":\"2026-11-10T00:00:00Z\",\"serverId\":\"test\"}";
            callback.getValue().handle("consumer",new Delivery(new Envelope(1,false,"skitrak.exchange","resort.1"),new AMQP.BasicProperties(),json.getBytes(StandardCharsets.UTF_8)));
            assertTrue(entered.await(5,TimeUnit.SECONDS));verify(channel,never()).basicAck(anyLong(),anyBoolean());
            commit.countDown();verify(channel,timeout(2000)).basicAck(1,true);
            verify(channel,never()).basicNack(anyLong(),anyBoolean(),anyBoolean());
        } finally {commit.countDown();pool.shutdownNow();}
    }
}
