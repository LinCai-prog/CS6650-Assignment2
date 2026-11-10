package service;

import com.google.gson.Gson;
import model.SwipeEvent;
import org.junit.jupiter.api.Test;
import storage.LeaderboardStore;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;
import static org.junit.jupiter.api.Assertions.*;

class BatchingSwipeProcessorTest {
    private SwipeEvent event(String id,String resort) {
        return new Gson().fromJson("{\"eventId\":\""+id+"\",\"resortID\":\""+resort+"\"}",SwipeEvent.class);
    }
    @Test void failedBatchIsRetriedBeforeLaterEventsAndNoFutureCompletesBeforeCommit() throws Exception {
        var store=mock(LeaderboardStore.class);var firstEntered=new CountDownLatch(1);var release=new CountDownLatch(1);
        var calls=Collections.synchronizedList(new ArrayList<List<String>>());var attempt=new AtomicInteger();
        when(store.processBatch(anyList())).thenAnswer(inv->{
            List<SwipeEvent> batch=inv.getArgument(0);List<String> ids=new ArrayList<>();for(var e:batch)ids.add(e.getEventId());calls.add(ids);
            if(attempt.getAndIncrement()==0){firstEntered.countDown();assertTrue(release.await(5,TimeUnit.SECONDS));throw new java.sql.SQLException("transient");}
            return Collections.nCopies(batch.size(),true);
        });
        try(var writer=new BatchingSwipeProcessor(store,1,5,10)) {
            var first=writer.submit(event("one","1"));assertTrue(firstEntered.await(5,TimeUnit.SECONDS));
            var later=writer.submit(event("two","1"));assertFalse(first.isDone());assertFalse(later.isDone());
            release.countDown();assertTrue(first.get(5,TimeUnit.SECONDS));assertTrue(later.get(5,TimeUnit.SECONDS));
            assertEquals(List.of(List.of("one"),List.of("one"),List.of("two")),calls);
        } finally {release.countDown();}
    }
    @Test void sparseTrafficFlushesAndCloseDrainsAcceptedEvents() throws Exception {
        var store=mock(LeaderboardStore.class);
        when(store.processBatch(anyList())).thenAnswer(inv->Collections.nCopies(((List<?>)inv.getArgument(0)).size(),true));
        var writer=new BatchingSwipeProcessor(store,50,5,100);
        var one=writer.submit(event("one","1"));assertTrue(one.get(2,TimeUnit.SECONDS));
        var two=writer.submit(event("two","1"));writer.close();assertTrue(two.get());
        assertThrows(RejectedExecutionException.class,()->writer.submit(event("late","1")));
    }
    @Test void admissionIsBoundedWhileDatabaseIsBlocked() throws Exception {
        var store=mock(LeaderboardStore.class);var entered=new CountDownLatch(1);var release=new CountDownLatch(1);
        when(store.processBatch(anyList())).thenAnswer(inv->{entered.countDown();assertTrue(release.await(5,TimeUnit.SECONDS));return List.of(true);});
        try(var writer=new BatchingSwipeProcessor(store,1,1,1)) {
            writer.submit(event("one","1"));assertTrue(entered.await(5,TimeUnit.SECONDS));writer.submit(event("two","2"));
            assertThrows(RejectedExecutionException.class,()->writer.submit(event("three","3")));release.countDown();
        } finally {release.countDown();}
    }
}
