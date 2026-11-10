package storage;

import com.google.gson.Gson;
import model.SwipeEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class BatchTransactionTest {
    @TempDir Path temp;
    private SwipeEvent event(String id, String resort, int vertical) {
        var event = new Gson().fromJson("{\"eventId\":\""+id+"\",\"resortID\":\""+resort+"\",\"dayID\":\"1\",\"skierID\":\"42\",\"vertical\":"+vertical+",\"timestamp\":\"2026-11-10T01:00:00Z\"}", SwipeEvent.class);
        event.markReceived(java.time.Instant.now(), System.nanoTime());
        return event;
    }
    @Test void commitsEveryIntermediateChangeAndDeduplicatesWithinBatchAndAfterRestart() throws Exception {
        String path = temp.resolve("batch.db").toString();
        var first=event("one","1",100);var second=event("two","1",200);var other=event("other","2",300);
        try (var store=new SqliteLeaderboardStore(path)) {
            assertEquals(List.of(true,true,false,true),store.processBatch(List.of(first,second,first,other)));
            assertTrue(store.getMetricsSummary().contains("receive-to-sql samples=3"));
            assertEquals(300,store.getTotalVertical("1","1","42"));
            var updates=store.getPendingUpdates(10);
            assertEquals(3,updates.size());
            assertEquals(100L,updates.get(0).getStandings().get(0).getValue());
            assertEquals(300L,updates.get(1).getStandings().get(0).getValue());
            assertEquals(2,updates.get(1).getVersion());
        }
        try (var store=new SqliteLeaderboardStore(path)) {
            assertEquals(List.of(false,false,false),store.processBatch(List.of(first,second,other)));
            assertTrue(store.getMetricsSummary().contains("receive-to-sql samples=0"));
            assertEquals(3,store.getPendingUpdates(10).size());
            assertEquals(2,store.getSnapshot("1","1").getVersion());
        }
    }
    @Test void failureInLaterEventRollsBackEntireBatchIncludingMetricsAndCanRetry() throws Exception {
        String path=temp.resolve("rollback.db").toString();
        try (var store=new SqliteLeaderboardStore(path);var db=DriverManager.getConnection("jdbc:sqlite:"+path)) {
            db.createStatement().execute("CREATE TRIGGER fail_second BEFORE INSERT ON leaderboard_outbox WHEN NEW.resort_id='2' BEGIN SELECT RAISE(ABORT,'injected'); END");
            var batch=List.of(event("one","1",100),event("two","2",200));
            assertThrows(java.sql.SQLException.class,()->store.processBatch(batch));
            assertEquals(0,store.getTotalVertical("1","1","42"));
            assertEquals(0,store.getSnapshot("1","1").getVersion());
            assertTrue(store.getPendingUpdates(10).isEmpty());
            assertTrue(store.getMetricsSummary().contains("Processed swipes=0,"));
            assertTrue(store.getMetricsSummary().contains("source-to-receive samples=0"));
            assertTrue(store.getMetricsSummary().contains("receive-to-sql samples=0"));
            db.createStatement().execute("DROP TRIGGER fail_second");
            assertEquals(List.of(true,true),store.processBatch(batch));
            assertTrue(store.getMetricsSummary().contains("receive-to-sql samples=2"));
            assertEquals(2,store.getPendingUpdates(10).size());
        }
    }
}
