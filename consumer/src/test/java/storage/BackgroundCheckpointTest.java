package storage;

import com.google.gson.Gson;
import model.SwipeEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

class BackgroundCheckpointTest {
    @TempDir Path temp;
    @Test void concurrentCheckpointsAndSnapshotsSeeOnlyCompleteDurableBatches() throws Exception {
        String path=temp.resolve("checkpoint.db").toString();var gson=new Gson();
        try(var writer=new SqliteLeaderboardStore(path,65536,16384,1);
            var reader=new SqliteLeaderboardStore(path)) {
            var pool=Executors.newSingleThreadExecutor();
            try {
                var task=pool.submit(()->{
                    for(int batch=0;batch<40;batch++) {
                        List<SwipeEvent> events=new ArrayList<>();
                        for(int i=0;i<25;i++) events.add(gson.fromJson("{\"eventId\":\""+batch+"-"+i+"\",\"resortID\":\"1\",\"dayID\":\"1\",\"skierID\":\"42\",\"vertical\":100,\"timestamp\":\""+Instant.now()+"\"}",SwipeEvent.class));
                        writer.processBatch(events);
                    }
                    return null;
                });
                do {
                    var snapshot=reader.getSnapshot("1","1");
                    assertEquals(0,snapshot.getVersion()%25,"partial transaction was visible");
                    if(snapshot.getVersion()>0) assertEquals(snapshot.getVersion()*100,snapshot.getStandings().get(0).getValue());
                } while(!task.isDone());
                task.get(10,TimeUnit.SECONDS);
                assertEquals(1000,reader.getSnapshot("1","1").getVersion());
                assertEquals(1000,writer.getPendingUpdates(2000).size());
                assertTrue(writer.getMetricsSummary().contains("checkpoint errors=0"));
            } finally {pool.shutdownNow();}
        }
        try(var reopened=new SqliteLeaderboardStore(path)) {
            assertEquals(100000,reopened.getTotalVertical("1","1","42"));
            assertEquals(1000,reopened.getSnapshot("1","1").getVersion());
        }
    }
}
