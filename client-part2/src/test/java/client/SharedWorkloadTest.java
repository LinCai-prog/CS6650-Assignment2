package client;

import model.SwipeEvent;
import metrics.SwipeMetricsCollector;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public final class SharedWorkloadTest {
    public static void main(String[] args) throws Exception {
        BlockingQueue<SwipeEvent> queue=new ArrayBlockingQueue<>(1000);
        Set<String> expected=new HashSet<>();
        for(int i=0;i<1000;i++){var e=new SwipeEvent(i,1,1,2026,1,1);queue.add(e);expected.add(e.getEventId());}
        Set<String> seen=ConcurrentHashMap.newKeySet();
        SwipeSender sender=new SwipeSender(null,"") {
            @Override public int sendSwipe(SwipeEvent e) throws InterruptedException {
                if(!seen.add(e.getEventId()))throw new AssertionError("Duplicate send");
                if(e.getSkierID()%7==0)Thread.sleep(2);
                return 201;
            }
        };
        var success=new AtomicInteger();var failure=new AtomicInteger();var metrics=new SwipeMetricsCollector();var pool=Executors.newFixedThreadPool(16);List<Future<?>> tasks=new ArrayList<>();
        for(int i=0;i<16;i++)tasks.add(pool.submit(new SenderWorker(queue,sender,success,failure,-1,metrics,new RequestPacer(0),"MAIN")));
        for(var task:tasks)task.get(5,TimeUnit.SECONDS);pool.shutdown();
        if(!seen.equals(expected)||success.get()!=1000||failure.get()!=0||metrics.getCount()!=1000||!queue.isEmpty())throw new AssertionError("Lost work under unequal sender latency");
        System.out.println("Shared workload preserved all 1,000 unique event IDs under unequal sender latency");
    }
}
