package client;

import java.util.*;
import java.util.concurrent.*;
import static java.util.concurrent.TimeUnit.*;

public final class RequestPacerTest {
    public static void main(String[] args) throws Exception {
        RequestPacer pacer = new RequestPacer(200);
        ExecutorService pool = Executors.newFixedThreadPool(20);
        long start=System.nanoTime();List<Future<?>> tasks=new ArrayList<>();
        for(int i=0;i<100;i++)tasks.add(pool.submit(()->{try {pacer.acquire();}catch(InterruptedException e){Thread.currentThread().interrupt();throw new RuntimeException(e);}}));
        for(var task:tasks)task.get(3,SECONDS);
        long elapsed=System.nanoTime()-start;pool.shutdown();
        if(elapsed<MILLISECONDS.toNanos(480))throw new AssertionError("Concurrent workers exceeded global rate");
        Thread thread=new Thread(()->{try{new RequestPacer(1).acquire();}catch(InterruptedException ignored){}});thread.start();thread.join(1000);
        new RequestPacer(0).acquire();
        System.out.println("Global request pacing concurrency checks passed");
    }
}
