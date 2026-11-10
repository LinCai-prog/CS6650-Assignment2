package client;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/** One shared no-burst schedule per phase; pacing time is excluded from HTTP latency. */
public final class RequestPacer {
    private final long intervalNanos;
    private final AtomicLong next = new AtomicLong();
    public RequestPacer(int perSecond) {
        if (perSecond < 0 || perSecond > 100000) throw new IllegalArgumentException("TARGET_RPS must be in 0..100000");
        intervalNanos = perSecond == 0 ? 0 : 1_000_000_000L / perSecond;
    }
    public void acquire() throws InterruptedException {
        if (intervalNanos == 0) return;
        long previous, permit, following;
        do {
            previous = next.get();
            permit = Math.max(System.nanoTime(), previous);
            following = permit + intervalNanos;
        } while (!next.compareAndSet(previous, following));
        long remaining;
        while ((remaining = permit - System.nanoTime()) > 0) {
            if (Thread.interrupted()) throw new InterruptedException();
            LockSupport.parkNanos(remaining);
        }
        if (Thread.interrupted()) throw new InterruptedException();
    }
}
