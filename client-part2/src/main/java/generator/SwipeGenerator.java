package generator;

import model.SwipeEvent;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadLocalRandom;

public class SwipeGenerator implements Runnable {

    private final BlockingQueue<SwipeEvent> queue;
    private final int totalEvents;
    private final int resortCount;

    public SwipeGenerator(
            BlockingQueue<SwipeEvent> queue,
            int totalEvents,
            int resortCount) {

        this.queue = queue;
        this.totalEvents = totalEvents;
        this.resortCount = resortCount;
    }

    @Override
    public void run() {

        ThreadLocalRandom random = ThreadLocalRandom.current();

        try {
            for (int i = 0; i < totalEvents; i++) {

                SwipeEvent event = new SwipeEvent(
                        random.nextInt(1, 100001),
                        random.nextInt(1, resortCount + 1),
                        random.nextInt(1, 41),
                        2026,
                        1,
                        random.nextInt(1, 361)
                );

                queue.put(event);
            }

            System.out.println(
                    "Finished generating " + totalEvents + " events."
            );

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Generator interrupted.");
        }
    }
}
