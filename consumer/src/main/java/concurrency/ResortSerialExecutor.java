
package concurrency;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

public class ResortSerialExecutor implements Executor {

    private final Executor workerPool;
    private final Queue<Runnable> tasks = new ArrayDeque<>();

    private final int maxPendingTasks;

    private boolean running = false;

    public ResortSerialExecutor(
            Executor workerPool,
            int maxPendingTasks) {

        if (workerPool == null) {
            throw new IllegalArgumentException(
                    "Worker pool cannot be null"
            );
        }

        if (maxPendingTasks < 1) {
            throw new IllegalArgumentException(
                    "maxPendingTasks must be positive"
            );
        }

        this.workerPool = workerPool;
        this.maxPendingTasks = maxPendingTasks;
    }

    // Preserve existing constructor compatibility.
    public ResortSerialExecutor(Executor workerPool) {
        this(workerPool, 10);
    }

    @Override
    public synchronized void execute(Runnable task) {

        if (task == null) {
            throw new NullPointerException("task");
        }

        if (tasks.size() >= maxPendingTasks) {
            throw new RejectedExecutionException(
                    "Resort task queue is full"
            );
        }

        tasks.offer(task);

        if (!running) {
            running = true;

            try {
                workerPool.execute(this::drainTasks);
            } catch (RejectedExecutionException e) {

                // The worker pool did not accept the drain
                // task, so undo this submission.
                running = false;
                tasks.remove(task);

                throw e;
            }
        }
    }

    private void drainTasks() {

        while (true) {

            Runnable next;

            synchronized (this) {
                next = tasks.poll();

                if (next == null) {
                    running = false;
                    return;
                }
            }

            try {
                next.run();
            } catch (Throwable error) {
                System.err.println(
                        "Resort task failed: " + error
                );
            }
        }
    }

    public synchronized int getPendingTaskCount() {
        return tasks.size();
    }
}
