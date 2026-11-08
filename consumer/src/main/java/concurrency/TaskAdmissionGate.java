
package concurrency;

import java.util.concurrent.RejectedExecutionException;

public class TaskAdmissionGate {

    private boolean accepting = true;

    /**
     * Atomically checks whether tasks are accepted
     * and submits the task.
     *
     * @return true if accepted, false if gate is closed
     */
    public synchronized boolean submit(
            Runnable task,
            ResortSerialExecutor executor) {

        if (!accepting) {
            return false;
        }

        try {
            executor.execute(task);
            return true;

        } catch (RejectedExecutionException e) {
            return false;
        }
    }

    /**
     * Prevents future task submissions.
     */
    public synchronized void close() {
        accepting = false;
    }

    public synchronized boolean isAccepting() {
        return accepting;
    }
}
