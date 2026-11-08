
package concurrency;

import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

class TaskAdmissionGateTest {

    @Test
    void shouldRejectNewTasksAfterGateCloses()
            throws Exception {

        ExecutorService workerPool =
                Executors.newFixedThreadPool(1);

        ResortSerialExecutor executor =
                new ResortSerialExecutor(workerPool, 10);

        TaskAdmissionGate gate =
                new TaskAdmissionGate();

        AtomicInteger counter =
                new AtomicInteger(0);

        try {
            // Gate is initially open.
            assertTrue(gate.isAccepting());

            // First task should be accepted.
            assertTrue(
                    gate.submit(
                            counter::incrementAndGet,
                            executor
                    )
            );

            // Shutdown starts.
            gate.close();

            assertFalse(gate.isAccepting());

            // New task must be rejected.
            assertFalse(
                    gate.submit(
                            counter::incrementAndGet,
                            executor
                    )
            );

            // Finish previously accepted tasks.
            workerPool.shutdown();

            assertTrue(
                    workerPool.awaitTermination(
                            5, TimeUnit.SECONDS
                    )
            );

            // Only the first task ran.
            assertEquals(1, counter.get());

        } finally {
            workerPool.shutdownNow();
        }
    }


    @Test
    void shouldNeverAcceptTasksAfterCloseReturns()
            throws Exception {

        ExecutorService workerPool =
                Executors.newFixedThreadPool(2);

        ResortSerialExecutor executor =
                new ResortSerialExecutor(workerPool, 1000);

        TaskAdmissionGate gate =
                new TaskAdmissionGate();

        CountDownLatch startLatch =
                new CountDownLatch(1);

        CountDownLatch closeFinished =
                new CountDownLatch(1);

        AtomicInteger acceptedAfterClose =
                new AtomicInteger(0);

        ExecutorService callers =
                Executors.newFixedThreadPool(2);

        try {

            Future<?> submitter = callers.submit(() -> {

                startLatch.await();

                for (int i = 0; i < 100; i++) {

                    // This flag is sampled before submit().
                    boolean closeAlreadyFinished =
                            closeFinished.getCount() == 0;

                    boolean accepted = gate.submit(
                            () -> {},
                            executor
                    );

                    if (closeAlreadyFinished && accepted) {
                        acceptedAfterClose.incrementAndGet();
                    }
                }

                return null;
            });

            Future<?> closer = callers.submit(() -> {

                startLatch.await();

                gate.close();
                closeFinished.countDown();

                return null;
            });

            // Release both threads simultaneously.
            startLatch.countDown();

            submitter.get(10, TimeUnit.SECONDS);
            closer.get(10, TimeUnit.SECONDS);

            assertFalse(gate.isAccepting());

            assertEquals(
                    0,
                    acceptedAfterClose.get()
            );

            // Explicitly verify post-close submission.
            assertFalse(
                    gate.submit(() -> {}, executor)
            );

        } finally {

            gate.close();

            callers.shutdownNow();
            workerPool.shutdown();

            assertTrue(
                    workerPool.awaitTermination(
                            10, TimeUnit.SECONDS
                    )
            );
        }
    }
}
