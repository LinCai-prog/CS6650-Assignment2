
package concurrency;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class ResortSerialExecutorTest {

    @Test
    void sameResortMustExecuteInOrder() throws Exception {

        ExecutorService workerPool =
                Executors.newFixedThreadPool(3);

        try {
            ResortSerialExecutor serialExecutor =
                    new ResortSerialExecutor(workerPool);

            List<String> executionOrder =
                    Collections.synchronizedList(
                            new ArrayList<>()
                    );

            CountDownLatch firstStarted =
                    new CountDownLatch(1);

            CountDownLatch releaseFirst =
                    new CountDownLatch(1);

            CountDownLatch allFinished =
                    new CountDownLatch(3);

            AtomicInteger runningTasks =
                    new AtomicInteger(0);

            AtomicInteger maxConcurrent =
                    new AtomicInteger(0);

            // Task A: block until the test releases it.
            serialExecutor.execute(() -> {
                int running = runningTasks.incrementAndGet();
                maxConcurrent.accumulateAndGet(
                        running, Math::max
                );

                try {
                    executionOrder.add("A");
                    firstStarted.countDown();

                    releaseFirst.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    runningTasks.decrementAndGet();
                    allFinished.countDown();
                }
            });

            assertTrue(
                    firstStarted.await(5, TimeUnit.SECONDS),
                    "Task A did not start"
            );

            // A is still blocked. Submit B and C.
            serialExecutor.execute(() -> {
                int running = runningTasks.incrementAndGet();
                maxConcurrent.accumulateAndGet(
                        running, Math::max
                );

                try {
                    executionOrder.add("B");
                } finally {
                    runningTasks.decrementAndGet();
                    allFinished.countDown();
                }
            });

            serialExecutor.execute(() -> {
                int running = runningTasks.incrementAndGet();
                maxConcurrent.accumulateAndGet(
                        running, Math::max
                );

                try {
                    executionOrder.add("C");
                } finally {
                    runningTasks.decrementAndGet();
                    allFinished.countDown();
                }
            });

            // A is blocked, so B and C must not execute.
            assertEquals(
                    Arrays.asList("A"),
                    new ArrayList<>(executionOrder)
            );

            // Now allow A to finish.
            releaseFirst.countDown();

            assertTrue(
                    allFinished.await(5, TimeUnit.SECONDS),
                    "Not all tasks finished"
            );

            assertEquals(
                    Arrays.asList("A", "B", "C"),
                    new ArrayList<>(executionOrder)
            );

            assertEquals(
                    1,
                    maxConcurrent.get(),
                    "Same-resort tasks must not overlap"
            );

        } finally {
            workerPool.shutdownNow();
        }
    }

    @Test
    void differentResortsCanExecuteConcurrently()
            throws Exception {

        ExecutorService workerPool =
                Executors.newFixedThreadPool(2);

        CountDownLatch releaseTasks =
                new CountDownLatch(1);

        try {
            ResortSerialExecutor resort1 =
                    new ResortSerialExecutor(workerPool);

            ResortSerialExecutor resort2 =
                    new ResortSerialExecutor(workerPool);

            CountDownLatch bothStarted =
                    new CountDownLatch(2);

            CountDownLatch bothFinished =
                    new CountDownLatch(2);

            AtomicInteger runningTasks =
                    new AtomicInteger(0);

            AtomicInteger maxConcurrent =
                    new AtomicInteger(0);

            Runnable task = () -> {
                int running = runningTasks.incrementAndGet();

                maxConcurrent.accumulateAndGet(
                        running, Math::max
                );

                bothStarted.countDown();

                try {
                    releaseTasks.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    runningTasks.decrementAndGet();
                    bothFinished.countDown();
                }
            };

            resort1.execute(task);
            resort2.execute(task);

            assertTrue(
                    bothStarted.await(5, TimeUnit.SECONDS),
                    "Both resort tasks should start"
            );

            assertEquals(
                    2,
                    maxConcurrent.get(),
                    "Different resorts should execute concurrently"
            );

            releaseTasks.countDown();

            assertTrue(
                    bothFinished.await(5, TimeUnit.SECONDS)
            );

        } finally {
            releaseTasks.countDown();
            workerPool.shutdownNow();
        }
    }


    @Test
    void shouldRejectWhenPendingQueueIsFull() throws Exception {

        ExecutorService workerPool =
                Executors.newFixedThreadPool(1);

        CountDownLatch taskStarted =
                new CountDownLatch(1);

        CountDownLatch releaseFirst =
                new CountDownLatch(1);

        CountDownLatch allFinished =
                new CountDownLatch(3);

        try {
            ResortSerialExecutor executor =
                    new ResortSerialExecutor(workerPool, 2);

            // First task is running, blocked on a latch.
            executor.execute(() -> {
                taskStarted.countDown();
                try {
                    releaseFirst.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    allFinished.countDown();
                }
            });

            assertTrue(
                    taskStarted.await(5, TimeUnit.SECONDS)
            );

            // Two tasks can wait in the queue.
            executor.execute(allFinished::countDown);
            executor.execute(allFinished::countDown);

            assertEquals(
                    2,
                    executor.getPendingTaskCount()
            );

            // A fourth task exceeds the pending capacity.
            assertThrows(
                    RejectedExecutionException.class,
                    () -> executor.execute(() -> {})
            );

            assertEquals(
                    2,
                    executor.getPendingTaskCount()
            );

            releaseFirst.countDown();

            assertTrue(
                    allFinished.await(5, TimeUnit.SECONDS)
            );

            assertEquals(
                    0,
                    executor.getPendingTaskCount()
            );

        } finally {
            releaseFirst.countDown();
            workerPool.shutdownNow();
        }
    }

    @Test
    void shouldRejectTaskWhenWorkerPoolIsShutdown() {

        ExecutorService workerPool =
                Executors.newFixedThreadPool(1);

        ResortSerialExecutor executor =
                new ResortSerialExecutor(workerPool, 2);

        // Simulate worker pool shutdown.
        workerPool.shutdown();

        // New work should be rejected.
        assertThrows(
                RejectedExecutionException.class,
                () -> executor.execute(() -> {
                    System.out.println("Should not execute");
                })
        );

        // Rejected task should not remain in the queue.
        assertEquals(
                0,
                executor.getPendingTaskCount()
        );
    }


    @Test
    void shouldDrainAcceptedTasksAfterWorkerPoolShutdown()
            throws Exception {

        // Create a worker pool with only one thread.
        ExecutorService workerPool =
                Executors.newFixedThreadPool(1);

        // Create the serial executor with capacity 10.
        ResortSerialExecutor executor =
                new ResortSerialExecutor(workerPool, 10);

        // Signals that Task A has started.
        CountDownLatch taskAStarted =
                new CountDownLatch(1);

        // Controls when Task A is allowed to finish.
        CountDownLatch releaseTaskA =
                new CountDownLatch(1);

        // Waits until A, B, and C have all finished.
        CountDownLatch allTasksFinished =
                new CountDownLatch(3);

        // Thread-safe list to record execution order.
        List<String> executionOrder =
                new CopyOnWriteArrayList<>();

        try {

            // ---------------------------
            // Step 1: Submit Task A.
            // ---------------------------
            executor.execute(() -> {

                executionOrder.add("A");

                // Notify the test that A has started.
                taskAStarted.countDown();

                try {
                    // Pause Task A here.
                    releaseTaskA.await();

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();

                } finally {
                    allTasksFinished.countDown();
                }
            });

            // Make sure A is actually running.
            assertTrue(
                    taskAStarted.await(
                            5, TimeUnit.SECONDS
                    )
            );

            // ---------------------------
            // Step 2: Queue Task B.
            // ---------------------------
            executor.execute(() -> {

                executionOrder.add("B");

                allTasksFinished.countDown();
            });

            // ---------------------------
            // Step 3: Queue Task C.
            // ---------------------------
            executor.execute(() -> {

                executionOrder.add("C");

                allTasksFinished.countDown();
            });

            // A is running, B and C are pending.
            assertEquals(
                    2,
                    executor.getPendingTaskCount()
            );

            // ---------------------------
            // Step 4: Shutdown worker pool.
            // ---------------------------
            workerPool.shutdown();

            // ---------------------------
            // Step 5: Release Task A.
            // ---------------------------
            releaseTaskA.countDown();

            // A, B and C should all finish.
            assertTrue(
                    allTasksFinished.await(
                            10, TimeUnit.SECONDS
                    )
            );

            // Verify execution order.
            assertEquals(
                    List.of("A", "B", "C"),
                    executionOrder
            );

            // Verify worker pool terminates.
            assertTrue(
                    workerPool.awaitTermination(
                            5, TimeUnit.SECONDS
                    )
            );

            // No pending tasks should remain.
            assertEquals(
                    0,
                    executor.getPendingTaskCount()
            );

        } finally {

            // Prevent a stuck test from leaving A blocked.
            releaseTaskA.countDown();

            // Clean up worker threads.
            workerPool.shutdownNow();
        }
    }


}
