
package rabbitmq;

import com.rabbitmq.client.Channel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class ChannelPool {

    private static final int BORROW_TIMEOUT_SECONDS = 1;

    private final RabbitMQManager manager;
    private final int poolSize;
    private final Object rebuildLock = new Object();

    private volatile PoolGeneration current;
    private volatile boolean closed = false;
    private long nextGeneration = 0;

    private static class PoolGeneration {

        final long id;
        final BlockingQueue<Channel> available;
        final Set<Channel> members;

        PoolGeneration(
                long id,
                BlockingQueue<Channel> available,
                Set<Channel> members
        ) {
            this.id = id;
            this.available = available;
            this.members = members;
        }
    }

    public ChannelPool(RabbitMQManager manager, int poolSize) {
        if (poolSize <= 0) {
            throw new IllegalArgumentException(
                    "Pool size must be positive."
            );
        }

        this.manager = manager;
        this.poolSize = poolSize;
    }

    public void init() throws Exception {
        synchronized (rebuildLock) {
            if (closed) {
                throw new IllegalStateException("Pool is closed.");
            }

            if (current == null) {
                current = createGeneration();
            }
        }
    }

    private PoolGeneration createGeneration() throws Exception {

        manager.connect();

        BlockingQueue<Channel> queue =
                new ArrayBlockingQueue<>(poolSize);

        Set<Channel> members = Collections.newSetFromMap(
                new IdentityHashMap<Channel, Boolean>()
        );

        List<Channel> created = new ArrayList<>();

        try {
            for (int i = 0; i < poolSize; i++) {

                Channel channel = manager.createChannel();
                created.add(channel);

                channel.confirmSelect();

                members.add(channel);
                queue.add(channel);
            }

            long id = ++nextGeneration;

            System.out.println(
                    "Created ChannelPool generation " + id
            );

            return new PoolGeneration(id, queue, members);

        } catch (Exception e) {

            // Do not expose a partially initialized pool.
            for (Channel channel : created) {
                closeChannel(channel);
            }

            throw e;
        }
    }

    public Channel borrowChannel() throws Exception {

        for (int attempt = 0; attempt < 3; attempt++) {

            if (closed) {
                throw new IllegalStateException("Pool is closed.");
            }

            PoolGeneration generation = current;

            if (generation == null) {
                init();
                continue;
            }

            if (!manager.isConnected()) {
                rebuildPool(generation);
                continue;
            }

            Channel channel = generation.available.poll(
                    BORROW_TIMEOUT_SECONDS,
                    TimeUnit.SECONDS
            );

            if (channel == null) {
                if (current != generation) {
                    continue;
                }

                throw new IllegalStateException(
                        "Timed out waiting for RabbitMQ channel."
                );
            }

            // A rebuild may have happened during poll().
            if (closed || current != generation) {
                closeChannel(channel);
                continue;
            }

            if (!channel.isOpen()) {
                closeChannel(channel);
                rebuildPool(generation);
                continue;
            }

            return channel;
        }

        throw new IllegalStateException(
                "Unable to borrow a healthy RabbitMQ channel."
        );
    }

    public void returnChannel(Channel channel) {

        if (channel == null) {
            return;
        }

        PoolGeneration generation = current;

        // Reject channels from previous generations.
        if (closed ||
                generation == null ||
                !generation.members.contains(channel) ||
                !channel.isOpen()) {

            closeChannel(channel);
            return;
        }

        if (!generation.available.offer(channel)) {
            closeChannel(channel);
        }

        // A rebuild could have happened during offer().
        // If so, discard this channel from the old generation.
        if (current != generation) {
            generation.available.remove(channel);
            closeChannel(channel);
        }
    }

    private void rebuildPool(
            PoolGeneration observedGeneration
    ) throws Exception {

        synchronized (rebuildLock) {

            if (closed) {
                throw new IllegalStateException("Pool is closed.");
            }

            // Another thread already replaced this generation.
            if (current != observedGeneration) {
                return;
            }

            // Reconnect if the underlying connection is dead.
            if (!manager.isConnected()) {
                manager.reconnect();
            }

            // Build everything before switching generations.
            PoolGeneration replacement = createGeneration();

            current = replacement;

            // Close idle channels from the old generation.
            // Borrowed channels are rejected when returned.
            if (observedGeneration != null) {
                Channel oldChannel;

                while ((oldChannel =
                        observedGeneration.available.poll()) != null) {

                    closeChannel(oldChannel);
                }
            }

            System.out.println(
                    "ChannelPool switched to generation "
                            + replacement.id
            );
        }
    }

    public void rebuildPool() throws Exception {
        rebuildPool(current);
    }

    public int availableChannels() {
        PoolGeneration generation = current;
        return generation == null ? 0 : generation.available.size();
    }

    public void close() {

        synchronized (rebuildLock) {

            closed = true;

            PoolGeneration generation = current;
            current = null;

            if (generation != null) {
                Channel channel;

                while ((channel =
                        generation.available.poll()) != null) {

                    closeChannel(channel);
                }
            }
        }
    }

    private static void closeChannel(Channel channel) {

        if (channel == null) {
            return;
        }

        try {
            if (channel.isOpen()) {
                channel.close();
            }
        } catch (Exception e) {
            System.err.println(
                    "Error closing channel: " + e.getMessage()
            );
        }
    }
}
