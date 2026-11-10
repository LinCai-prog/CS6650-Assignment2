
package config;

public final class ConsumerConfig {

    private ConsumerConfig() {}

    public static final String HOST =
            System.getenv().getOrDefault("RABBITMQ_HOST", "127.0.0.1");

    public static final int PORT =
            Integer.parseInt(
                    System.getenv().getOrDefault("RABBITMQ_PORT", "5673")
            );

    public static final String USERNAME =
            System.getenv().getOrDefault("RABBITMQ_USERNAME", "skitrak-app");

    public static final String PASSWORD =
            System.getenv("RABBITMQ_PASSWORD");

    public static final String SWIPE_EXCHANGE = "skitrak.exchange";

    public static final int PREFETCH_COUNT = positive("CONSUMER_PREFETCH", 50);
    public static final int DB_BATCH_SIZE = positive("DB_BATCH_SIZE", 50);
    public static final int DB_BATCH_WAIT_MS = positive("DB_BATCH_WAIT_MS", 5);
    public static final int SQLITE_CACHE_KIB = positive("SQLITE_CACHE_KIB", 65536);
    public static final int SQLITE_CHECKPOINT_PAGES = positive("SQLITE_CHECKPOINT_PAGES", 16384);
    public static final int SQLITE_CHECKPOINT_MS = positive("SQLITE_CHECKPOINT_MS", 1000);
    public static final int ACK_BATCH_SIZE = positive("ACK_BATCH_SIZE", 25);
    public static final int ACK_FLUSH_MS = positive("ACK_FLUSH_MS", 5);

    private static int positive(String name, int fallback) {
        int value = Integer.parseInt(System.getenv().getOrDefault(name, String.valueOf(fallback)));
        if (value < 1) throw new IllegalArgumentException(name + " must be positive");
        return value;
    }


    public static final int RESORT_COUNT =
            Integer.parseInt(
                    System.getenv().getOrDefault("RESORT_COUNT", "10")
            );

    public static final int WORKER_THREADS =
            positive("CONSUMER_THREADS", 10);

}
