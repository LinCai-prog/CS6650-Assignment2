package rabbitmq;

public final class RabbitMQConfig {

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

    public static final int RESORT_COUNT = 10;

    public static final int CHANNEL_POOL_SIZE = 32;

    public static final int MESSAGE_TTL_MS = 600_000;
    public static final int QUEUE_MAX_LENGTH = 1_000_000;

    private RabbitMQConfig() {
        // Prevent instantiation
    }
}