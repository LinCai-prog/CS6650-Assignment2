
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

    public static final int PREFETCH_COUNT = 10;


    public static final int RESORT_COUNT =
            Integer.parseInt(
                    System.getenv().getOrDefault("RESORT_COUNT", "10")
            );

    public static final int WORKER_THREADS =
            Integer.parseInt(
                    System.getenv().getOrDefault("CONSUMER_THREADS", "10")
            );

}
