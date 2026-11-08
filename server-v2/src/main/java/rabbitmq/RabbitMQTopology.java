
package rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.BuiltinExchangeType;

import java.util.HashMap;
import java.util.Map;

public class RabbitMQTopology {

    private final RabbitMQManager manager;

    public RabbitMQTopology(RabbitMQManager manager) {
        this.manager = manager;
    }

    public void initialize() throws Exception {

        try (Channel channel = manager.createChannel()) {

            // 1. Declare the topic exchange.
            channel.exchangeDeclare(
                    RabbitMQConfig.SWIPE_EXCHANGE,
                    BuiltinExchangeType.TOPIC,
                    true
            );

            // 2. Create one queue for each resort.
            for (int resortId = 1;
                 resortId <= RabbitMQConfig.RESORT_COUNT;
                 resortId++) {

                String queueName = "resort." + resortId;

                Map<String, Object> arguments = new HashMap<>();

                arguments.put(
                        "x-message-ttl",
                        RabbitMQConfig.MESSAGE_TTL_MS
                );

                arguments.put(
                        "x-max-length",
                        RabbitMQConfig.QUEUE_MAX_LENGTH
                );

                // Reject new messages rather than silently
                // removing the oldest messages when full.
                arguments.put("x-overflow", "reject-publish");

                // 3. Declare a durable queue.
                channel.queueDeclare(
                        queueName,
                        true,
                        false,
                        false,
                        arguments
                );

                // 4. Bind the queue to the exchange.
                channel.queueBind(
                        queueName,
                        RabbitMQConfig.SWIPE_EXCHANGE,
                        "resort." + resortId
                );

                System.out.println(
                        "Declared queue and binding: " + queueName
                );
            }

            System.out.println(
                    "RabbitMQ topology initialized successfully."
            );
        }
    }

    public static void main(String[] args) {
        RabbitMQManager manager = new RabbitMQManager();

        try {
            manager.connect();

            RabbitMQTopology topology = new RabbitMQTopology(manager);
            topology.initialize();

            System.out.println("Topology setup PASSED!");

        } catch (Exception e) {
            System.err.println("Topology setup FAILED!");
            e.printStackTrace();
            System.exit(1);
        } finally {
            manager.close();
        }
    }
}
