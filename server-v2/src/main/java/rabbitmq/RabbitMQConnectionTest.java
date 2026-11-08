
package rabbitmq;

import com.rabbitmq.client.Channel;

public class RabbitMQConnectionTest {

    public static void main(String[] args) {
        RabbitMQManager manager = new RabbitMQManager();

        try {
            System.out.println("Connecting to RabbitMQ...");
            System.out.println("Host: " + RabbitMQConfig.HOST);
            System.out.println("Port: " + RabbitMQConfig.PORT);
            System.out.println("User: " + RabbitMQConfig.USERNAME);

            System.out.println(
                    "Password configured: " +
                            (RabbitMQConfig.PASSWORD != null
                                    && !RabbitMQConfig.PASSWORD.isEmpty())
            );

            // Step 1: Establish an AMQP connection
            manager.connect();

            System.out.println("Connection established!");

            // Step 2: Create a channel using that connection
            Channel channel = manager.createChannel();

            System.out.println("Channel created!");
            System.out.println("Channel open: " + channel.isOpen());

            // Step 3: Release the test channel
            channel.close();

            System.out.println("RabbitMQ connection test PASSED!");

        } catch (Exception e) {
            System.err.println("RabbitMQ connection test FAILED!");
            e.printStackTrace();
        } finally {
            try {
                manager.close();
            } catch (Exception e) {
                System.err.println("Error closing RabbitMQ connection: "
                        + e.getMessage());
            }
        }
    }
}
