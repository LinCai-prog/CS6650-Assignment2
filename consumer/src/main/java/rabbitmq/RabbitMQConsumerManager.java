
package rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import config.ConsumerConfig;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class RabbitMQConsumerManager implements AutoCloseable {

    private Connection connection;

    public void connect() throws IOException, TimeoutException {
        if (ConsumerConfig.PASSWORD == null ||
                ConsumerConfig.PASSWORD.isEmpty()) {
            throw new IllegalStateException(
                    "RABBITMQ_PASSWORD environment variable is required"
            );
        }

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(ConsumerConfig.HOST);
        factory.setPort(ConsumerConfig.PORT);
        factory.setUsername(ConsumerConfig.USERNAME);
        factory.setPassword(ConsumerConfig.PASSWORD);
        factory.setVirtualHost("/");

        factory.setRequestedHeartbeat(30);
        factory.setConnectionTimeout(10000);
        factory.setAutomaticRecoveryEnabled(true);
        factory.setTopologyRecoveryEnabled(true);

        connection = factory.newConnection("skitrak-consumer");

        System.out.println("Consumer connected to RabbitMQ.");
    }

    public Channel createChannel() throws IOException {
        if (connection == null || !connection.isOpen()) {
            throw new IllegalStateException(
                    "RabbitMQ connection is not available"
            );
        }
        return connection.createChannel();
    }

    @Override
    public void close() throws Exception {
        if (connection != null && connection.isOpen()) {
            connection.close();
        }
    }
}
