
package rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class RabbitMQManager {

    private final ConnectionFactory factory;
    private Connection connection;

    public RabbitMQManager() {
        factory = new ConnectionFactory();
        factory.setHost(RabbitMQConfig.HOST);
        factory.setPort(RabbitMQConfig.PORT);
        factory.setUsername(RabbitMQConfig.USERNAME);
        factory.setPassword(RabbitMQConfig.PASSWORD);

        // We handle connection recovery explicitly.
        factory.setAutomaticRecoveryEnabled(false);
    }

    public synchronized void connect() throws Exception {
        if (isConnected()) {
            return;
        }

        connection = factory.newConnection();
        System.out.println("RabbitMQ connection established.");
    }

    public synchronized Channel createChannel() throws Exception {
        connect();
        return connection.createChannel();
    }

    public synchronized boolean isConnected() {
        return connection != null && connection.isOpen();
    }

    public synchronized void reconnect() throws Exception {
        if (isConnected()) {
            return;
        }

        close();
        connect();

        System.out.println("RabbitMQ connection recovered.");
    }

    public synchronized void close() {
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception e) {
                System.err.println(
                        "Error closing RabbitMQ connection: "
                                + e.getMessage()
                );
            } finally {
                connection = null;
            }
        }
    }
}
