
package rabbitmq;

import model.LiftRide;
import model.SwipeEventMessage;
import util.LiftVerticalLookup;

public class SwipePublisherTest {

    public static void main(String[] args) throws Exception {

        RabbitMQManager manager = new RabbitMQManager();
        ChannelPool pool = new ChannelPool(manager, 4);

        try {
            // 1. Initialize the channel pool
            pool.init();

            // 2. Build a test lift ride
            LiftRide ride = new LiftRide(217, 17);

            // 3. Look up the real vertical from JSON
            int vertical =
                    LiftVerticalLookup.getVertical(ride.getLiftID());

            // 4. Create a complete swipe event
            SwipeEventMessage event = new SwipeEventMessage(
                    "3",
                    "1",
                    "1234",
                    ride,
                    vertical,
                    "local-test-server"
            );

            // 5. Publish to RabbitMQ
            SwipePublisher publisher = new SwipePublisher(pool);

            publisher.publish(event);

            System.out.println("Published event: "
                    + event.getEventId());

            System.out.println("Resort: "
                    + event.getResortID());

            System.out.println("Vertical: "
                    + event.getVertical());

            System.out.println("SwipePublisher test PASSED!");

        } finally {
            pool.close();
            manager.close();
        }
    }
}
