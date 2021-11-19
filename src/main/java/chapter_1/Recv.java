package chapter_1;

import com.rabbitmq.client.*;

import java.nio.charset.StandardCharsets;

public class Recv {

    private final static String QUEUE_NAME = "routineKey";

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("39.105.24.162");
        factory.setUsername("admin");
        factory.setPassword("password");
        factory.setPort(5672);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        System.out.println(" [*] Waiting for messages. to exit press control+C");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            AMQP.BasicProperties properties = delivery.getProperties();
            Envelope envelope = delivery.getEnvelope();
            System.out.println(" [body] Received'" + message + "'");
            System.out.println(" [envelope] Received'" + envelope + "'");
            System.out.println(" [properties] Received'" + properties + "'");
        };
        channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> { });
    }
}
