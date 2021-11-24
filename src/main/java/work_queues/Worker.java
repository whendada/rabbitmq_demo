package work_queues;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

public class Worker {

    private static final String TASK_QUEUE_NAME = "task_queue";

    public static void main(String[] args) throws Exception{
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("39.105.24.162");
        factory.setUsername("admin");
        factory.setPassword("password");
        factory.setPort(5672);
        final Connection connection = factory.newConnection();
        final Channel channel = connection.createChannel();

        // durable=true表示队列一直存活，如果rabbitMQ服务器挂了，重启之后队列也会重启
        // 但是如果有一个重名的队列，此时会以第一次的配置为准，后面的配置不会生效，所以最好是重新设置一个队列
        channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        // 这里表示不要把消息给一个正在处理消息的consumer
        channel.basicQos(1);

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");

            System.out.println(" [x] Received '" + message + "'");
            try {
                doWork(message);
            } finally {
                System.out.println(" [x] Done");
                // 这一步是因为开启了显式确认，向rabbitMQ server发送回执
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            }
        };
        // 显式确认，和 basicAck一起使用
        // autoAck为true表示收到消息就会将消息删除，为false表示必须向rabbit MQ确认，然后再删除，如果没有回执就会一直发
        channel.basicConsume(TASK_QUEUE_NAME, false, deliverCallback, consumerTag -> { });
    }

    private static void doWork(String task) {
        for (char ch : task.toCharArray()) {
            if (ch == '.') {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException _ignored) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}
