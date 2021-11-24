package work_queues;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

public class NewTask {

    private static final String TASK_QUEUE_NAME = "task_queue";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("39.105.24.162");
        factory.setUsername("admin");
        factory.setPassword("password");
        factory.setPort(5672);

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);

            String message = String.join(" ", argv);

            // PERSISTENT_TEXT_PLAIN，消息持久化配置，mq会把消息存入disk，但这个也不一定完全能保证被持久化。
            // 因为这里会有一个mq接收消息但还未保存它的空窗期，这时候crash了是没办法的。
            channel.basicPublish("", TASK_QUEUE_NAME,
                    MessageProperties.PERSISTENT_TEXT_PLAIN,
                    message.getBytes("UTF-8"));
            System.out.println(" [x] Sent '" + message + "'");
        }
    }
}
