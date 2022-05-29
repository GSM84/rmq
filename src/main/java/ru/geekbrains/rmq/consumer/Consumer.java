package ru.geekbrains.rmq.consumer;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.Scanner;

public class Consumer {
    private static final String EXCHANGE_NAME = "it_blog_exchanger";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);

        String queueName = channel.queueDeclare().getQueue();
        System.out.println("QUEUE NAME: " + queueName);


        Scanner in = new Scanner(System.in);
        String command;
        String routingKey = null;

        while (true) {
            System.out.println("Write command: set_topic xxx/unset_topic xxx");
            command = in.nextLine();
            if (command.substring(0, command.indexOf(32)).equals("set_topic")) {
                routingKey = subscribe(command, channel, queueName);

            } else if (command.substring(0, command.indexOf(32)).equals("unset_topic")) {
                channel.queueUnbind(queueName, EXCHANGE_NAME, routingKey);

            } else if (command.equals("q")){
                in.close();
                System.exit(0);
            }
        }
    }

    public static String subscribe(String command, Channel channel, String queueName) throws IOException {
        String routingKey = command.substring(command.indexOf(32)+ 1);

        channel.queueBind(queueName, EXCHANGE_NAME, routingKey);

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [x] Received '" + delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");
        };

        System.out.println(" [*] Waiting for messages with routing key (" + routingKey + "):");

        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
        });

        return routingKey;
    }
}
