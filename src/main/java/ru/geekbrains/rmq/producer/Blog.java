package ru.geekbrains.rmq.producer;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

public class Blog {
    private static final String EXCHANGE_NAME = "it_blog_exchanger";

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel();
             Scanner in = new Scanner(System.in)
        ) {
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);
            while (true) {
                System.out.println("введите сообщение");
                sendMessage(in.nextLine(), channel);
            }
        }
    }

    public static void sendMessage(String message, Channel channel) throws IOException {
        int separator = message.indexOf(32);
        String routingKey = message.substring(0, separator);
        String payLoad    = message.substring(separator + 1);

        channel.basicPublish(EXCHANGE_NAME, routingKey, null, payLoad.getBytes("UTF-8"));
        System.out.println(" [x] Sent '" + routingKey + "':'" + payLoad + "'");
    }
}
