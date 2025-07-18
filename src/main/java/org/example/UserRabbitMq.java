package org.example;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.util.Scanner;

public class UserRabbitMq {
    private String rabbitMqBroker;
    private String fila;
    private Logger logger;

    public UserRabbitMq(String broker, String fila) {
        this.rabbitMqBroker = broker;
        this.fila = fila;
        this.logger = new Logger(String.format("%s.log", UserRabbitMq.class.getName()));
    }

    public void run() throws Exception {
        ConnectionFactory fabrica = new ConnectionFactory();
        fabrica.setHost(rabbitMqBroker);
        Connection connection = fabrica.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(fila, false, false, false, null);
        logger.log("Conectado ao RabbitMQ e aguardando mensagens na fila '" + fila + "'.");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            logger.log("Dados históricos recebidos: " + message);
        };
        channel.basicConsume(fila, true, deliverCallback, consumerTag -> { });

        // Mantém a thread principal ativa
        Thread.currentThread().join();

        // Fecha a conexão
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                if (channel != null && channel.isOpen()) {
                    channel.close();
                }
                if (connection != null && connection.isOpen()) {
                    connection.close();
                }
                System.out.println("Conexão com RabbitMQ encerrada.");
            } catch (Exception e) {
                System.err.println("Erro ao encerrar recursos do RabbitMQ: " + e.getMessage());
            }
        }));
    }

    public static void main(String[] args) {
        Scanner s = new Scanner(System.in);
        String rabbitMqHost;
        String fila = "climatic_data_history";

        System.out.print("Host do RabbitMQ: ");
        rabbitMqHost = s.nextLine();

        try {
            UserRabbitMq client = new UserRabbitMq(rabbitMqHost, fila);
            client.run();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            s.close();
        }
    }
}
