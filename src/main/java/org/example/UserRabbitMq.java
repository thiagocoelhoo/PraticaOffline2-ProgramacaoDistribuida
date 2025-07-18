package org.example;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class UserRabbitMq {
    private String rabbitMqBroker;
    private String fila;
    private Logger logger;
    private final List<String> mensagensRecebidas = new ArrayList<>();
    private final int MAX_MENSAGENS = 10;

    public UserRabbitMq(String broker, String fila) {
        this.rabbitMqBroker = broker;
        this.fila = fila;
        this.logger = new Logger(String.format("%s.log", UserRabbitMq.class.getName()));
    }

    public void run() throws Exception {
        ConnectionFactory fabrica = new ConnectionFactory();
        fabrica.setHost(rabbitMqBroker);

        try {
            Connection connection = fabrica.newConnection();
            Channel channel = connection.createChannel();
            channel.queueDeclare(fila, false, false, false, null);
            logger.log("Conectado ao RabbitMQ e aguardando mensagens na fila '" + fila + "'.");

            Object lock = new Object();

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), "UTF-8");
                mensagensRecebidas.add(message);
                logger.log("Dados históricos recebidos: " + message);

                if (mensagensRecebidas.size() >= MAX_MENSAGENS) {
                    synchronized (lock) {
                        lock.notify();
                    }
                }
            };

            channel.basicConsume(fila, true, deliverCallback, consumerTag -> { });

            // Aguarda até que as mensagens sejam recebidas
            synchronized (lock) {
                lock.wait();
            }

            // Exibe o "dashboard" no terminal
            System.out.println("\n==== DASHBOARD DE DADOS HISTÓRICOS ====");
            for (int i = 0; i < mensagensRecebidas.size(); i++) {
                System.out.printf("Mensagem %02d: %s%n", i + 1, mensagensRecebidas.get(i));
            }
            System.out.println("=======================================\n");

        } catch (Exception e) {
            logger.log("Erro ao consumir mensagens: " + e.getMessage());
            throw e;
        }
    }

    public static void main(String[] args) {
        Scanner s = new Scanner(System.in);
        String rabbitMqHost;
        String fila = "drone_data_history";

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
