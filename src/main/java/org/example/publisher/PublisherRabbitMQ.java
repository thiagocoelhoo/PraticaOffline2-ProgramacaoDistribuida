package org.example.publisher;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/*
Publisher RabbitMQ:
Um endpoint (microsserviço) que publica os dados em um broker RabbitMQ.

Isso permite que outros consumidores obtenham os dados para criar históricos,
dashboards e realizar outras operações.

O processo que consome esses dados pode ter uma base de dados específica
para guardar os dados obtidos e servi-los a clientes HTTP que fazem buscas nessa base.
*/

public class PublisherRabbitMQ {
    private static final String RABBITMQ_HOST = "localhost";
    private static final String QUEUE_NAME = "drone_data_history";

    private Connection connection;
    private Channel channel;

    public PublisherRabbitMQ() {
    }

    public void run() {
        try {
            setupRabbitMq();
            Thread.currentThread().join();
        } catch (IOException | TimeoutException | InterruptedException e) {
            System.err.println("Erro ao inicializar PublisherRabbitMQ: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void setupRabbitMq() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(RABBITMQ_HOST);

        connection = factory.newConnection();
        channel = connection.createChannel();

        channel.queueDeclare(
            QUEUE_NAME,
            false,
            false,
            false,
            null
        );
        System.out.println("PublisherRabbitMQ conectado ao RabbitMQ");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            publishData(message);
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        };

        // Iniciar o consumo das mensagens da fila
        channel.basicConsume(
            QUEUE_NAME,
            false,
            deliverCallback,
            consumerTag -> {}
        );
    }

    private void publishData(String data) {
        try {
            channel.basicPublish(
                "",
                QUEUE_NAME,
                null,
                data.getBytes()
            );
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void close() throws IOException, TimeoutException {
        if (channel != null && channel.isOpen()) {
            channel.close();
        }
        if (connection != null && connection.isOpen()) {
            connection.close();
        }
        System.out.println("PublisherRabbitMQ connection closed.");
    }

    public static void main(String[] args) {
        PublisherRabbitMQ pubRabbitMQ = new PublisherRabbitMQ();
        pubRabbitMQ.run();
    }
}