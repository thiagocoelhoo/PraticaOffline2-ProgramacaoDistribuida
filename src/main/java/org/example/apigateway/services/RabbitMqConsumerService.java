package org.example.apigateway.services;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.stereotype.Service;
import org.example.apigateway.models.DroneData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;

@Service
public class RabbitMqConsumerService {
    private static final String RABBITMQ_HOST = "localhost";
    private static final String DRONE_DATA_HISTORY_QUEUE = "gateway_to_rabbitmq_publisher_queue";

    private Connection connection;
    private Channel channel;

    private final List<DroneData> historicalDataLog = Collections.synchronizedList(new ArrayList<>());
    private static final int MAX_HISTORY_ITEMS = 100;

    @PostConstruct
    public void init() {
        try {
            setupRabbitMqConsumer();
        } catch (IOException | TimeoutException e) {
            System.err.println("RabbitMqConsumerService: Erro ao iniciar o consumidor RabbitMQ");
            e.printStackTrace();
        }
    }

    private void setupRabbitMqConsumer() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(RABBITMQ_HOST);

        connection = factory.newConnection();
        channel = connection.createChannel();

        channel.queueDeclare(
            DRONE_DATA_HISTORY_QUEUE,
            false,
            false,
            false,
            null
        );
        
        System.out.println("RabbitMqConsumerService: Conectado ao RabbitMQ");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String messagePayload = new String(delivery.getBody(), "UTF-8");

            try {
                DroneData parsedData = DroneData.parseFromString(messagePayload);
                
                historicalDataLog.add(parsedData);
                if (historicalDataLog.size() > MAX_HISTORY_ITEMS) {
                    historicalDataLog.remove(0);
                }
            } catch (IllegalArgumentException e) {
                e.printStackTrace();
            }
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        };

        channel.basicConsume(DRONE_DATA_HISTORY_QUEUE, false, deliverCallback, consumerTag -> {});
    }

    public List<DroneData> getAllHistoricalData() {
        return new ArrayList<>(historicalDataLog);
    }

    public List<DroneData> getHistoricalDataByRegion(String region) {
        List<DroneData> filteredData = new ArrayList<>();
        
        for (DroneData data : historicalDataLog) {
            if (
                    data.getRegiao() != null &&
                    data.getRegiao().equals(region.toUpperCase())
            ) {
                filteredData.add(data);
            }
        }
        
        return filteredData;
    }

    @PreDestroy
    public void destroy() {
        try {
            if (channel != null && channel.isOpen()) {
                channel.close();
            }
            if (connection != null && connection.isOpen()) {
                connection.close();
            }            
        } catch (IOException | TimeoutException e) {
            System.err.println("RabbitMqConsumerService: Erro ao fechar conexão RabbitMQ");
        }
    }
}