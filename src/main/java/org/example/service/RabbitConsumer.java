package org.example.service;

import org.example.model.DroneData;
import org.springframework.web.client.RestTemplate;

public class RabbitConsumer {
    // Supondo que já existe lógica para consumir do RabbitMQ
    public void handleRabbitMessage(DroneData data) {
        // Envia para o API Gateway via REST
        RestTemplate restTemplate = new RestTemplate();
        String apiGatewayUrl = "http://localhost:8081/data";
        try {
            restTemplate.postForEntity(apiGatewayUrl, data, Void.class);
            System.out.println("Dados enviados via REST para o API Gateway.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
