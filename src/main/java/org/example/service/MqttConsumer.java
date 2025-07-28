package org.example.service;

import org.example.model.DroneData;
import org.springframework.web.client.RestTemplate;

public class MqttConsumer {
    // Supondo que já existe lógica para consumir do MQTT
    public void handleMqttMessage(DroneData data) {
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
