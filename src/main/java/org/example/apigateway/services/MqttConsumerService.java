package org.example.apigateway.services;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.springframework.stereotype.Service;
import org.example.apigateway.models.DroneData;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class MqttConsumerService {
    private static final String MQTT_BROKER = "tcp://broker.emqx.io:1883";
    private static final String REALTIME_TOPIC_BASE = "drone_data/realtime";
    private static final String[] REALTIME_REGION_TOPICS = {
        REALTIME_TOPIC_BASE + "/norte",
        REALTIME_TOPIC_BASE + "/sul",
        REALTIME_TOPIC_BASE + "/leste",
        REALTIME_TOPIC_BASE + "/oeste"
    };

    private MqttClient mqttClient;
    private final Map<String, DroneData> droneData = new ConcurrentHashMap<>(); // [{regiao: data}]

    @PostConstruct
    public void init() {
        try {
            setupMqttConsumer();
        } catch (MqttException e) {
            System.err.println("MqttConsumerService: Erro ao iniciar o consumidor MQTT");
            e.printStackTrace();
        }
    }

    private void setupMqttConsumer() throws MqttException {
        String clientId = MqttClient.generateClientId();
        mqttClient = new MqttClient(MQTT_BROKER, clientId, new MemoryPersistence());
        
        MqttConnectOptions connectOptions = new MqttConnectOptions();
        connectOptions.setCleanSession(true);

        mqttClient.connect(connectOptions);
        mqttClient.setCallback(new MqttCallback() {
            @Override
            public void connectionLost(Throwable throwable) {
                System.err.println("MqttConsumerService: Conexão MQTT perdida");
            }

            @Override
            public void messageArrived(String topic, MqttMessage mqttMessage) {
                String message = new String(mqttMessage.getPayload());
                // Parse da mensagem para um objeto DroneData
                if (topic.startsWith(REALTIME_TOPIC_BASE + "/")) {
                    String region = topic.replace(REALTIME_TOPIC_BASE + "/", "").toUpperCase();
                    try {
                        DroneData parsedData = DroneData.parseFromString(message);
                        droneData.put(region.toLowerCase(), parsedData);
                    } catch (IllegalArgumentException e) {
                        e.printStackTrace();
                    }
                }
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {
            }
        });

        // Subscrever aos tópicos
        for (String topic : REALTIME_REGION_TOPICS) {
            mqttClient.subscribe(topic);
            System.out.println("MqttConsumerService: Subscrito ao tópico: " + topic);
        }
        mqttClient.subscribe(REALTIME_TOPIC_BASE);
    }

    /**
     * Retorna os últimos dados em tempo real para todas as regiões.
     */
    public Map<String, DroneData> getAllLatestRealtimeData() {
        return new ConcurrentHashMap<>(droneData);
    }

    public DroneData getLatestRealtimeData(String region) {
        return droneData.get(region);
    }

    @PreDestroy
    public void destroy() {
        try {
            if (mqttClient != null && mqttClient.isConnected()) {
                mqttClient.disconnect();
                mqttClient.close();
            }
        } catch (MqttException e) {
            System.err.println("MqttConsumerService: Erro ao fechar conexão MQTT");
        }
    }
}