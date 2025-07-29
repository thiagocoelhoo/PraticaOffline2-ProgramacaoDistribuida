package org.example.publisher;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

/*
Publisher MQTT:
Um endpoint (microsserviço) que publica os dados climáticos
processados em um broker MQTT.

Consumidores podem obter esses dados em tempo real,
acompanhar a evolução do sistema e criar dashboards dinâmicos
a partir deles.

Os clientes podem usar filtros de tópicos para receber
todos os dados ou escolher dados de uma região específica.
*/

public class PublisherMQTT {
    private static final String MQTT_BROKER = "tcp://broker.emqx.io:1883";
    private static final String REALTIME_TOPIC_BASE = "drone_data/realtime";
    private MqttClient mqttClient;

    public PublisherMQTT() {
    }

    public void run() {
        try {
            setupMqttClient();
            Thread.currentThread().join();
        } catch (MqttException | InterruptedException e) {
            System.err.println("Erro ao inicializar PublisherMQTT: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Publica dados climáticos processados recebidos do Gateway para tópicos MQTT.
     */
    public void publishProcessedData(String processedData, String region) throws MqttException {
        MqttMessage message = new MqttMessage(processedData.getBytes());
        message.setQos(1);

        // Publica em um tópico geral para todos os dados em tempo real
        mqttClient.publish(REALTIME_TOPIC_BASE, message);
        System.out.println("PublisherMQTT: Publicado no tópico geral '" + REALTIME_TOPIC_BASE + "' - Mensagem: " + processedData);

        // Publica em um tópico específico da região
        String specificTopic = REALTIME_TOPIC_BASE + "/" + region.toLowerCase();
        mqttClient.publish(specificTopic, message);
        System.out.println("PublisherMQTT: Publicado no tópico específico '" + specificTopic + "' - Mensagem: " + processedData);
    }

    private void setupMqttClient() throws MqttException {
        String clientId = MqttClient.generateClientId();
        mqttClient = new MqttClient(MQTT_BROKER, clientId, new MemoryPersistence());

        MqttConnectOptions connectOptions = new MqttConnectOptions();
        connectOptions.setCleanSession(true);

        mqttClient.connect(connectOptions);
        System.out.println("PublisherMQTT conectado ao broker: " + MQTT_BROKER);

        mqttClient.setCallback(new MqttCallback() {
            @Override
            public void connectionLost(Throwable throwable) {
                System.err.println("PublisherMQTT: Conexão perdida com o broker: " + throwable.getMessage());
            }

            @Override
            public void messageArrived(String topic, MqttMessage mqttMessage) {
                // Tratar as mensagens recebidas do gateway (dados dos drones já processados)
                String processedData = new String(mqttMessage.getPayload());
                System.out.println("PublisherMQTT: Dados processados recebidos do Gateway - Tópico: " + topic + ", Mensagem: " + processedData);

                // Extrair a região do tópico para republicar nos tópicos de tempo real
                String region = topic.replace("gateway/drone-data/", "");

                // Re-publicar os dados nos tópicos de tempo real para os consumidores
                try {
                    publishProcessedData(processedData, region);
                } catch (MqttException e) {
                    System.err.println("PublisherMQTT: Erro ao re-publicar dados para tempo real: " + e.getMessage());
                    e.printStackTrace();
                }
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
            }
        });

        mqttClient.subscribe("gateway/drone-data/NORTE");
        mqttClient.subscribe("gateway/drone-data/SUL");
        mqttClient.subscribe("gateway/drone-data/LESTE");
        mqttClient.subscribe("gateway/drone-data/OESTE");
    }

    public static void main(String[] args) {
        PublisherMQTT pubMqtt = new PublisherMQTT();
        pubMqtt.run();
    }
}