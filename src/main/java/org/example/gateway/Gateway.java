package org.example.gateway;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.io.IOException;

import org.example.drone.DroneData;
import org.example.Logger;

public class Gateway {
    private static final String MQTT_BROKER = "tcp://broker.emqx.io:1883";
    private static final String RABBITMQ_HOST = "localhost";

    private static final String GATEWAY_TO_RABBITMQ_PUBLISHER_QUEUE = "drone_data_history"; 
    private static final String GATEWAY_TO_MQTT_PUBLISHER_TOPIC_BASE = "gateway/drone-data/"; 
    private static final String[] DRONE_SUBSCRIPTION_TOPICS = {
        "drone/NORTE",
        "drone/SUL",
        "drone/LESTE",
        "drone/OESTE"
    };

    private MqttClient mqttClient;
    private Channel rabbitMqChannel;
    
    private final String databaseIp;
    private final int databasePort;
    private final Logger logger;

    public Gateway(String databaseIp, int databasePort) {
        this.databaseIp = databaseIp;
        this.databasePort = databasePort;
        this.logger = new Logger(String.format("%s.log", Gateway.class.getName()));
    }

    public Gateway() {
        this("localhost", 2000);
    }

    private String saveData(String message) throws IOException {
        try (Socket databaseSocket = new Socket(databaseIp, databasePort);
            DataOutputStream outStream = new DataOutputStream(databaseSocket.getOutputStream());
            DataInputStream inStream = new DataInputStream(databaseSocket.getInputStream())) {

            outStream.writeUTF(message);
            StringBuilder responseBuilder = new StringBuilder();
            String part;
            do {
                part = inStream.readUTF();
                responseBuilder.append(part);
            } while (!(part.endsWith("OK") || part.endsWith("END")));

            return responseBuilder.toString();
        }
    }

    public void processAndDistributeData(String droneDataPayload, String region) {
        try {
            // Processar os dados brutos do drone
            String processedData = formatDroneData(droneDataPayload, region);

            // Armazenar no banco de dados
            saveData(processedData);
            
            // Publicar dados nos endpoints
            publishRabbitMQ(processedData);
            publishMQTT(processedData, region);

        } catch (IOException e) {
            logger.log("Erro de I/O ao processar e distribuir dados: " + e.getMessage());
            e.printStackTrace();
        } catch (MqttException e) {
            logger.log("Erro MQTT ao processar e distribuir dados: " + e.getMessage());
            e.printStackTrace();
        } catch (Exception e) {
            logger.log("Erro inesperado ao processar e distribuir dados: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Formata os dados brutos do drone para o formato padronizado.
     */
    private String formatDroneData(String data, String region) {
        try {
            DroneData droneData = DroneData.parse(data);
            return String.format(
                    "[%s | %.2f | %.2f | %.2f | %.2f]",
                    region,
                    droneData.getTemperatura(),
                    droneData.getUmidade(),
                    droneData.getPressao(),
                    droneData.getRadiacao()
            );
        } catch (Exception e) {
            logger.log("Erro ao formatar dados do drone '" + data + "': " + e.getMessage());
            return null;
        }
    }

    /**
     * Publica os dados processados na fila do RabbitMQ para o Publisher RabbitMQ.
     */
    private void publishRabbitMQ(String processedData) throws IOException {
        rabbitMqChannel.basicPublish("", GATEWAY_TO_RABBITMQ_PUBLISHER_QUEUE, null, processedData.getBytes());
        logger.log("Dados enviados para Publisher RabbitMQ (fila: " + GATEWAY_TO_RABBITMQ_PUBLISHER_QUEUE + "): " + processedData);
    }

    /**
     * Publica os dados processados no tópico MQTT para o Publisher MQTT.
     */
    private void publishMQTT(String processedData, String region) throws MqttException {
        String topic = GATEWAY_TO_MQTT_PUBLISHER_TOPIC_BASE + region.toUpperCase();
        MqttMessage message = new MqttMessage(processedData.getBytes());
        message.setQos(1);
        
        mqttClient.publish(topic, message);
        logger.log("Dados enviados para Publisher MQTT (tópico: " + topic + "): " + processedData);
    }

    
    private void setupMqttClient() throws MqttException {
        String clientId = MqttClient.generateClientId();
        mqttClient = new MqttClient(MQTT_BROKER, clientId, new MemoryPersistence());
        MqttConnectOptions connectOptions = new MqttConnectOptions();
        connectOptions.setCleanSession(true);
        mqttClient.connect(connectOptions);

        if (mqttClient.isConnected()) {
            logger.log("Gateway conectado ao broker MQTT: " + MQTT_BROKER);
        }

        mqttClient.setCallback(new MqttCallback() {
            @Override
            public void messageArrived(String topic, MqttMessage mqttMessage) {
                String message = new String(mqttMessage.getPayload());
                logger.log("Gateway: Mensagem MQTT recebida dos drones - Tópico: " + topic + ", Mensagem: " + message);

                // Extrai a região do tópico do drone (ex: "drone/NORTE" -> "NORTE")
                String region = topic.replace("drone/", "").toUpperCase();
                
                // Processa e distribui os dados para os Publishers
                processAndDistributeData(message, region);
            }

            @Override
            public void connectionLost(Throwable throwable) {
                logger.log("Gateway: Conexão MQTT perdida: " + throwable.getMessage());
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
            }
        });

        for (String topic : DRONE_SUBSCRIPTION_TOPICS) {
            mqttClient.subscribe(topic);
        }
    }

    private void setupRabbitMq() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(RABBITMQ_HOST);
        Connection connection = factory.newConnection();
        rabbitMqChannel = connection.createChannel();
        
        // Declara a fila onde o Gateway publicará para o Publisher RabbitMQ
        // Garante que a fila exista.
        rabbitMqChannel.queueDeclare(
            GATEWAY_TO_RABBITMQ_PUBLISHER_QUEUE, 
            false,
            false,
            false,
            null
        );
        logger.log("Gateway conectado ao RabbitMQ e fila declarada: " + GATEWAY_TO_RABBITMQ_PUBLISHER_QUEUE);
    }

    public void run() throws Exception {
        setupMqttClient();
        setupRabbitMq();
        logger.log("Gateway inicializado e rodando. Aguardando dados dos drones...");
        Thread.currentThread().join();
    }

    public static void main(String[] args) {
        Gateway gateway = new Gateway();
        
        try {
            gateway.run();
        } catch (Exception e) {
            gateway.logger.log("Gateway falhou ao iniciar: " + e.getMessage());
            e.printStackTrace();
        }
    }
}