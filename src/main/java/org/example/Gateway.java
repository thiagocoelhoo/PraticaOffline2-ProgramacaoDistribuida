package org.example;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.eclipse.paho.client.mqttv3.*;
import com.rabbitmq.client.Channel;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;

public class Gateway {
    public static String mqttBroker = "tcp://broker.emqx.io:1883";
    private String clientId;
    private MqttClient mqttClient = null;

    public static String rabbitMqHost = "localhost";
    private Channel rabbitMqChannel;

    private String databaseIp;
    private int databasePort;

    private Logger logger;

    public Gateway(String databaseIp, int databasePort) {
        this.databaseIp = databaseIp;
        this.databasePort = databasePort;
        this.logger = new Logger(String.format("%s_%s.log", Gateway.class.getName(), clientId));
    }

    private String sendToDatabase(String message) throws Exception {
        Socket databaseSocket = new Socket(databaseIp, databasePort);
        DataOutputStream outStream = new DataOutputStream(databaseSocket.getOutputStream());
        DataInputStream inStream = new DataInputStream(databaseSocket.getInputStream());

        String response = "";

        try {
            outStream.writeUTF(message);

            while (!(response.endsWith("OK") || response.endsWith("END"))) {
                response += inStream.readUTF();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (databaseSocket != null) {
                databaseSocket.close();
            }
        }

        return response;
    }

    private void processAndStoreData(String droneData, String regiao) {
        try {
            // Processar e armazenar no banco de dados
            DroneData data = DroneData.parse(droneData);
            String processedData = String.format(
                    "[%.2f | %.2f | %.2f | %.2f]",
                    data.temperatura,
                    data.umidade,
                    data.pressao,
                    data.radiacao
            );
            // sendToDatabase(processedData);

            // Publicar dados via rabbitmq
            rabbitMqChannel.basicPublish("", "climatic_data_history", null, processedData.getBytes());

            // Publicar dados via mqtt
            String topico = "climatic_data/realtime/" + regiao;
            MqttMessage realTimeMessage = new MqttMessage(processedData.getBytes());
            realTimeMessage.setQos(1);

            mqttClient.publish("climatic_data/realtime", realTimeMessage); // General topic
            mqttClient.publish(topico, realTimeMessage); // Specific region topic

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void setupMqttClient() throws MqttException {
        MemoryPersistence persistence = new MemoryPersistence();
        clientId = MqttClient.generateClientId();
        mqttClient = new MqttClient(mqttBroker, clientId, persistence);
        MqttConnectOptions connectOptions = new MqttConnectOptions();
        connectOptions.setCleanSession(true);
        mqttClient.connect(connectOptions);

        if (mqttClient.isConnected()) {
            logger.log("Conectado ao broker:" + mqttBroker);
        }

        mqttClient.setCallback(new MqttCallback() {
            @Override
            public void messageArrived(String topico, MqttMessage mqttMessage) throws Exception {
                String message = new String(mqttMessage.getPayload());
                logger.log(topico + ": " + message);

                String regiao = topico
                        .replace("drone/", "")
                        .toLowerCase();
                processAndStoreData(message, regiao);
            }

            @Override
            public void connectionLost(Throwable throwable) {
                System.out.println("Connection lost");
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
            }
        });

        mqttClient.subscribe("drone/NORTE");
        mqttClient.subscribe("drone/SUL");
        mqttClient.subscribe("drone/LESTE");
        mqttClient.subscribe("drone/OESTE");
    }

    private void setupRabbitMq() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(rabbitMqHost);
        Connection connection = factory.newConnection();
        rabbitMqChannel = connection.createChannel();
        rabbitMqChannel.queueDeclare("climatic_data_history", false, false, false, null);
        logger.log("Gateway connected to RabbitMQ.");
    }

    public void run() throws Exception {
        // initializeExecutorService();
        setupMqttClient();
        setupRabbitMq();
        logger.log("Gateway inicializado.");

        // Manter servidor rodando
        while (true) {
            Thread.sleep(1000);
        }
    }

    public static void main(String[] args) {
        String databaseIp = "localhost";
        int databasePort = 8080;

        Gateway gateway = new Gateway(databaseIp, databasePort);
        try {
            gateway.run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
