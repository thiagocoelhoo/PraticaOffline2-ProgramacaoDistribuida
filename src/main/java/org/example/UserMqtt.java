package org.example;

import java.util.Scanner;
import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

public class UserMqtt {
    private String mqttBroker = "tcp://broker.emqx.io:1883";
    private String clientId;
    private MqttClient mqttClient = null;

    UserMqtt() {
    }

    public void run() throws MqttException, InterruptedException {
        Scanner scanner = new Scanner(System.in);

        MemoryPersistence persistence = new MemoryPersistence();
        clientId = MqttClient.generateClientId();
        mqttClient = new MqttClient(mqttBroker, clientId, persistence);
        MqttConnectOptions connectOptions = new MqttConnectOptions();
        connectOptions.setCleanSession(true);

        System.out.println("Conectando ao Broker MQTT: " + mqttBroker);
        mqttClient.connect(connectOptions);
        System.out.println("Conectado.");

        while (true) {
            System.out.println("\nEscolha uma opção para receber dados em tempo real:");
            System.out.println("1 - Receber todos os dados em tempo real");
            System.out.println("2 - Receber dados em tempo real de uma região específica (Norte, Sul, Leste, Oeste)");
            System.out.println("3 - Sair");
            System.out.print("Opção: ");

            int option = scanner.nextInt();
            scanner.nextLine();

            String topic = "";
            switch (option) {
                case 1:
                    topic = "drone_data/realtime";
                    break;

                case 2:
                    System.out.print("Informe a região (Norte, Sul, Leste, Oeste): ");
                    String region = scanner.nextLine().toLowerCase();

                    if (region.equals("norte") || region.equals("sul") || region.equals("leste") || region.equals("oeste")) {
                        topic = "drone_data/realtime/" + region;
                    }
                    else {
                        System.out.println("Região inválida. Por favor, escolha entre Norte, Sul, Leste ou Oeste.");
                        continue;
                    }
                    break;

                case 3:
                    System.out.println("Encerrando o cliente.");
                    if (mqttClient != null && mqttClient.isConnected()) {
                        mqttClient.disconnect();
                        mqttClient.close();
                    }
                    return;
                default:
                    System.out.println("Opção inválida. Tente novamente.");
                    continue;
            }

            System.out.println("Inscrevendo-se no tópico: " + topic);
            mqttClient.subscribe(topic, 1, new IMqttMessageListener() {
                @Override
                public void messageArrived(String topic, MqttMessage message) throws Exception {
                    System.out.println("Dados em tempo real (" + topic + "): " + new String(message.getPayload()));
                }
            });
            System.out.println("Aguardando dados em tempo real. Pressione Enter para mudar a assinatura ou sair...");
            scanner.nextLine();
            mqttClient.unsubscribe(topic);
        }
    }

    public static void main(String args[]) {
        Scanner s = new Scanner(System.in);
        try {
            UserMqtt client = new UserMqtt();
            client.run();
        } catch (MqttException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            s.close();
        }
    }
}
