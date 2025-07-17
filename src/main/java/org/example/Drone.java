package org.example;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.util.Scanner;

enum DroneType {
    NORTE,
    SUL,
    LESTE,
    OESTE
}

public class Drone {
    private DroneType regiao;
    private MqttClient client;
    private String clientId;
    private MqttConnectOptions options;

    public Drone(DroneType regiao, String broker) throws MqttException {
        this.regiao = regiao;
        this.clientId = MqttClient.generateClientId();
        this.client = new MqttClient(broker, this.clientId);
        this.options = new MqttConnectOptions();
        this.options.setCleanSession(true);
        client.connect(options);
    }

    private DroneData collectData() {
        double pressao = Math.random() * 1000 + 950; // hPa
        double radiacao = Math.random() * 1000; // W/m^2
        double temperatura = Math.random() * 30 + 15; // °C
        double umidade = Math.random() * 100; // %
        return new DroneData(pressao, radiacao, temperatura, umidade);
    }

    private String getDroneData() {
        DroneData data = collectData();
        switch (regiao) {
            case NORTE:
                return String.format("%f-%f-%f-%f", data.pressao, data.radiacao, data.temperatura, data.umidade);
            case SUL:
                return String.format("(%f;%f;%f;%f)", data.pressao, data.radiacao, data.temperatura, data.umidade);
            case LESTE:
                return String.format("{%f,%f,%f,%f}", data.pressao, data.radiacao, data.temperatura, data.umidade);
            case OESTE:
                return String.format("%f#%f#%f#%f", data.pressao, data.radiacao, data.temperatura, data.umidade);
            default:
                return "";
        }
    }

    private void sendData() throws MqttException {
        String data = getDroneData();
        MqttMessage message = new MqttMessage(data.getBytes());
        String topic = "drone/" + regiao.toString();
        if (client.isConnected()) {
            client.publish(topic, message);
        } else {
            System.out.println("Not connected");
        }
    }

    public void run() throws InterruptedException, MqttException {
        while (true) {
            sendData();
             Thread.sleep((long) Math.floor(2000 + Math.random() * 3000));
        }
        // client.disconnect();
    }

    public static void main(String[] args) throws Exception{
        Scanner s = new Scanner(System.in);
        String broker = "tcp://broker.emqx.io:1883";

        System.out.print("Broker: " + broker + "\n");
        // broker = s.nextLine();

        System.out.println("Escolha a região do drone");
        System.out.println("1 - NORTE");
        System.out.println("2 - SUL");
        System.out.println("3 - LESTE");
        System.out.println("4 - OESTE");

        System.out.print("Região:");
        int regiao = s.nextInt();
        Drone drone;

        switch (regiao) {
            case 1:
                drone = new Drone(DroneType.NORTE, broker);
                break;
            case 2:
                drone = new Drone(DroneType.SUL, broker);
                break;
            case 3:
                drone = new Drone(DroneType.LESTE, broker);
                break;
            case 4:
                drone = new Drone(DroneType.OESTE, broker);
                break;
            default: {
                System.out.println("Região inválida.");
                return;
            }
        }

        try {
            drone.run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
