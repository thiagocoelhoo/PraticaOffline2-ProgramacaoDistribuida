package org.example;

import org.eclipse.paho.client.mqttv3.*;

public class Gateway {
    private String broker = "tcp://broker.emqx.io:1883";
    private String clientId;
    private MqttClient client = null;
    private MqttConnectOptions options = null;

    public Gateway() {
    }

    public void init() throws MqttException {
        this.clientId = MqttClient.generateClientId();
        this.client = new MqttClient(this.broker, this.clientId);
        this.options = new MqttConnectOptions();
        this.options.setCleanSession(true);
        this.client.connect(options);

        if (this.client.isConnected()) {
            System.out.println("Connected to broker: " + this.broker);
        }
        else {
            System.out.println("Not connected to broker: " + this.broker);
        }

        this.client.setCallback(new MqttCallback() {
            @Override
            public void messageArrived(String topico, MqttMessage mqttMessage) throws Exception {
                String message = new String(mqttMessage.getPayload());
                System.out.println(topico + ": " + message);
            }

            @Override
            public void connectionLost(Throwable throwable) {
                System.out.println("Connection lost");
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
            }
        });

        client.subscribe("drone/NORTE");
        client.subscribe("drone/SUL");
        client.subscribe("drone/LESTE");
        client.subscribe("drone/OESTE");
    }

    public static void main(String[] args) {
        Gateway gateway = new Gateway();
        try {
            gateway.init();
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }
}
