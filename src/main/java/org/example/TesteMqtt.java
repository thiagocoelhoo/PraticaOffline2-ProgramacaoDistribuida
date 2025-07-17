package org.example;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.util.Random;

public class TesteMqtt {
     // String broker = "tcp://mqtt.eclipseprojects.io:1883";
     String broker = "tcp://broker.emqx.io:1883";
//    String broker = "tcp://localhost:1883";

    public TesteMqtt() throws InterruptedException {
        this.init();
    }

    public void init() throws InterruptedException {

        System.out.println("[*] Inicializando um publisher...");

        try {
            /*
             MqttClient.generateClientId(): gera um ID único para identificar
             o cliente que está se conectando ao broker MQTT.
             Esse ID é necessário para que o broker saiba quem
             está publicando ou assinando as mensagens.
             */

            String idCliente = MqttClient.generateClientId();
            System.out.println("[*] ID do Cliente: " + idCliente);
            /*
            Cria um cliente Mqtt
             */
            MqttClient clienteMqtt = new MqttClient(broker, idCliente);

            /*
            Um objeto MqttConnectOptions é criado para configurar
            as opções de conexão.
            */
            MqttConnectOptions opcoesConexao = new MqttConnectOptions();

            /*
            setCleanSession(true) indica que o cliente deseja
            uma sessão limpa, ou seja, não vai manter informações
            de estado persistente.
            Toda vez que se conectar, será uma nova sessão
            sem armazenar assinaturas ou mensagens anteriores.
             */
            opcoesConexao.setCleanSession(true);

            // Conectar cliente ao broker
            System.out.println("[*] Conectando-se ao broker " + broker);
            /*
            O cliente é conectado ao broker MQTT usando as opções
            de conexão configuradas.
             */
            clienteMqtt.connect(opcoesConexao);
            /*
            clienteMqtt.connect(opcoesConexao) tenta estabelecer
            a conexão com o broker.
            Imprime no console se o cliente foi conectado
            com sucesso
            (clienteMqtt.isConnected() retorna true se a
            conexão foi estabelecida).
             */
            System.out.println("[*] Conectado: " + clienteMqtt.isConnected());

            // Cria uma mensagem Mqtt
            String conteudo = "Faça alguma coisa: " + new Random().nextInt(1000);
            /*
            A mensagem é convertida para um array de bytes
            (conteudo.getBytes()) e passada para o
            construtor de MqttMessage (as mensagens MQTT
            são transmitidas em formato binário).
             */
            MqttMessage mensagem = new MqttMessage(conteudo.getBytes());

            // Configura a QoS das mensagens
            /* qos = 0;
             * 		entrega no máximo uma vez
             *      não há confirmação de entrega
             *      não há armazenamento
             *      suscetível a perdas
             * qos = 1;
             *      modo padrão
             *      mensagem entregue pelo menos uma vez
             *      emissor reenvia mensagem até receber uma confirmação
             *      mensagem é armazenada localmente até ser processada
             *      mensagem é excluída do receptor após processamento
             * qos = 2;
             *      entrega exatamente uma vez
             *      mensagem é armazenada localmente até ser processada
             *      mais seguro e mais lento
             *      dois pares de transmisão: um para o receptor confirmar
             *          recebimento e outro para o receptor confirmar processamento
             */

            mensagem.setQos(0);

            System.out.println("[*] Publicando mensagem: " + conteudo);
            /*
            publish() envia a mensagem para o broker,
            que a roteia para os clientes que estejam
            assinados no tópico "mqtt/pratica1".
             */
            clienteMqtt.publish("mqtt/pratica1", mensagem);

            // Desconecta o cliente
            clienteMqtt.disconnect();

            System.out.println("[*] Mensagem publicada. Saindo...");

            System.exit(0);

        } catch (MqttException me) {

            System.out.println("razão: "+me.getReasonCode());
            System.out.println("mensagem: "+me.getMessage());
            System.out.println("loc: "+me.getLocalizedMessage());
            System.out.println("causa: "+me.getCause());
            System.out.println("exception: "+me);
        }
    }

    public static void main(String[] args) throws InterruptedException {

        new TesteMqtt();

    }
}
