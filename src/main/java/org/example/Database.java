package org.example;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class Database {
    private List<String> items;
    private Logger logger;

    Database() {
        this.items = new LinkedList<String>();
        logger = new Logger(Database.class.getName() + ".log");
    }

    private void handleClient(Socket clientSocket) throws Exception {
        DataInputStream inStream = new DataInputStream(clientSocket.getInputStream());
        DataOutputStream outStream = new DataOutputStream(clientSocket.getOutputStream());
        String message = inStream.readUTF();

        logger.log(message);

        if (message.equals("read")) {
            synchronized (items) {
                int start = Math.max(0, items.size() - 8);
                for (int i = start; i < items.size(); i++) {
                    String item = items.get(i);
                    String response = String.format("%d - %s\n", i, item);
                    outStream.writeUTF(response);
                }
            }
            outStream.writeUTF("END");
        } else {
            synchronized (items) {
                items.add(message);
            }
            outStream.writeUTF("OK");
        }

        clientSocket.close();
    }

    public void run(int port) throws Exception {
        ServerSocket serverSocket = new ServerSocket(port);
        ExecutorService executor = Executors.newFixedThreadPool(10);

        logger.log(String.format("Banco de dados escutando na porta %d\n", port));

        while (true) {
            Socket clientSocket = serverSocket.accept();
            executor.submit(() -> {
                try {
                    handleClient(clientSocket);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }
    }

    public static void main(String args[])  {
        Scanner s = new Scanner(System.in);
        int port;

        System.out.print("Porta: ");
        port = s.nextInt();

        try {
            Database db = new Database();
            db.run(port);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
