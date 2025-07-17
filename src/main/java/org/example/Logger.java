package org.example;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Logger {
    private final String path;

    public Logger(String path) {
        this.path = path;
    }

    public void log(String message) throws IOException {
        synchronized (path) {
            File file = new File(path);
            FileWriter fw = new FileWriter(file, true);

            Date now = new Date();
            SimpleDateFormat dateFormatter = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss");
            String timestamp = dateFormatter.format(now);

            message = String.format("[%s] [%s] %s\n", timestamp, Thread.currentThread().getName(), message);
            fw.write(message);
            System.out.print(message);
            fw.close();
        }
    }

    public void skipLine() throws IOException {
        synchronized (path) {
            File file = new File(path);
            FileWriter fw = new FileWriter(file, true);
            fw.write("\n");
            System.out.print("\n");
            fw.close();
        }
    }

    public void logError(Exception e) throws IOException {
        log("[ERROR] " + e.getMessage());
    }
}
