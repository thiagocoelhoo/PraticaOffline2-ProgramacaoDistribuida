package org.example;

import java.util.Arrays;
import java.util.List;

public class DroneData {
    // int droneId;
    double pressao;
    double radiacao;
    double temperatura;
    double umidade;

    public DroneData(double pressao, double radiacao, double temperatura, double umidade) {
        // this.droneId = droneId;
        this.pressao = pressao;
        this.radiacao = radiacao;
        this.temperatura = temperatura;
        this.umidade = umidade;
    }

    public static DroneData parse(String str) {
        str = str
                .replace("(", "")
                .replace(")", "")
                .replace("{", "")
                .replace("}", "")
                .replace("[", "")
                .replace("]", "")
                .replace("-", "")
                .replace(",", "")
                .replace(";", "")
                .replace("#", "")
                .replace("//", "");

        List<String> valores = Arrays.asList(str.split("\\s"));
        List<Double> dados = valores.stream()
                .map(valor -> Double.parseDouble(valor))
                .toList();

        return new DroneData(dados.get(0), dados.get(1), dados.get(2), dados.get(3));
    }

    public String toString() {
        return String.format("[%.2f//%.2f//%.2f//%.2f]", pressao, radiacao, temperatura, umidade);
    }
}
