package org.example.apigateway.models;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class DroneData {
    private String regiao;
    private double pressao;
    private double radiacao;
    private double temperatura;
    private double umidade;

    public DroneData(String regiao, double pressao, double radiacao, double temperatura, double umidade) {
        this.regiao = regiao;
        this.pressao = pressao;
        this.radiacao = radiacao;
        this.temperatura = temperatura;
        this.umidade = umidade;
    }

    public static DroneData parseFromString(String dataString) {
        String cleanedString = dataString.trim().replace("[", "").replace("]", "").trim();
        String[] parts = cleanedString.split("\\|");

        try {
            String region = parts[0].trim();
            double temperatura = Double.parseDouble(parts[1].trim());
            double umidade = Double.parseDouble(parts[2].trim());
            double pressao = Double.parseDouble(parts[3].trim());
            double radiacao = Double.parseDouble(parts[4].trim());

            return new DroneData(region.toUpperCase(), pressao, radiacao, temperatura, umidade);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Erro ao parsear valores numéricos na string: " + dataString, e);
        }
    }

    @Override
    public String toString() {
        return "DroneData{" +
               "regiao=\"" + regiao + '"' +
               ", pressao=" + pressao +
               ", radiacao=" + radiacao +
               ", temperatura=" + temperatura +
               ", umidade=" + umidade +
               '}';
    }
}