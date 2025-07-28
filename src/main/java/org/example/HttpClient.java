package org.example;

import org.springframework.web.client.RestTemplate;
import org.example.model.DroneData;

public class HttpClient {
    public static void main(String[] args) {
        RestTemplate restTemplate = new RestTemplate();
        String apiGatewayUrl = "http://localhost:8081/data";
        try {
            DroneData[] data = restTemplate.getForObject(apiGatewayUrl, DroneData[].class);
            for (DroneData d : data) {
                System.out.println(d);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
