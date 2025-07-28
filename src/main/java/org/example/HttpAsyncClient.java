package org.example;

import org.example.model.DroneData;
import org.springframework.web.reactive.function.client.WebClient;

import reactor.core.publisher.Mono;

public class HttpAsyncClient {
    public static void main(String[] args) {
        WebClient webClient = WebClient.create("http://localhost:8081");
        Mono<DroneData[]> response = webClient.get()
                .uri("/data")
                .retrieve()
                .bodyToMono(DroneData[].class);
        response.subscribe(dataArr -> {
            for (DroneData d : dataArr) {
                System.out.println(d);
            }
        });
        // Aguarda para ver o resultado
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
