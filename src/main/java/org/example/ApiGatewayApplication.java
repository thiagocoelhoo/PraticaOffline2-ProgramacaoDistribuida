package org.example;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

// @RestController
// public class ApiGatewayApplication {
//     private List<DroneData> latestData = new ArrayList<>();

//     @PostMapping("/data")
//     public void receiveData(@RequestBody DroneData data) {
//         latestData.add(data);
//         System.out.println("Dados recebidos no API Gateway: " + data);
//     }

//     @GetMapping("/data")
//     public List<DroneData> getData() {
//         return latestData;
//     }
// }

@SpringBootApplication
public class ApiGatewayApplication {
    public static void main(String[] args) {
        SpringApplication.run(ApiGatewayApplication.class, args);
    }
}
