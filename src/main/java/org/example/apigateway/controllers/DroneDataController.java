package org.example.apigateway.controllers;

import org.example.apigateway.models.DroneData;
import org.example.apigateway.services.MqttConsumerService;
import org.example.apigateway.services.RabbitMqConsumerService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/drone-data")
public class DroneDataController {
    private static final List<String> REGIONS = Arrays.asList("norte", "sul", "leste", "oeste");
    private final MqttConsumerService mqttConsumerService;
    private final RabbitMqConsumerService rabbitMqConsumerService; 

    public DroneDataController(MqttConsumerService mqttConsumerService,
                               RabbitMqConsumerService rabbitMqConsumerService) {
        this.mqttConsumerService = mqttConsumerService;
        this.rabbitMqConsumerService = rabbitMqConsumerService;
    }

    private boolean isValidRegion(String region) {
        return REGIONS.contains(region.toLowerCase());
    }

    private <T> ResponseEntity<Map<String, Object>> createSuccessResponse(List<T> items, HttpStatus status) {
        Map<String, Object> content = new HashMap<>();
        Map<String, Object> response = new HashMap<>();
        
        content.put("items", items);
        content.put("count", items.size());
        
        response.put("content", content);
        response.put("error", null);
        
        return new ResponseEntity<>(response, status);
    }

    private ResponseEntity<Map<String, Object>> createErrorResponse(String errorMessage, HttpStatus status) {
        Map<String, Object> response = new HashMap<>();
        Map<String, Object> content = new HashMap<>();
        
        response.put("error", errorMessage);
        response.put("content", content);
        return new ResponseEntity<>(response, status);
    }

    @GetMapping("/realtime/{region}")
    public ResponseEntity<Map<String, Object>> getRealtimeDataByRegion(@PathVariable String region) {
        if (!isValidRegion(region)) {
            return createErrorResponse(
                "Região inválida.",
                HttpStatus.BAD_REQUEST
            );
        }

        DroneData data = mqttConsumerService.getLatestRealtimeData(region);

        if (data == null) {
            return createErrorResponse(
                "Nenhum dado em tempo real disponível para a região: " + region,
                HttpStatus.NO_CONTENT
            );
        }
    
        return createSuccessResponse(
            Collections.singletonList(data),
            HttpStatus.OK
        );
    }

    @GetMapping("/realtime/all")
    public ResponseEntity<Map<String, Object>> getAllRealtimeData() {
        Map<String, DroneData> allDataMap = mqttConsumerService.getAllLatestRealtimeData();
        List<DroneData> allDataList = allDataMap.values().stream().collect(Collectors.toList());

        if (allDataList.isEmpty()) {
            return createErrorResponse(
                "Nenhum dado em tempo real disponível.",
                HttpStatus.NO_CONTENT
            );
        }
        
        return createSuccessResponse(
            allDataList,
            HttpStatus.OK
        );
    }

    @GetMapping("/history/{region}")
    public ResponseEntity<Map<String, Object>> getHistoricalDataByRegion(@PathVariable String region) {
        if (!isValidRegion(region)) {
            return createErrorResponse(
                "Região inválida.",
                HttpStatus.BAD_REQUEST
            );
        }

        List<DroneData> historicalData = rabbitMqConsumerService.getHistoricalDataByRegion(region);
        
        if (historicalData.isEmpty()) {
            return createErrorResponse(
                "Nenhum dado histórico disponível para a região: " + region,
                HttpStatus.NO_CONTENT
            );
        }

        return createSuccessResponse(
            historicalData,
            HttpStatus.OK
        );
    }

    @GetMapping("/history/all")
    public ResponseEntity<Map<String, Object>> getAllHistoricalData() {
        List<DroneData> allHistoricalData = rabbitMqConsumerService.getAllHistoricalData();
        
        if (allHistoricalData.isEmpty()) {
            return createErrorResponse(
                "Nenhum dado histórico disponível.",
                HttpStatus.NO_CONTENT
            );
        }
        
        return createSuccessResponse(
            allHistoricalData,
            HttpStatus.OK
        );
    }
}