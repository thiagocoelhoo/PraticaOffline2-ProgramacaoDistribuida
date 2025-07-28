package org.example.controller;

import org.example.Gateway;
import org.example.model.DroneData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
public class DataController {
    @Autowired
    private Gateway gateway;

    @PostMapping("/drone-data")
    public void receiveDroneData(@RequestBody DroneData data) {
        // Encaminha para MQTT e RabbitMQ
        gateway.processAndStoreData(data.toString(), "regiao");
    }
}
