package com.github.dimitryivaniuta.kraftdemo.api;

import com.github.dimitryivaniuta.kraftdemo.producer.DemoProducer;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/messages")
@RequiredArgsConstructor
public class ProducerController {

    private final DemoProducer producer;

    @PostMapping
    public ResponseEntity<ProduceResponse> produce(@Valid @RequestBody ProduceRequest req) {
        var eventId = producer.send(req.key(), req.value(), req.partition(), req.eventId());
        return ResponseEntity.accepted().body(new ProduceResponse(eventId));
    }
}
