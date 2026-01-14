package com.github.dimitryivaniuta.kraftdemo.producer;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class DemoProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${app.topic.name}")
    private String topic;

    /**
     * Sends a message.
     * <p>
     * Delivery guarantees (producer side):
     * - acks=all + enable.idempotence=true (see application.yml)
     * - makes producer retries safe (no duplicates due to retries for a single producer session)
     * <p>
     * NOTE: end-to-end exactly-once still requires idempotent processing on the consumer side
     * (implemented via Inbox + DB dedup).
     *
     * @return eventId used for dedup downstream
     */
    public UUID send(String key, String value, Integer partition, UUID eventIdOrNull) {
        UUID eventId = eventIdOrNull != null ? eventIdOrNull : UUID.randomUUID();

        var record = new ProducerRecord<String, String>(topic, partition, key, value);

        // stable idempotency key for consumers
        record.headers().add(new RecordHeader("x-event-id", eventId.toString().getBytes(StandardCharsets.UTF_8)));

        // purely for tooling visibility
        record.headers().add(new RecordHeader("x-demo", "kraft-demo".getBytes(StandardCharsets.UTF_8)));

        kafkaTemplate.send(record);
        return eventId;
    }
}
