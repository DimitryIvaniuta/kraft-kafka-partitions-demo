package com.github.dimitryivaniuta.kraftdemo.reliability.service;

import com.github.dimitryivaniuta.kraftdemo.persistence.entity.BusinessEvent;
import com.github.dimitryivaniuta.kraftdemo.persistence.repo.BusinessEventRepository;
import com.github.dimitryivaniuta.kraftdemo.reliability.exception.PoisonMessageException;
import com.github.dimitryivaniuta.kraftdemo.reliability.exception.TransientProcessingException;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The “business logic”:
 * - validate payload
 * - persist side effect into DB (unique by eventId + group)
 * - mark inbox PROCESSED
 * <p>
 * Delivery guarantee:
 * - Kafka consumer is at-least-once
 * - DB side effect is exactly-once (idempotent) thanks to unique constraint + inbox store
 */
@Service
@RequiredArgsConstructor
public class ProcessingService {

    private final BusinessEventRepository businessRepo;
    private final InboxService inbox;

    // demo helper: simulate transient failures for values containing "FLAKY"
    private final ConcurrentHashMap<UUID, AtomicInteger> flakyAttempts = new ConcurrentHashMap<>();

    @Transactional
    public void process(UUID eventId, String groupId, ConsumerRecord<String, String> record) {
        validate(record);

        simulateTransientFailureIfNeeded(eventId, record);

        try {
            businessRepo.save(BusinessEvent.builder()
                    .eventId(eventId)
                    .consumerGroup(groupId)
                    .recordKey(record.key())
                    .recordValue(record.value())
                    .receivedPartition(record.partition())
                    .receivedOffset(record.offset())
                    .createdAt(Instant.now())
                    .build());
        } catch (DataIntegrityViolationException dup) {
            // Side effect already stored (duplicate delivery) => safe to proceed as "already processed"
        }

        inbox.markProcessed(eventId, groupId);
    }

    private static void validate(ConsumerRecord<String, String> record) {
        if (record.value() == null || record.value().isBlank()) {
            throw new PoisonMessageException("Payload is blank");
        }
        if (record.value().contains("POISON")) {
            throw new PoisonMessageException("Payload marked as POISON");
        }
    }

    private void simulateTransientFailureIfNeeded(UUID eventId, ConsumerRecord<String, String> record) {
        if (!record.value().contains("FLAKY")) return;

        var counter = flakyAttempts.computeIfAbsent(eventId, id -> new AtomicInteger(0));
        int attempt = counter.incrementAndGet();

        // fail first 2 attempts, then succeed
        if (attempt <= 2) {
            throw new TransientProcessingException("Simulated transient error for flaky message, attempt=" + attempt);
        }
    }
}
