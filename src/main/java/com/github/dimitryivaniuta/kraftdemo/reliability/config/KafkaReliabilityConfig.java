package com.github.dimitryivaniuta.kraftdemo.reliability.config;

import com.github.dimitryivaniuta.kraftdemo.reliability.exception.PoisonMessageException;
import com.github.dimitryivaniuta.kraftdemo.reliability.service.InboxService;
import com.github.dimitryivaniuta.kraftdemo.reliability.service.PoisonMessageService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.ConsumerAwareRecordRecoverer;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.UUID;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class KafkaReliabilityConfig {

    private final PoisonMessageService poison;
    private final InboxService inbox;

    /**
     * Production-grade retry strategy:
     * - Retry transient errors with exponential backoff
     * - Do NOT retry permanent validation errors (poison)
     * - After retries exhausted -> persist poison message + mark FAILED + commit offset (so consumer continues)
     */
    @Bean
    public DefaultErrorHandler kafkaErrorHandler() {
        var backoff = new ExponentialBackOffWithMaxRetries(5);
        backoff.setInitialInterval(300);
        backoff.setMultiplier(2.0);
        backoff.setMaxInterval(5_000);

        ConsumerAwareRecordRecoverer recoverer = (ConsumerRecord<?, ?> rec, Exception ex, Consumer<?, ?> consumer) -> {
            @SuppressWarnings("unchecked")
            var record = (ConsumerRecord<String, String>) rec;

            String groupId = consumer.groupMetadata() != null ? consumer.groupMetadata().groupId() : "unknown-group";
            UUID eventId = tryReadEventId(record).orElse(null);

            log.error("[RECOVER] group={} topic={} partition={} offset={} eventId={} ex={}",
                    groupId, record.topic(), record.partition(), record.offset(), eventId, ex.toString());

            if (eventId != null) {
                inbox.markFailed(eventId, groupId, ex.getClass().getSimpleName() + ": " + safe(ex.getMessage()));
            }
            poison.store(eventId, groupId, record, ex);
        };

        var handler = new DefaultErrorHandler(recoverer, backoff);
        handler.setCommitRecovered(true); // IMPORTANT: move past poison message after recovery
        handler.addNotRetryableExceptions(PoisonMessageException.class, IllegalArgumentException.class);

        handler.setRetryListeners((record, ex, deliveryAttempt) -> log.warn(
                "[RETRY] topic={} partition={} offset={} attempt={} ex={}",
                record.topic(), record.partition(), record.offset(), deliveryAttempt, ex.toString()
        ));

        return handler;
    }

    private static Optional<UUID> tryReadEventId(ConsumerRecord<String, String> record) {
        Header h = record.headers().lastHeader("x-event-id");
        if (h == null) return Optional.empty();
        try {
            return Optional.of(UUID.fromString(new String(h.value(), StandardCharsets.UTF_8)));
        } catch (Exception ignore) {
            return Optional.empty();
        }
    }

    private static String safe(String s) {
        if (s == null) return null;
        return s.length() > 1000 ? s.substring(0, 1000) : s;
    }
}
