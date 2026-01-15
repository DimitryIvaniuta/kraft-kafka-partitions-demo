package com.github.dimitryivaniuta.kraftdemo.reliability.util;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.UUID;

/**
 * All idempotency/dedup relies on a stable eventId.
 * <p>
 * Producer always sets header 'x-event-id'.
 * If it's missing (e.g., legacy publisher), we fall back to a deterministic ID derived from topic/partition/offset,
 * which is stable for redeliveries of the same record.
 */
public final class EventIdUtil {

    private EventIdUtil() {
    }

    public static UUID resolveEventId(ConsumerRecord<String, String> record) {
        return readEventIdHeader(record).orElseGet(() -> deterministicFromRecord(record));
    }

    public static Optional<UUID> readEventIdHeader(ConsumerRecord<String, String> record) {
        Header h = record.headers().lastHeader("x-event-id");
        if (h == null) return Optional.empty();
        try {
            return Optional.of(UUID.fromString(new String(h.value(), StandardCharsets.UTF_8)));
        } catch (Exception ignore) {
            return Optional.empty();
        }
    }

    private static UUID deterministicFromRecord(ConsumerRecord<String, String> record) {
        // stable for re-deliveries: same topic/partition/offset
        String raw = record.topic() + "|" + record.partition() + "|" + record.offset();
        return UUID.nameUUIDFromBytes(raw.getBytes(StandardCharsets.UTF_8));
    }
}
