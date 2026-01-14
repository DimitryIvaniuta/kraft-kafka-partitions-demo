package com.github.dimitryivaniuta.kraftdemo.reliability.service;

import lombok.RequiredArgsConstructor;
import lombok.Value;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;
import java.util.UUID;

/**
 * Inbox pattern:
 * - at-least-once consumption (Kafka) + exactly-once side effects (DB) via idempotency store.
 * <p>
 * Idempotency scope:
 * - per consumer group (each group is an independent stream)
 */
@Service
@RequiredArgsConstructor
public class InboxService {

    private final JdbcTemplate jdbc;

    public enum InboxStatus {PROCESSING, PROCESSED, FAILED}

    @Value
    public static class ClaimResult {
        boolean duplicateProcessed;
        int attempt;
        InboxStatus status;
    }

    /**
     * Claim a record for processing and bump attempt counter.
     * <p>
     * MUST commit even if processing later fails, otherwise attempt count would reset to 1 on every retry.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public ClaimResult claim(UUID eventId, String groupId, ConsumerRecord<String, String> record) {
        // Try insert (first delivery for this group)
        int inserted = jdbc.update(
                """                insert into kafka_event_inbox(event_id, consumer_group, topic, partition, offset, status, attempt, updated_at)
                        values (?, ?, ?, ?, ?, 'PROCESSING', 1, now())
                        on conflict (event_id, consumer_group) do nothing
                        """,

                eventId,
                groupId,
                record.topic(),
                record.partition(),
                record.offset()
        );

        if (inserted == 1) {
            return new ClaimResult(false, 1, InboxStatus.PROCESSING);
        }

        // Existing event for this group => check status and bump attempt if needed
        var row = jdbc.queryForMap(
                "select status, attempt from kafka_event_inbox where event_id = ? and consumer_group = ?",
                eventId,
                groupId
        );

        InboxStatus status = InboxStatus.valueOf(((String) row.get("status")).toUpperCase());
        int attempt = ((Number) row.get("attempt")).intValue();

        if (status == InboxStatus.PROCESSED) {
            return new ClaimResult(true, attempt, status);
        }

        int nextAttempt = attempt + 1;
        jdbc.update(
                """                update kafka_event_inbox
                           set status = 'PROCESSING',
                               attempt = ?,
                               topic = ?,
                               partition = ?,
                               offset = ?,
                               updated_at = now()
                         where event_id = ? and consumer_group = ?
                        """,

                nextAttempt,
                record.topic(),
                record.partition(),
                record.offset(),
                eventId,
                groupId
        );

        return new ClaimResult(false, nextAttempt, InboxStatus.PROCESSING);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void markProcessed(UUID eventId, String groupId) {
        jdbc.update(
                """                update kafka_event_inbox
                           set status = 'PROCESSED',
                               updated_at = now(),
                               last_error = null
                         where event_id = ? and consumer_group = ?
                        """,

                eventId,
                groupId
        );
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void markFailed(UUID eventId, String groupId, String error) {
        jdbc.update(
                """                update kafka_event_inbox
                           set status = 'FAILED',
                               updated_at = now(),
                               last_error = ?
                         where event_id = ? and consumer_group = ?
                        """,

                error,
                eventId,
                groupId
        );
    }

    public Optional<InboxStatus> getStatus(UUID eventId, String groupId) {
        var rows = jdbc.query(
                "select status from kafka_event_inbox where event_id = ? and consumer_group = ?",
                (rs, rowNum) -> InboxStatus.valueOf(rs.getString(1).toUpperCase()),
                eventId,
                groupId
        );
        return rows.stream().findFirst();
    }
}
