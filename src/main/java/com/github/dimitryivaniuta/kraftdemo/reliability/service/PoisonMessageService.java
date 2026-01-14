package com.github.dimitryivaniuta.kraftdemo.reliability.service;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class PoisonMessageService {

    private final JdbcTemplate jdbc;

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void store(UUID eventIdOrNull, String groupId, ConsumerRecord<String, String> record, Exception ex) {
        jdbc.update(
                """                insert into kafka_poison_message(
                            event_id, topic, partition, offset, consumer_group,
                            record_key, record_value,
                            error_class, error_message, stacktrace
                        ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,

                eventIdOrNull,
                record.topic(),
                record.partition(),
                record.offset(),
                groupId,
                record.key(),
                record.value(),
                ex.getClass().getName(),
                safe(ex.getMessage()),
                stacktrace(ex)
        );
    }

    private static String safe(String s) {
        if (s == null) return null;
        return s.length() > 2000 ? s.substring(0, 2000) : s;
    }

    private static String stacktrace(Exception ex) {
        var sw = new StringWriter();
        ex.printStackTrace(new PrintWriter(sw));
        var st = sw.toString();
        return st.length() > 15000 ? st.substring(0, 15000) : st;
    }
}
