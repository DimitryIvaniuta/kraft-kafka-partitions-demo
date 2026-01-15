package com.github.dimitryivaniuta.kraftdemo.consumer;

import com.github.dimitryivaniuta.kraftdemo.observe.ObservedMessageStore;
import com.github.dimitryivaniuta.kraftdemo.reliability.service.InboxService;
import com.github.dimitryivaniuta.kraftdemo.reliability.service.ProcessingService;
import com.github.dimitryivaniuta.kraftdemo.reliability.util.EventIdUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Topic: 1 topic with 3 partitions (0,1,2)
 * Groups:
 * - group-a: 2 consumers
 * - consumer #1 reads partition 0
 * - consumer #2 reads partitions 1 and 2
 * - group-b: 1 consumer reads all partitions
 * <p>
 * Delivery & reliability (production-grade approach):
 * - Kafka consumption: at-least-once
 * - DB side effects: exactly-once using Inbox + unique constraint (dedup)
 * - Retries: DefaultErrorHandler with exponential backoff
 * - Poison messages: stored in DB and offset is committed so consumer continues
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class PartitionedConsumers {

    public static final String GROUP_A = "group-a";
    public static final String GROUP_B = "group-b";

    public static final String L_GROUP_A_P0 = "groupA-consumer-p0";
    public static final String L_GROUP_A_P12 = "groupA-consumer-p1p2";
    public static final String L_GROUP_B_ALL = "groupB-consumer-all";

    private final ObservedMessageStore store;
    private final InboxService inbox;
    private final ProcessingService processing;

    @KafkaListener(
            id = L_GROUP_A_P0,
            groupId = GROUP_A,
            topicPartitions = @TopicPartition(topic = "${app.topic.name}", partitions = {"0"}),
            clientIdPrefix = "groupA-p0"
    )
    public void groupAConsumerPartition0(ConsumerRecord<String, String> record) {
        handle(L_GROUP_A_P0, GROUP_A, record);
    }

    @KafkaListener(
            id = L_GROUP_A_P12,
            groupId = GROUP_A,
            topicPartitions = @TopicPartition(topic = "${app.topic.name}", partitions = {"1", "2"}),
            clientIdPrefix = "groupA-p1p2"
    )
    public void groupAConsumerPartitions1And2(ConsumerRecord<String, String> record) {
        handle(L_GROUP_A_P12, GROUP_A, record);
    }

    @KafkaListener(
            id = L_GROUP_B_ALL,
            groupId = GROUP_B,
            topics = "${app.topic.name}",
            clientIdPrefix = "groupB-all"
    )
    public void groupBConsumerAllPartitions(ConsumerRecord<String, String> record) {
        handle(L_GROUP_B_ALL, GROUP_B, record);
    }

    private void handle(String listenerId, String groupId, ConsumerRecord<String, String> record) {
        UUID eventId = EventIdUtil.resolveEventId(record);

        log.info("[{} / {}] eventId={} key={} value={} partition={} offset={}",
                listenerId, groupId, eventId, record.key(), record.value(), record.partition(), record.offset());

        // Keep the original in-memory observation store (useful for quick local demo + some tests)
        store.onRecord(listenerId, groupId, record);

        var claim = inbox.claim(eventId, groupId, record);
        if (claim.isDuplicateProcessed()) {
            log.info("[{} / {}] DUPLICATE already processed, skipping. eventId={} partition={} offset={}",
                    listenerId, groupId, eventId, record.partition(), record.offset());
            return;
        }

        processing.process(eventId, groupId, record);
    }
}
