package com.github.dimitryivaniuta.kraftdemo.persistence.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(
        name = "business_event",
        uniqueConstraints = @UniqueConstraint(name = "uq_business_event", columnNames = {"event_id", "consumer_group"})
)
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class BusinessEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "event_id", nullable = false, updatable = false)
    private UUID eventId;

    @Column(name = "consumer_group", nullable = false, updatable = false, length = 128)
    private String consumerGroup;

    @Column(name = "record_key")
    private String recordKey;

    @Column(name = "record_value")
    private String recordValue;

    @Column(name = "received_partition", nullable = false, updatable = false)
    private int receivedPartition;

    @Column(name = "received_offset", nullable = false, updatable = false)
    private long receivedOffset;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;
}
