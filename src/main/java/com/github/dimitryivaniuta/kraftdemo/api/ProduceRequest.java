package com.github.dimitryivaniuta.kraftdemo.api;

import jakarta.validation.constraints.NotBlank;

import java.util.UUID;

/**
 * Partition is optional - if provided, it must be 0..2.
 * <p>
 * eventId is optional:
 * - if omitted, backend generates it
 * - if provided, it is used for idempotency/dedup end-to-end (recommended in real systems)
 */
public record ProduceRequest(
        @NotBlank String key,
        @NotBlank String value,
        Integer partition,
        UUID eventId
) {
}
