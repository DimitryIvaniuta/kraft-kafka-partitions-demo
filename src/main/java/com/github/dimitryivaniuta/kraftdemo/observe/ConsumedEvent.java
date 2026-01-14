package com.github.dimitryivaniuta.kraftdemo.observe;

public record ConsumedEvent(
        String listenerId,
        String groupId,
        String key,
        String value,
        int partition,
        long offset
) {
}
