package com.github.dimitryivaniuta.kraftdemo.observe;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Small in-memory store used by integration tests and for debugging.
 * In real systems you'd push metrics/logs and not keep records in-memory.
 */
@Component
public class ObservedMessageStore {

    private final Map<String, List<ConsumedEvent>> eventsByListener = new ConcurrentHashMap<>();
    private final Map<String, CountDownLatch> latchByListener = new ConcurrentHashMap<>();

    public void reset() {
        eventsByListener.clear();
        latchByListener.clear();
    }

    public void expect(String listenerId, int expectedCount) {
        latchByListener.put(listenerId, new CountDownLatch(expectedCount));
    }

    public void onRecord(String listenerId, String groupId, ConsumerRecord<String, String> r) {
        eventsByListener.computeIfAbsent(listenerId, __ -> new CopyOnWriteArrayList<>())
                .add(new ConsumedEvent(listenerId, groupId, r.key(), r.value(), r.partition(), r.offset()));
        var latch = latchByListener.get(listenerId);
        if (latch != null) latch.countDown();
    }

    public boolean await(String listenerId, long timeout, TimeUnit unit) throws InterruptedException {
        var latch = latchByListener.get(listenerId);
        if (latch == null) return true;
        return latch.await(timeout, unit);
    }

    public List<ConsumedEvent> events(String listenerId) {
        return eventsByListener.getOrDefault(listenerId, List.of());
    }
}
