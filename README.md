# kraft-kafka-partitions-demo (v1.1)

Real-world **Spring Boot (Java 21) + Apache Kafka (KRaft)** demo:

## Topology (as requested)

- **1 topic**: `demo.events`
- **3 partitions**: `0,1,2`
- **2 consumer groups**:
  - **group-a** has **2 consumers**:
    - consumer #1 reads **partition 0**
    - consumer #2 reads **partitions 1 and 2**
  - **group-b** has **1 consumer** that reads **all partitions** (because it’s alone in the group)

Partition assignment is **deterministic** because we bind listeners to partitions explicitly using
`@KafkaListener(topicPartitions = @TopicPartition(...))`.

## Production-grade reliability features (implemented)

### 1) Kafka delivery guarantees (what you *can* and *cannot* guarantee)

- **Producer** is configured for **strong delivery**:
  - `acks=all`
  - `enable.idempotence=true`
  - high retries
- **Consumer** is **at-least-once** (normal for Kafka).
- **Exactly-once side effects** are achieved via **idempotent processing** (Inbox + dedup in PostgreSQL).
  - This is the standard approach when you have DB side effects (because there is no Kafka↔DB 2PC).

### 2) Duplicates: idempotency + dedup store (PostgreSQL)

We use an **Inbox pattern** table (`kafka_event_inbox`) keyed by:
- `(event_id, consumer_group)` — important: each group is an independent stream.

Processing flow:
1. `claim(eventId, groupId)` **increments attempt** in a separate transaction (so retries do not reset attempts)
2. If already **PROCESSED**, we **skip** (duplicate delivery)
3. Otherwise process the business side effect and mark **PROCESSED**

### 3) Retries + poison messages

- `DefaultErrorHandler` with **exponential backoff**
- **Non-retryable** (poison) exceptions: `PoisonMessageException`, `IllegalArgumentException`
- After retries exhausted (or for poison):
  - store record + error into **`kafka_poison_message`**
  - mark inbox status **FAILED**
  - **commit offset** (so consumer continues)

### 4) Ordering

Kafka guarantees ordering **per partition**.
This demo preserves that by:
- explicit partition assignment (group-a)
- `concurrency=1` per listener container

Cross-partition ordering is **not guaranteed** (and should not be assumed in real designs).

## Run locally (KRaft + Postgres)

```bash
docker compose up -d
./gradlew bootRun
```

## Publish messages

### Send to a specific partition

```bash
curl -X POST http://localhost:8080/api/messages \
  -H "Content-Type: application/json" \
  -d '{"key":"k0","value":"hello","partition":0}'
```

### Provide your own eventId (recommended)

```bash
curl -X POST http://localhost:8080/api/messages \
  -H "Content-Type: application/json" \
  -d '{"key":"k0","value":"hello","partition":0,"eventId":"11111111-1111-1111-1111-111111111111"}'
```

## DB tables (Flyway)

- `kafka_event_inbox` (status + attempt + dedup per group)
- `kafka_poison_message` (final “dead letter” storage in DB)
- `business_event` (demo side effect; unique by eventId+group)

## Tests

Integration tests use **Testcontainers** (Kafka + Postgres) and validate:
- partition routing per group
- dedup prevents duplicate DB side effects
- poison messages are stored and offsets advance
- transient errors are retried and eventually succeed
