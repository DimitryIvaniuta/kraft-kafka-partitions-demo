-- Inbox / idempotency store (at-least-once consumption + exactly-once side effects via dedup)
-- IMPORTANT: idempotency must be per consumer-group (each group is an independent stream).
create table if not exists kafka_event_inbox (
    event_id uuid not null,
    consumer_group text not null,
    topic text not null,
    partition int not null,
    offset bigint not null,
    status varchar(16) not null,
    attempt int not null,
    last_error text null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (event_id, consumer_group)
);

create index if not exists idx_inbox_group_status on kafka_event_inbox (consumer_group, status);

-- Where we persist poison messages after retries are exhausted (or for non-retryable exceptions)
create table if not exists kafka_poison_message (
    id bigserial primary key,
    event_id uuid null,
    topic text not null,
    partition int not null,
    offset bigint not null,
    consumer_group text not null,
    record_key text null,
    record_value text null,
    error_class text not null,
    error_message text null,
    stacktrace text null,
    created_at timestamptz not null default now()
);

-- Demo "business side effect" to prove idempotency + ordering per partition
create table if not exists business_event (
    id bigserial primary key,
    event_id uuid not null,
    consumer_group text not null,
    record_key text null,
    record_value text null,
    received_partition int not null,
    received_offset bigint not null,
    created_at timestamptz not null default now(),
    constraint uq_business_event unique (event_id, consumer_group)
);

create index if not exists idx_business_group_partition_offset on business_event (consumer_group, received_partition, received_offset);
