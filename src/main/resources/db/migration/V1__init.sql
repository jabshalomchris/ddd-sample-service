-- Producer side: the aggregate and the outbox live in the same schema,
-- so both are written in one local transaction.

create table reservation (
    reservation_id uuid         primary key,
    guest_name     varchar(200) not null,
    party_size     int          not null check (party_size between 1 and 12),
    reserved_for   timestamptz  not null,
    status         varchar(20)  not null,
    revision       bigint       not null,
    version        bigint       not null
);

create table outbox_message (
    id             bigserial    primary key,   -- insertion order = publishing order
    event_id       uuid         not null unique,
    aggregate_type varchar(100) not null,
    aggregate_id   varchar(100) not null,      -- becomes the Kafka message key
    event_type     varchar(100) not null,
    topic          varchar(200) not null,
    payload        jsonb        not null,
    created_at     timestamptz  not null,
    published_at   timestamptz                 -- null = not yet sent to Kafka
);

create index outbox_message_unpublished_idx
    on outbox_message (id) where published_at is null;

-- Consumer side. In the real system these tables belong to the consuming
-- service's own schema; the sample keeps both sides in one service.

create table processed_event (
    consumer     varchar(100) not null,
    event_id     uuid         not null,
    processed_at timestamptz  not null default now(),
    primary key (consumer, event_id)
);

create table reservation_view (
    reservation_id uuid         primary key,
    guest_name     varchar(200),
    party_size     int,
    status         varchar(20)  not null,
    last_revision  bigint       not null
);
