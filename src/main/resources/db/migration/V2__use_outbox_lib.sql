-- Switch from the stand-in outbox to the supervisors' outbox library (outbox-lib-jpa 10.x).

drop table outbox_message;

-- Copied unchanged from outbox-lib: database/migration/B0001__v7.0.0_initial.sql
-- The library does not create this table itself; every producing service migrates it.
-- (The library states PostgreSQL 17; it is tested here on PostgreSQL 18.)

CREATE TABLE IF NOT EXISTS event_outbox (
    id                  UUID            NOT NULL,
    sequence_id         BIGSERIAL       NOT NULL,
    topic_name          VARCHAR         NOT NULL,
    partition           SMALLINT        NOT NULL,
    created_at          TIMESTAMPTZ     NOT NULL        DEFAULT NOW(),
    due_delay_ms        INTEGER,
    due_by              TIMESTAMPTZ,
    trace_id            VARCHAR,
    event_key           VARCHAR,
    event_type          VARCHAR         NOT NULL,
    event_type_version  SMALLINT        NOT NULL,
    entity_name         VARCHAR         NOT NULL,
    entity_id           VARCHAR         NOT NULL,
    custom_headers      JSONB,
    payload             BYTEA           NOT NULL,
    is_read             BOOLEAN         NOT NULL        DEFAULT FALSE,
    version             SMALLINT        NOT NULL,
    PRIMARY KEY (id)
    );

CREATE UNIQUE INDEX IF NOT EXISTS uidx_event_outbox__sequence_id ON event_outbox (sequence_id);

CREATE INDEX IF NOT EXISTS idx_event_outbox__topic_partitions_sequence ON event_outbox(topic_name, partition);

CREATE OR REPLACE FUNCTION calc_event_outbox_due_by() RETURNS trigger AS $$
BEGIN
    IF NEW.due_delay_ms IS NOT NULL THEN
        NEW.due_by := NOW() + (NEW.due_delay_ms * interval '1 millisecond');
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trg_outbox_event_set_due_by
BEFORE INSERT ON event_outbox
FOR EACH ROW EXECUTE FUNCTION calc_event_outbox_due_by();

-- Consumer side: also remember key, type and sequence id of each processed event (shown on the demo page).
alter table processed_event add column event_key   varchar(100);
alter table processed_event add column event_type  varchar(100);
alter table processed_event add column sequence_id bigint;
