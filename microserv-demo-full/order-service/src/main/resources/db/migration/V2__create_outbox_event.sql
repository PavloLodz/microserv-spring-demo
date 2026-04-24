-- V2__create_outbox_event.sql
-- Creates the outbox_event table for the transactional outbox pattern.
-- Events are written in the same transaction as the business operation and
-- published to Kafka asynchronously by a background worker (Phase 6).
-- This guarantees at-least-once delivery without distributed transactions.

CREATE TABLE outbox_event (
    id             UUID         NOT NULL,
    aggregate_type VARCHAR(50)  NOT NULL,
    aggregate_id   UUID         NOT NULL,
    event_type     VARCHAR(50)  NOT NULL,
    payload        JSONB        NOT NULL,
    status         VARCHAR(20)  NOT NULL,
    created_at     TIMESTAMPTZ  NOT NULL,
    processed_at   TIMESTAMPTZ,
    CONSTRAINT pk_outbox_event PRIMARY KEY (id)
);
