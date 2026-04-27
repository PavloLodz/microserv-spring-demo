-- V5__add_outbox_retry_count.sql
-- Adds a retry counter to outbox_event for the outbox background poller.
--
-- Purpose: the poller increments this counter on each failed Kafka publish attempt.
-- Once the counter reaches the configured threshold (outbox.max-retry, default 5),
-- the row transitions from PENDING to FAILED, quarantining poison-pill events so
-- they no longer block the poll cycle.
--
-- DEFAULT 0 ensures all existing rows start with zero retries — no data migration needed.

ALTER TABLE outbox_event
    ADD COLUMN retry_count INTEGER NOT NULL DEFAULT 0;
