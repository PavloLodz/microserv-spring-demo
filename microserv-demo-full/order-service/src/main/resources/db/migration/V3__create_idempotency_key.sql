-- V3__create_idempotency_key.sql
-- Creates the idempotency_key table to support safe retries of POST /orders requests.
-- Each client-supplied idempotency key is stored with the request hash and the serialised
-- HTTP response so that duplicate requests receive the identical response without re-executing
-- the business logic. Records expire after a TTL and are cleaned up by a scheduled job (Phase 10).

CREATE TABLE idempotency_key (
    id            UUID         NOT NULL,
    key           VARCHAR(255) NOT NULL,
    request_hash  VARCHAR(64)  NOT NULL,
    response_body JSONB,
    status        VARCHAR(50)  NOT NULL,
    created_at    TIMESTAMPTZ  NOT NULL,
    expires_at    TIMESTAMPTZ  NOT NULL,
    CONSTRAINT pk_idempotency_key PRIMARY KEY (id),
    CONSTRAINT uq_idempotency_key_key UNIQUE (key)
);
