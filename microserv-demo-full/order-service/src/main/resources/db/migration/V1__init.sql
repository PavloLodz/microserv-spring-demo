-- V1__init.sql
-- Creates the orders table.
-- `id` is a UUIDv7 generated in the application layer and used as the public/external primary key.
-- `debug_id` is a DB-owned sequential identity column for internal debugging only; never set by the application.

CREATE TABLE orders (
    id            UUID           NOT NULL,
    debug_id      BIGINT         GENERATED ALWAYS AS IDENTITY,
    customer_id   UUID           NOT NULL,
    status        VARCHAR(20)    NOT NULL,
    total_amount  NUMERIC(19, 2) NOT NULL,
    created_at    TIMESTAMPTZ    NOT NULL,
    updated_at    TIMESTAMPTZ    NOT NULL,
    deleted_at    TIMESTAMPTZ,
    version       BIGINT         NOT NULL DEFAULT 0,
    CONSTRAINT pk_orders PRIMARY KEY (id)
);
