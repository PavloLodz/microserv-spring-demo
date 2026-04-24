-- V4__add_indexes.sql
-- Performance indexes for orders, outbox_event, and idempotency_key tables.
-- Kept separate from table DDL so indexes can be reviewed, added, or dropped independently
-- without touching table structure — a common operational pattern.

-- orders: filter by customer (list orders for a customer — Phase 3 CRUD)
CREATE INDEX idx_orders_customer_id ON orders (customer_id);

-- orders: filter by status (status-based filtering)
CREATE INDEX idx_orders_status ON orders (status);

-- orders: range scans and ordering by creation time (default sort order for pagination)
CREATE INDEX idx_orders_created_at ON orders (created_at);

-- outbox_event: worker polls for PENDING rows — this is the hot query path
-- Partial index covers only unpublished rows; once a row transitions to PROCESSED
-- it exits the index, keeping it small and fast as the table grows.
CREATE INDEX idx_outbox_event_status ON outbox_event (status) WHERE status = 'PENDING';

-- outbox_event: traceability — find all events for a given order
CREATE INDEX idx_outbox_event_aggregate_id ON outbox_event (aggregate_id);

-- idempotency_key: no additional index on key — the UNIQUE constraint (uq_idempotency_key_key)
-- automatically creates a unique B-tree index; a duplicate index would waste space.

-- idempotency_key: TTL cleanup job — find expired rows (WHERE expires_at < now())
CREATE INDEX idx_idempotency_key_expires_at ON idempotency_key (expires_at);
