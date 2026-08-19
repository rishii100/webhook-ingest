-- The original schema has a non-unique index on event_id which does not
-- prevent duplicate rows from concurrent at-least-once deliveries.
-- This adds a true UNIQUE constraint as the durable dedup guarantee.
DROP INDEX IF EXISTS idx_events_event_id;
ALTER TABLE events ADD CONSTRAINT events_event_id_unique UNIQUE (event_id);
