-- processed_integration_events tracks which integration events have already
-- triggered a workflow run, providing idempotency for the Kafka consumer.
--
-- Guarantee: at-least-once Kafka delivery means the same event may arrive
-- multiple times. Without this table, each delivery would create a duplicate
-- workflow run. This table is the deduplication guard.
--
-- The composite primary key (source_event_id, project_id) means the same event
-- can trigger one workflow run per project without cross-project interference.
-- When a new trigger fires it inserts a row; if the row already exists the INSERT
-- is rejected by the PK constraint and the handler returns nil (idempotent skip).
CREATE TABLE IF NOT EXISTS processed_integration_events (
    -- source_event_id is the 'id' field from the IntegrationEvent — the UUID
    -- assigned by Event Streaming's store on ingest (globally unique).
    source_event_id TEXT        NOT NULL,
    -- project_id scopes the dedup record to a project so that the same upstream
    -- event can be processed by different projects' triggers independently.
    project_id      UUID        NOT NULL REFERENCES projects (id) ON DELETE CASCADE,
    -- workflow_run_id is the run that was created for this event+project pair.
    -- Stored for operational debugging ("which run came from this event?").
    workflow_run_id UUID        NOT NULL,
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source_event_id, project_id)
);

-- Index supports queries like "what runs did this event trigger across projects?"
CREATE INDEX IF NOT EXISTS idx_processed_events_source
    ON processed_integration_events (source_event_id);

-- Cleanup index: processed events older than N days can be purged by a maintenance
-- job without affecting current behaviour (old events won't be redelivered by Kafka
-- after its retention window has passed anyway).
CREATE INDEX IF NOT EXISTS idx_processed_events_processed_at
    ON processed_integration_events (processed_at DESC);
