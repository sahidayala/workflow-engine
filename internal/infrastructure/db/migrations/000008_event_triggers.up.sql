-- event_triggers maps an integration event type to a workflow that should
-- be triggered when an event of that type arrives for the project's tenant.
-- One event type can map to at most one workflow per project.
CREATE TABLE IF NOT EXISTS event_triggers (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id  UUID NOT NULL REFERENCES projects (id) ON DELETE CASCADE,
    event_type  TEXT NOT NULL,  -- e.g. "tenant.created", "identity.user.invited"
    workflow_id UUID NOT NULL REFERENCES workflows (id) ON DELETE CASCADE,
    is_active   BOOLEAN NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT event_triggers_project_event_type_key UNIQUE (project_id, event_type)
);

CREATE INDEX IF NOT EXISTS idx_event_triggers_project_id ON event_triggers (project_id);
CREATE INDEX IF NOT EXISTS idx_event_triggers_event_type  ON event_triggers (event_type) WHERE is_active = TRUE;
