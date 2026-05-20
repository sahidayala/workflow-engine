-- external_tenant_id maps this project to a tenant in the SaaS platform.
-- When the Kafka consumer receives an integration event carrying a tenantId,
-- it looks up the project by this column to find the target workflow project.
ALTER TABLE projects ADD COLUMN IF NOT EXISTS external_tenant_id TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS idx_projects_external_tenant_id
    ON projects (external_tenant_id)
    WHERE external_tenant_id IS NOT NULL;
