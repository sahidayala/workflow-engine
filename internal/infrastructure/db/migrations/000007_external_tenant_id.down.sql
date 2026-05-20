DROP INDEX IF EXISTS idx_projects_external_tenant_id;
ALTER TABLE projects DROP COLUMN IF EXISTS external_tenant_id;
