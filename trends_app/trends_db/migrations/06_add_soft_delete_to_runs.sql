-- Migration 06: Add Soft Delete Support to Health Check Runs
-- Date: 2025-11-14
-- Purpose: Add soft delete functionality to preserve historical data and enable recovery

-- Add soft delete columns to health_check_runs table
ALTER TABLE health_check_runs
ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS deleted_by VARCHAR(50);

-- Add index for performance (partial index on non-deleted runs)
CREATE INDEX IF NOT EXISTS idx_health_check_runs_active
ON health_check_runs(deleted_at)
WHERE deleted_at IS NULL;

-- Add index for deleted runs view
CREATE INDEX IF NOT EXISTS idx_health_check_runs_deleted
ON health_check_runs(deleted_at)
WHERE deleted_at IS NOT NULL;

-- Add comment for documentation
COMMENT ON COLUMN health_check_runs.deleted_at IS 'Timestamp when run was soft-deleted (NULL = active)';
COMMENT ON COLUMN health_check_runs.deleted_by IS 'Username who soft-deleted the run';

-- Create view for active (non-deleted) runs
CREATE OR REPLACE VIEW active_health_check_runs AS
SELECT * FROM health_check_runs
WHERE deleted_at IS NULL;

-- Create view for deleted runs (for recovery)
CREATE OR REPLACE VIEW deleted_health_check_runs AS
SELECT * FROM health_check_runs
WHERE deleted_at IS NOT NULL;

-- Grant permissions on views
GRANT SELECT ON active_health_check_runs TO PUBLIC;
GRANT SELECT ON deleted_health_check_runs TO PUBLIC;

-- Migration complete
SELECT 'Soft delete migration completed successfully' AS status;
