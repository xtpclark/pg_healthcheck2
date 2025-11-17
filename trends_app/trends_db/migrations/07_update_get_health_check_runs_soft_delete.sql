-- Migration 07: Update get_health_check_runs() to support soft delete filtering
-- Date: 2025-11-14
-- Purpose: Add parameter to filter out soft-deleted runs by default

CREATE OR REPLACE FUNCTION get_health_check_runs(
    p_company_ids INT[],
    p_user_id INT,
    p_company_name TEXT DEFAULT NULL,
    p_target_host TEXT DEFAULT NULL,
    p_target_port INT DEFAULT NULL,
    p_target_db_name TEXT DEFAULT NULL,
    p_start_date DATE DEFAULT NULL,
    p_end_date DATE DEFAULT NULL,
    p_include_deleted BOOLEAN DEFAULT FALSE  -- NEW: Include soft-deleted runs (default: false)
)
RETURNS JSONB AS $$
DECLARE
    v_result JSONB;
BEGIN
    -- Build and execute query with dynamic filters
    SELECT COALESCE(jsonb_agg(
        jsonb_build_object(
            'id', id,
            'run_timestamp', run_timestamp,
            'company_name', company_name,
            'target_host', target_host,
            'target_port', target_port,
            'target_db_name', target_db_name,
            'db_technology', db_technology,
            'critical_count', critical_count,
            'high_count', high_count,
            'medium_count', medium_count,
            'is_favorite', is_favorite,
            'deleted_at', deleted_at,       -- NEW: Include deleted timestamp
            'deleted_by', deleted_by        -- NEW: Include who deleted it
        )
        ORDER BY run_timestamp DESC
    ), '[]'::jsonb)
    INTO v_result
    FROM (
        SELECT
            hcr.id,
            hcr.run_timestamp,
            c.company_name,
            hcr.target_host,
            hcr.target_port,
            hcr.target_db_name,
            hcr.db_technology,
            hcr.deleted_at,      -- NEW: Return deleted timestamp
            hcr.deleted_by,      -- NEW: Return who deleted it
            -- Calculate counts from triggered rules
            COALESCE(
                (SELECT COUNT(*) FROM health_check_triggered_rules
                 WHERE run_id = hcr.id AND severity_level = 'critical'), 0
            ) AS critical_count,
            COALESCE(
                (SELECT COUNT(*) FROM health_check_triggered_rules
                 WHERE run_id = hcr.id AND severity_level = 'high'), 0
            ) AS high_count,
            COALESCE(
                (SELECT COUNT(*) FROM health_check_triggered_rules
                 WHERE run_id = hcr.id AND severity_level = 'medium'), 0
            ) AS medium_count,
            CASE WHEN ufr.user_id IS NOT NULL THEN true ELSE false END AS is_favorite
        FROM health_check_runs hcr
        JOIN companies c ON hcr.company_id = c.id
        LEFT JOIN user_favorite_runs ufr
            ON hcr.id = ufr.run_id AND ufr.user_id = p_user_id
        WHERE hcr.company_id = ANY(p_company_ids)
          -- NEW: Soft delete filter (exclude deleted runs unless requested)
          AND (p_include_deleted OR hcr.deleted_at IS NULL)
          -- Optional target filters
          AND (p_company_name IS NULL OR c.company_name = p_company_name)
          AND (p_target_host IS NULL OR hcr.target_host = p_target_host)
          AND (p_target_port IS NULL OR hcr.target_port = p_target_port)
          AND (p_target_db_name IS NULL OR hcr.target_db_name = p_target_db_name)
          -- Optional date filters
          AND (p_start_date IS NULL OR hcr.run_timestamp >= p_start_date)
          AND (p_end_date IS NULL OR hcr.run_timestamp < (p_end_date + INTERVAL '1 day'))
    ) subquery;

    RETURN v_result;
END;
$$ LANGUAGE plpgsql STABLE;

-- Update function comment
COMMENT ON FUNCTION get_health_check_runs(INT[], INT, TEXT, TEXT, INT, TEXT, DATE, DATE, BOOLEAN) IS
'Abstraction layer for health check runs query with soft delete support. Returns JSONB array of runs with filtering and user favorite status. By default excludes soft-deleted runs unless p_include_deleted=true.';

-- Migration complete
SELECT 'Updated get_health_check_runs() with soft delete support' AS status;
