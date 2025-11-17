-- Migration: Update get_health_check_runs() function
-- Description: Adds company_name, db_technology, and count fields to return structure
-- Date: 2025-01-XX

BEGIN;

DROP FUNCTION IF EXISTS get_health_check_runs(INT[], INT, TEXT, TEXT, INT, TEXT, DATE, DATE);

CREATE OR REPLACE FUNCTION get_health_check_runs(
    p_company_ids INT[],
    p_user_id INT,
    p_company_name TEXT DEFAULT NULL,
    p_target_host TEXT DEFAULT NULL,
    p_target_port INT DEFAULT NULL,
    p_target_db_name TEXT DEFAULT NULL,
    p_start_date DATE DEFAULT NULL,
    p_end_date DATE DEFAULT NULL
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
            'is_favorite', is_favorite
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
COMMENT ON FUNCTION get_health_check_runs(INT[], INT, TEXT, TEXT, INT, TEXT, DATE, DATE) IS
'Abstraction layer for health check runs query. Returns JSONB array of runs with company_name, db_technology, issue counts, filtering, and user favorite status. Designed for future ClickHouse migration via FDW.';

COMMIT;
