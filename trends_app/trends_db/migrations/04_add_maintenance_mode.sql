-- Migration: Add maintenance_mode metric and clean up legacy metrics
-- Date: 2025-11-13
-- Description: Adds maintenance_mode metric for system-wide login control
--              and removes legacy unused authentication metrics

-- Clean up legacy metrics that are not used in the current implementation
-- These were copied from the original pattern but are not referenced in the codebase
DELETE FROM metric WHERE metric_name IN ('AllowedUserLogins', 'ActiveOnly', 'AdminOnly', 'Any');

-- Add maintenance_mode metric (default: false - system is operational)
SELECT setmetric('maintenance_mode', 'f');

-- Set the module for maintenance_mode
UPDATE metric SET metric_module = 'security' WHERE metric_name = 'maintenance_mode';

-- Verify the result
SELECT metric_name, metric_value, metric_module
FROM metric
WHERE metric_module = 'security'
ORDER BY metric_name;
