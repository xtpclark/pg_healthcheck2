-- SQL Script to Intentionally Cause a Temporary File Spill
-- This is for testing the temp_files_analysis health check.

-- Set a very low work_mem for this session to easily force a spill to disk.
-- The default is typically 4MB or higher. We set it to the minimum (64kB).
SET work_mem = '64kB';

-- Announce the current work_mem setting for clarity.
SHOW work_mem;

-- Use EXPLAIN ANALYZE to execute the query and see the query plan details,
-- which will include information about temporary file usage.
-- This query generates a large number of random text strings and sorts them.
-- The sort operation will exceed the 64kB work_mem limit, forcing a spill.
EXPLAIN ANALYZE
SELECT md5(random()::text) AS random_text
FROM generate_series(1, 200000) s(i) -- Generate 200,000 rows
ORDER BY random_text;

-- The output of the above query will contain a line similar to:
-- "Sort Method: external merge Disk: 2488kB"
-- This confirms that temporary files were written to disk.

-- Reset work_mem to its default value for the session.
RESET work_mem;

-- Verify that work_mem has been reset.
SHOW work_mem;
