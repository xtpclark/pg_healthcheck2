-- SQL Script to Create Test Tables with Nearly Exhausted Sequences
-- This script is for testing the primary_key_exhaustion health check.

-- Drop tables if they exist to make the script re-runnable
DROP TABLE IF EXISTS high_usage_integer_pk;
DROP TABLE IF EXISTS high_usage_smallint_pk;

------------------------------------------------------------------
-- Example 1: Standard Integer (4-byte) Primary Key Exhaustion
------------------------------------------------------------------

-- Create a table with a standard SERIAL primary key, which creates an INTEGER column.
CREATE TABLE high_usage_integer_pk (
    id SERIAL PRIMARY KEY,
    data TEXT
);

-- The maximum value for a standard integer is 2,147,483,647.
-- We will set the sequence to 98% of its maximum value to trigger the health check.
-- 2,147,483,647 * 0.98 = 2,104,533,974

-- Use setval() to advance the sequence for the 'id' column to a high value.
-- The sequence name is automatically generated as <table_name>_<column_name>_seq.
SELECT setval('high_usage_integer_pk_id_seq', 2104533974);

-- Insert a dummy row to confirm the next value will be high
INSERT INTO high_usage_integer_pk (data) VALUES ('test data');

-- Verify the result (the id should be 2104533975)
SELECT * FROM high_usage_integer_pk;


------------------------------------------------------------------
-- Example 2: Smallint (2-byte) Primary Key Exhaustion
------------------------------------------------------------------

-- Create a table with a SMALLSERIAL primary key, which creates a SMALLINT column.
CREATE TABLE high_usage_smallint_pk (
    id SMALLSERIAL PRIMARY KEY,
    data TEXT
);

-- The maximum value for a smallint is 32,767.
-- We will set the sequence to 95% of its maximum value.
-- 32,767 * 0.95 = 31,128

SELECT setval('high_usage_smallint_pk_id_seq', 31128);

-- Insert a dummy row
INSERT INTO high_usage_smallint_pk (data) VALUES ('test data');

-- Verify the result (the id should be 31129)
SELECT * FROM high_usage_smallint_pk;
