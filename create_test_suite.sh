#!/bin/bash

# This script creates various PostgreSQL objects and generates some load
# to create diverse test cases for the pg_healthcheck report.

# --- Configuration ---
CONFIG_FILE="./config/config.yaml" # Path to your config.yaml

# Read database connection details from config.yaml
# Using grep and awk for basic YAML parsing without external dependencies like yq
DB_HOST=$(grep -E '^\s*host:' "$CONFIG_FILE" | awk '{print $2}' | tr -d '\r')
DB_PORT=$(grep -E '^\s*port:' "$CONFIG_FILE" | awk '{print $2}' | tr -d '\r')
DB_NAME=$(grep -E '^\s*database:' "$CONFIG_FILE" | awk '{print $2}' | tr -d '\r')
DB_USER=$(grep -E '^\s*user:' "$CONFIG_FILE" | awk '{print $2}' | tr -d '\r')
DB_PASSWORD=$(grep -E '^\s*password:' "$CONFIG_FILE" | awk '{print $2}' | tr -d '\r')

# Export password for psql/pgbench
export PGPASSWORD="$DB_PASSWORD"

echo "--- Starting PostgreSQL Test Case Generation ---"
echo "Connecting to: $DB_USER@$DB_HOST:$DB_PORT/$DB_NAME"

# --- Helper function for executing SQL ---
execute_sql() {
    local sql_query=$1
    echo "Executing SQL: $sql_query"
    psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "$sql_query"
    if [ $? -ne 0 ]; then
        echo "ERROR: SQL execution failed for query: $sql_query"
        exit 1
    fi
}

# --- Helper function for executing pgbench ---
execute_pgbench() {
    local pgbench_cmd=$1
    echo "Executing pgbench: $pgbench_cmd"
    $pgbench_cmd
    if [ $? -ne 0 ]; then
        echo "ERROR: pgbench command failed: $pgbench_cmd"
        exit 1
    fi
}

# --- 1. pgbench setup and load generation ---
echo "--- Setting up pgbench and generating load ---"
PGBENCH_TEST_DB="pgbench_test_db"

# Create a test database for pgbench if it doesn't exist
execute_sql "CREATE DATABASE $PGBENCH_TEST_DB;"

# Initialize pgbench tables in the test database
execute_pgbench "pgbench -i -s 1 -h $DB_HOST -p $DB_PORT -U $DB_USER -d $PGBENCH_TEST_DB"

# Run pgbench to generate activity (inserts, updates, etc.)
# -c 10: 10 clients
# -j 2: 2 threads
# -t 1000: 1000 transactions per client
# This will create significant n_tup_ins and other activity
echo "Running pgbench for load generation (this might take a moment)..."
execute_pgbench "pgbench -c 20 -j 4 -t 1000 -h $DB_HOST -p $DB_PORT -U $DB_USER -d $PGBENCH_TEST_DB"
echo "pgbench load generation complete."

# --- 2. Create a Partitioned Table ---
echo "--- Creating a Partitioned Table ---"
execute_sql "
CREATE TABLE public.sensor_data (
    id SERIAL,
    reading_time TIMESTAMP NOT NULL,
    sensor_id INT NOT NULL,
    temperature NUMERIC,
    humidity NUMERIC
) PARTITION BY RANGE (reading_time);
"

execute_sql "
CREATE TABLE public.sensor_data_2023_q1 PARTITION OF public.sensor_data
FOR VALUES FROM ('2023-01-01') TO ('2023-04-01');
"
execute_sql "
CREATE TABLE public.sensor_data_2023_q2 PARTITION OF public.sensor_data
FOR VALUES FROM ('2023-04-01') TO ('2023-07-01');
"
execute_sql "
CREATE TABLE public.sensor_data_2023_q3 PARTITION OF public.sensor_data
FOR VALUES FROM ('2023-07-01') TO ('2023-10-01');
"
execute_sql "
CREATE TABLE public.sensor_data_2023_q4 PARTITION OF public.sensor_data
FOR VALUES FROM ('2023-10-01') TO ('2024-01-01');
"

echo "Inserting data into partitioned table..."
execute_sql "
INSERT INTO public.sensor_data (reading_time, sensor_id, temperature, humidity)
SELECT
    GENERATE_SERIES('2023-01-01 00:00:00'::timestamp, '2023-12-31 23:59:59'::timestamp, '1 hour'::interval),
    (RANDOM() * 10)::int,
    (RANDOM() * 50 + 10)::numeric(5,2),
    (RANDOM() * 100)::numeric(5,2);
"
echo "Partitioned table created and populated."

# --- 3. Create a Materialized View ---
echo "--- Creating a Materialized View ---"
execute_sql "
CREATE TABLE public.sales_data (
    sale_id SERIAL PRIMARY KEY,
    product_id INT,
    sale_date DATE,
    amount NUMERIC
);
"
execute_sql "
INSERT INTO public.sales_data (product_id, sale_date, amount)
SELECT
    (RANDOM() * 100)::int,
    '2023-01-01'::date + (n * '1 day'::interval),
    (RANDOM() * 1000)::numeric(10,2)
FROM GENERATE_SERIES(1, 365) AS n;
"

execute_sql "
CREATE MATERIALIZED VIEW public.daily_sales_summary AS
SELECT
    sale_date,
    COUNT(sale_id) AS total_sales,
    SUM(amount) AS total_amount
FROM
    public.sales_data
GROUP BY
    sale_date
ORDER BY
    sale_date;
"
echo "Materialized view created. Refreshing..."
execute_sql "REFRESH MATERIALIZED VIEW public.daily_sales_summary;"
echo "Materialized view refreshed."

# --- 4. Create Test Users ---
echo "--- Creating Test Users ---"

# Insecure user (simple password)
execute_sql "
DO \$\$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'insecure_user') THEN
        CREATE USER insecure_user WITH PASSWORD 'password123';
    END IF;
END
\$\$;
"
echo "User 'insecure_user' created."

# Admin user (with elevated privileges)
execute_sql "
DO \$\$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'admin_user') THEN
        CREATE USER admin_user WITH PASSWORD 'StrongPassword!23' CREATEDB CREATEROLE;
    END IF;
END
\$\$;
"
echo "User 'admin_user' created."

# Grant CREATE on public schema to a non-admin user (security audit flag)
execute_sql "GRANT CREATE ON SCHEMA public TO insecure_user;"
echo "Granted CREATE on public schema to 'insecure_user'."

# --- 5. Create Foreign Key Audit Test Cases ---
echo "--- Creating Foreign Key Audit Test Cases ---"

# Parent table for FK tests
execute_sql "
CREATE TABLE public.parent_products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL,
    price NUMERIC(10, 2)
);
"
execute_sql "INSERT INTO public.parent_products (product_name, price) VALUES ('Laptop', 1200.00), ('Mouse', 25.00);"

# Child table with an INDEXED Foreign Key (should NOT be flagged by audit)
execute_sql "
CREATE TABLE public.orders_indexed_fk (
    order_id SERIAL PRIMARY KEY,
    product_id INT NOT NULL,
    quantity INT NOT NULL,
    CONSTRAINT fk_product_indexed FOREIGN KEY (product_id) REFERENCES public.parent_products(product_id)
);
"
execute_sql "CREATE INDEX idx_orders_indexed_fk_product_id ON public.orders_indexed_fk (product_id);"
execute_sql "INSERT INTO public.orders_indexed_fk (product_id, quantity) VALUES (1, 2), (2, 1);"

# Child table with an UNINDEXED Foreign Key (should BE flagged by audit)
execute_sql "
CREATE TABLE public.orders_unindexed_fk (
    order_id SERIAL PRIMARY KEY,
    product_id INT NOT NULL,
    quantity INT NOT NULL,
    CONSTRAINT fk_product_unindexed FOREIGN KEY (product_id) REFERENCES public.parent_products(product_id)
);
"
# DO NOT create an index on product_id for this table
execute_sql "INSERT INTO public.orders_unindexed_fk (product_id, quantity) VALUES (1, 3), (2, 2);"

echo "Foreign Key test cases created."


echo "--- Test Case Generation Complete ---"
echo "You can now run your pg_healthcheck.py script to analyze the database."

# --- Optional: Cleanup Commands ---
echo ""
echo "--- Cleanup Commands (Run these manually if you want to remove test objects) ---"
echo "psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c \"DROP TABLE public.sensor_data CASCADE;\""
echo "psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c \"DROP TABLE public.sales_data CASCADE;\""
echo "psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c \"DROP MATERIALIZED VIEW public.daily_sales_summary;\""
echo "psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c \"DROP TABLE public.orders_indexed_fk CASCADE;\""
echo "psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c \"DROP TABLE public.orders_unindexed_fk CASCADE;\""
echo "psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c \"DROP TABLE public.parent_products CASCADE;\""
echo "psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c \"DROP USER IF EXISTS insecure_user;\""
echo "psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c \"DROP USER IF EXISTS admin_user;\""
echo "psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d postgres -c \"DROP DATABASE IF EXISTS $PGBENCH_TEST_DB;\""
