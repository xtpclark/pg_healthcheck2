#!/bin/bash

# This script creates various PostgreSQL objects and generates some load
# to create diverse test cases for the pg_healthcheck report.
# It is designed to be idempotent, dropping and recreating the test database.

# --- Configuration ---
CONFIG_FILE="./config/config.yaml" # Path to your config.yaml

# Read database connection details from config.yaml
# Using grep and awk for basic YAML parsing without external dependencies like yq
DB_HOST=$(grep -E '^\s*host:' "$CONFIG_FILE" | awk '{print $2}' | tr -d '\r')
DB_PORT=$(grep -E '^\s*port:' "$CONFIG_FILE" | awk '{print $2}' | tr -d '\r')
# Use a dedicated test database name
DB_USER=$(grep -E '^\s*user:' "$CONFIG_FILE" | awk '{print $2}' | tr -d '\r')
DB_PASSWORD=$(grep -E '^\s*password:' "$CONFIG_FILE" | awk '{print $2}' | tr -d '\r')

# Define the dedicated test database name
TEST_DB_NAME="healthcheck_demo"

# Export password for psql/pgbench
export PGPASSWORD="$DB_PASSWORD"

echo "--- Starting PostgreSQL Test Case Generation ---"

# --- Helper function for executing SQL ---
execute_sql() {
    local sql_query=$1
    local target_db=${2:-$TEST_DB_NAME} # Default to TEST_DB_NAME if not provided
    echo "Executing SQL on $target_db: $sql_query"
    psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$target_db" -c "$sql_query"
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

# --- Idempotency: Drop and Create Test Database ---
echo "--- Dropping existing database '$TEST_DB_NAME' if it exists ---"
execute_sql "DROP DATABASE IF EXISTS $TEST_DB_NAME WITH (FORCE);" "postgres" # Use FORCE for active connections

echo "--- Creating database '$TEST_DB_NAME' ---"
execute_sql "CREATE DATABASE $TEST_DB_NAME;" "postgres"
echo "Connected to: $DB_USER@$DB_HOST:$DB_PORT/$TEST_DB_NAME"

echo "--- Creating pg_stat_statements '$TEST_DB_NAME' ---"
execute_sql "CREATE EXTENSION pg_stat_statements;"

# --- 1. pgbench setup and load generation ---
echo "--- Setting up pgbench and generating load ---"

# Initialize pgbench tables in the test database with a larger scale factor
# -s 100 will create approx. 1.5GB of data for default tables.
# This ensures a dataset of at least 100MB easily.
execute_pgbench "pgbench -i -s 100 -h $DB_HOST -p $DB_PORT -U $DB_USER -d $TEST_DB_NAME"

# Run pgbench to generate activity (inserts, updates, etc.)
# -c 20: 20 clients
# -j 4: 4 threads
# -t 10000: 10000 transactions per client (increased for more activity)
echo "Running pgbench for load generation (this might take a moment)..."
execute_pgbench "pgbench -c 20 -j 4 -t 1000 -h $DB_HOST -p $DB_PORT -U $DB_USER -d $TEST_DB_NAME"
echo "pgbench load generation complete."

# --- 2. Create a Partitioned Table ---
echo "--- Creating a Partitioned Table ---"
execute_sql "
CREATE TABLE public.sensor_data (
    id BIGSERIAL,
    reading_time TIMESTAMP NOT NULL,
    sensor_id INT NOT NULL,
    temperature NUMERIC,
    humidity NUMERIC
) PARTITION BY RANGE (reading_time);
"

# Create more partitions for better coverage
execute_sql "
CREATE TABLE public.sensor_data_2022_q4 PARTITION OF public.sensor_data
FOR VALUES FROM ('2022-10-01') TO ('2023-01-01');
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
execute_sql "
CREATE TABLE public.sensor_data_2024_q1 PARTITION OF public.sensor_data
FOR VALUES FROM ('2024-01-01') TO ('2024-04-01');
"

echo "Inserting data into partitioned table (approx 1 year of hourly data, ~8760 rows per partition)..."
execute_sql "
INSERT INTO public.sensor_data (reading_time, sensor_id, temperature, humidity)
SELECT
    GENERATE_SERIES('2022-10-01 00:00:00'::timestamp, '2024-03-31 23:59:59'::timestamp, '1 hour'::interval),
    (RANDOM() * 100)::int, -- More sensor IDs
    (RANDOM() * 50 + 10)::numeric(5,2),
    (RANDOM() * 100)::numeric(5,2);
"
echo "Partitioned table created and populated."

# --- 3. Create a Materialized View ---
echo "--- Creating a Materialized View ---"
execute_sql "
CREATE TABLE public.sales_data (
    sale_id BIGSERIAL PRIMARY KEY,
    product_id INT,
    sale_date DATE,
    amount NUMERIC(10, 2),
    customer_id INT
);
"
# Insert more data to make it larger
echo "Inserting data into sales_data (approx 100,000 rows)..."
execute_sql "
INSERT INTO public.sales_data (product_id, sale_date, amount, customer_id)
SELECT
    (RANDOM() * 1000)::int,
    '2022-01-01'::date + (n * '1 day'::interval),
    (RANDOM() * 1000)::numeric(10,2),
    (RANDOM() * 5000)::int
FROM GENERATE_SERIES(1, 100000) AS n;
"

execute_sql "
CREATE MATERIALIZED VIEW public.daily_sales_summary AS
SELECT
    sale_date,
    COUNT(sale_id) AS total_sales,
    SUM(amount) AS total_amount,
    COUNT(DISTINCT customer_id) as distinct_customers
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
    product_id BIGSERIAL PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL,
    price NUMERIC(10, 2)
);
"
execute_sql "INSERT INTO public.parent_products (product_name, price) VALUES ('Laptop', 1200.00), ('Mouse', 25.00), ('Keyboard', 75.00), ('Monitor', 300.00);"

# Child table with an INDEXED Foreign Key (should NOT be flagged by audit)
execute_sql "
CREATE TABLE public.orders_indexed_fk (
    order_id BIGSERIAL PRIMARY KEY,
    product_id BIGINT NOT NULL,
    quantity INT NOT NULL,
    order_date DATE DEFAULT CURRENT_DATE,
    CONSTRAINT fk_product_indexed FOREIGN KEY (product_id) REFERENCES public.parent_products(product_id)
);
"
execute_sql "CREATE INDEX idx_orders_indexed_fk_product_id ON public.orders_indexed_fk (product_id);"
execute_sql "INSERT INTO public.orders_indexed_fk (product_id, quantity) VALUES (1, 2), (2, 1), (3, 5), (4, 1);"

# Child table with an UNINDEXED Foreign Key (should BE flagged by audit)
execute_sql "
CREATE TABLE public.orders_unindexed_fk (
    order_id BIGSERIAL PRIMARY KEY,
    product_id BIGINT NOT NULL,
    quantity INT NOT NULL,
    order_date DATE DEFAULT CURRENT_DATE,
    CONSTRAINT fk_product_unindexed FOREIGN KEY (product_id) REFERENCES public.parent_products(product_id)
);
"
# DO NOT create an index on product_id for this table
execute_sql "INSERT INTO public.orders_unindexed_fk (product_id, quantity) VALUES (1, 3), (2, 2), (3, 1), (4, 4);"

echo "Foreign Key test cases created."

# --- 6. Create Duplicate Indexes ---
echo "--- Creating Duplicate Indexes ---"

execute_sql "
CREATE TABLE public.users_dup_idx (
    user_id BIGSERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE,
    email VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);
"

# Primary index is automatically created on user_id
execute_sql "CREATE UNIQUE INDEX idx_users_dup_username ON public.users_dup_idx (username);"
execute_sql "CREATE UNIQUE INDEX idx_users_dup_username_redundant ON public.users_dup_idx (username);" # Duplicate
execute_sql "CREATE INDEX idx_users_dup_email ON public.users_dup_idx (email);"
execute_sql "CREATE INDEX idx_users_dup_email_redundant ON public.users_dup_idx (email);" # Duplicate

echo "Inserting data into users_dup_idx..."
execute_sql "
INSERT INTO public.users_dup_idx (username, email)
SELECT
    'user_' || n,
    'user' || n || '@example.com'
FROM GENERATE_SERIES(1, 10000) AS n;
"
echo "Duplicate indexes created."

# --- 7. Create GIN Indexes ---
echo "--- Creating GIN Indexes ---"

# Enable pg_trgm for text search example
execute_sql "CREATE EXTENSION IF NOT EXISTS pg_trgm;"

execute_sql "
CREATE TABLE public.documents_gin (
    doc_id BIGSERIAL PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    content TEXT
);
"

echo "Inserting data into documents_gin..."
execute_sql "
INSERT INTO public.documents_gin (title, content)
SELECT
    'Document Title ' || n,
    'This is the content for document ' || n || '. It contains various keywords like PostgreSQL, database, performance, security, and optimization. Some documents might mention analytics or data warehousing.'
FROM GENERATE_SERIES(1, 5000) AS n;
"

execute_sql "CREATE INDEX idx_documents_gin_content ON public.documents_gin USING GIN (to_tsvector('english', content));"
execute_sql "CREATE INDEX idx_documents_gin_title_trgm ON public.documents_gin USING GIN (title gin_trgm_ops);" # For trigram search

echo "GIN indexes created and populated."

# --- 8. Create BRIN Indexes ---
echo "--- Creating BRIN Indexes ---"

execute_sql "
CREATE TABLE public.logs_brin (
    log_id BIGSERIAL PRIMARY KEY,
    log_time TIMESTAMP NOT NULL,
    message TEXT,
    severity VARCHAR(20)
);
"

echo "Inserting data into logs_brin (approx 1 million rows, ordered by time)..."
execute_sql "
INSERT INTO public.logs_brin (log_time, message, severity)
SELECT
    GENERATE_SERIES('2023-01-01 00:00:00'::timestamp, '2024-01-01 00:00:00'::timestamp, '1 minute'::interval),
    'Log message for event ' || n,
    CASE (n % 3)
        WHEN 0 THEN 'INFO'
        WHEN 1 THEN 'WARNING'
        ELSE 'ERROR'
    END
FROM GENERATE_SERIES(1, 10) AS n; -- 365 days * 24 hours * 60 minutes
-- FROM GENERATE_SERIES(1, 525600) AS n; -- 365 days * 24 hours * 60 minutes
"

execute_sql "CREATE INDEX idx_logs_brin_log_time ON public.logs_brin USING BRIN (log_time);"

echo "BRIN index created and populated."

# --- 9. Create a Table with high insert activity (for n_tup_ins check)
echo "--- Creating a table with high insert activity ---"
execute_sql "
CREATE TABLE public.event_stream (
    event_id BIGSERIAL PRIMARY KEY,
    event_time TIMESTAMP DEFAULT NOW(),
    event_type VARCHAR(50),
    payload JSONB
);
"
echo "Inserting a large number of rows into event_stream (approx 200,000 rows)..."
execute_sql "
INSERT INTO public.event_stream (event_type, payload)
SELECT
    CASE (n % 5)
        WHEN 0 THEN 'LOGIN'
        WHEN 1 THEN 'LOGOUT'
        WHEN 2 THEN 'CLICK'
        WHEN 3 THEN 'PURCHASE'
        ELSE 'VIEW'
    END,
    ('{\"user_id\": ' || (RANDOM() * 10000)::int || ', \"data\": \"some_data_' || n || '\"}')::jsonb
FROM GENERATE_SERIES(1, 200000) AS n;
"
echo "High insert activity table populated."

# --- 10. Function Audit Test Cases ---
echo "--- Creating Function Audit Test Cases ---"

# 10.1. SECURITY DEFINER function
execute_sql "
CREATE FUNCTION public.get_sensitive_data_secdef(user_id_param INT)
RETURNS TEXT
LANGUAGE plpgsql
SECURITY DEFINER
AS \$\$
BEGIN
    -- In a real scenario, this would query a sensitive table
    RETURN 'Sensitive data for user ' || user_id_param || ' (accessed by SECURITY DEFINER)';
END;
\$\$;
"
echo "Created SECURITY DEFINER function: public.get_sensitive_data_secdef"

# 10.2. Function owned by a superuser (admin_user created earlier)
execute_sql "
CREATE FUNCTION public.perform_admin_task()
RETURNS TEXT
LANGUAGE plpgsql
AS \$\$
BEGIN
    RETURN 'Admin task performed by function owned by superuser.';
END;
\$\$;
"
execute_sql "ALTER FUNCTION public.perform_admin_task() OWNER TO admin_user;"
echo "Created function public.perform_admin_task and set owner to admin_user."

# 10.3. Functions for pg_stat_statements performance tracking
execute_sql "
CREATE FUNCTION public.calculate_sum(a INT, b INT)
RETURNS INT
LANGUAGE plpgsql
AS \$\$
BEGIN
    RETURN a + b;
END;
\$\$;
"
execute_sql "
CREATE FUNCTION public.get_random_text()
RETURNS TEXT
LANGUAGE plpgsql
AS \$\$
BEGIN
    RETURN 'Random text: ' || md5(random()::text);
END;
\$\$;
"
echo "Calling functions to generate pg_stat_statements data..."
for i in $(seq 1 1000); do
    execute_sql "SELECT public.calculate_sum(10, 20);"
done
for i in $(seq 1 500); do
    execute_sql "SELECT public.get_random_text();"
done
echo "Functions called to generate pg_stat_statements data."

# 10.4. Function using dynamic SQL (for manual audit note)
execute_sql "
CREATE FUNCTION public.dynamic_query_executor(table_name TEXT, column_name TEXT)
RETURNS SETOF TEXT
LANGUAGE plpgsql
AS \$\$
DECLARE
    result_row TEXT;
BEGIN
    RETURN QUERY EXECUTE format('SELECT %I FROM %I LIMIT 10', column_name, table_name);
END;
\$\$;
"
echo "Created function public.dynamic_query_executor (uses dynamic SQL)."

echo "--- Test Case Generation Complete ---"
echo "You can now run your pg_healthcheck.py script to analyze the database: $TEST_DB_NAME"

# --- Final Cleanup Commands (for manual use if needed) ---
echo ""
echo "--- Manual Cleanup Commands (Run these if you want to remove test objects manually) ---"
echo "psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d postgres -c \"DROP DATABASE IF EXISTS $TEST_DB_NAME WITH (FORCE);\""
