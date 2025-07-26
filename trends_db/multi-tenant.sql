-- Create the 'companies' table to support multi-tenancy
CREATE TABLE IF NOT EXISTS companies (
    id SERIAL PRIMARY KEY,
    company_name TEXT NOT NULL UNIQUE
);


DROP TABLE users;

CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    company_id INT NOT NULL REFERENCES companies(id),
    -- New column to force password change on first login
    password_change_required BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Insert a default admin user for the default company
-- The password is 'admin'. A new hash should be generated if you change this.
INSERT INTO users (username, password_hash, company_id, password_change_required)
VALUES (
    'admin',
    'scrypt:32768:8:1$YJ9Uu45G2V2xQuwM$9b7138092955f55331f4a6c4c23f2f1165d4b8ac04938a25e22971847e170940c60807c427027c95e1c458117f353a261a868a86a032d184a86b3605c116c4e1',
    1,
    TRUE
) ON CONFLICT (username) DO NOTHING;


-- Alter the health_check_runs table to use company_id
ALTER TABLE health_check_runs DROP COLUMN IF EXISTS company_name;
ALTER TABLE health_check_runs ADD COLUMN IF NOT EXISTS company_id INT;

-- (Optional) Add foreign key after data migration
-- ALTER TABLE health_check_runs ADD CONSTRAINT fk_company_id FOREIGN KEY (company_id) REFERENCES companies(id);

-- Grant permissions
GRANT SELECT, INSERT ON companies TO your_app_user;
GRANT SELECT, INSERT, UPDATE ON users TO your_app_user;
GRANT USAGE, SELECT ON SEQUENCE companies_id_seq TO your_app_user;
GRANT USAGE, SELECT ON SEQUENCE users_id_seq TO your_app_user;



