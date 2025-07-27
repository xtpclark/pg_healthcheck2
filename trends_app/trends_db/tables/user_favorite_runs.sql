-- =====================================================================
-- New table to track user's favorite runs
--
-- This table creates a many-to-many relationship between users and
-- health_check_runs, allowing each user to have their own list of
-- favorite runs without modifying the main health_check_runs table.
-- =====================================================================

CREATE TABLE user_favorite_runs (
    user_id INTEGER NOT NULL,
    run_id INTEGER NOT NULL,
    
    -- Foreign key to the users table. When a user is deleted,
    -- their favorites are also deleted (ON DELETE CASCADE).
    CONSTRAINT fk_user
        FOREIGN KEY(user_id) 
        REFERENCES users(id)
        ON DELETE CASCADE,

    -- Foreign key to the health_check_runs table.
    CONSTRAINT fk_run
        FOREIGN KEY(run_id) 
        REFERENCES health_check_runs(id)
        ON DELETE CASCADE,

    -- The primary key ensures that a user can only favorite a run once.
    PRIMARY KEY (user_id, run_id)
);

-- Optional: Add an index for faster lookups when querying by run_id
CREATE INDEX idx_user_favorite_runs_run_id ON user_favorite_runs(run_id);
