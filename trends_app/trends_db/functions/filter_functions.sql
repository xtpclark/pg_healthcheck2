-- Filter Management Functions (Modern JSONB approach)

-- Save or update a user filter
CREATE OR REPLACE FUNCTION save_user_filter(
    p_screen TEXT,
    p_username TEXT,
    p_filter_name TEXT,
    p_filter_values JSONB,
    p_set_selected BOOLEAN DEFAULT true
)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_filter_id INTEGER;
BEGIN
    -- Check if filter with this name already exists for this user/screen
    SELECT filter_id INTO v_filter_id
    FROM filter
    WHERE filter_screen = p_screen
      AND filter_username = p_username
      AND filter_name = p_filter_name;

    IF v_filter_id IS NOT NULL THEN
        -- Update existing filter
        UPDATE filter
        SET filter_values = p_filter_values,
            updated_at = NOW()
        WHERE filter_id = v_filter_id;
    ELSE
        -- Insert new filter
        INSERT INTO filter (filter_screen, filter_username, filter_name, filter_values, filter_selected)
        VALUES (p_screen, p_username, p_filter_name, p_filter_values, false)
        RETURNING filter_id INTO v_filter_id;
    END IF;

    -- Set as selected if requested
    IF p_set_selected THEN
        PERFORM set_selected_filter(p_screen, p_username, v_filter_id);
    END IF;

    RETURN v_filter_id;
END;
$$;

-- Set a filter as the selected one for a user/screen
CREATE OR REPLACE FUNCTION set_selected_filter(
    p_screen TEXT,
    p_username TEXT,
    p_filter_id INTEGER
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    -- Deselect all filters for this user/screen
    UPDATE filter
    SET filter_selected = false
    WHERE filter_screen = p_screen
      AND filter_username = p_username;

    -- Select the specified filter
    UPDATE filter
    SET filter_selected = true,
        updated_at = NOW()
    WHERE filter_id = p_filter_id;
END;
$$;

-- Get the selected filter for a user/screen
CREATE OR REPLACE FUNCTION get_selected_filter(
    p_screen TEXT,
    p_username TEXT
)
RETURNS TABLE (
    filter_id INTEGER,
    filter_name TEXT,
    filter_values JSONB
)
LANGUAGE plpgsql
STABLE
AS $$
BEGIN
    RETURN QUERY
    SELECT f.filter_id, f.filter_name, f.filter_values
    FROM filter f
    WHERE f.filter_screen = p_screen
      AND f.filter_username = p_username
      AND f.filter_selected = true
    LIMIT 1;
END;
$$;

-- Get all saved filters for a user/screen
CREATE OR REPLACE FUNCTION get_user_filters(
    p_screen TEXT,
    p_username TEXT
)
RETURNS TABLE (
    filter_id INTEGER,
    filter_name TEXT,
    filter_values JSONB,
    filter_selected BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
LANGUAGE plpgsql
STABLE
AS $$
BEGIN
    RETURN QUERY
    SELECT f.filter_id, f.filter_name, f.filter_values, f.filter_selected,
           f.created_at, f.updated_at
    FROM filter f
    WHERE f.filter_screen = p_screen
      AND f.filter_username = p_username
    ORDER BY f.filter_selected DESC, f.updated_at DESC;
END;
$$;

-- Delete a user filter
CREATE OR REPLACE FUNCTION delete_user_filter(
    p_filter_id INTEGER,
    p_username TEXT
)
RETURNS BOOLEAN
LANGUAGE plpgsql
AS $$
DECLARE
    v_deleted BOOLEAN;
BEGIN
    DELETE FROM filter
    WHERE filter_id = p_filter_id
      AND filter_username = p_username;

    GET DIAGNOSTICS v_deleted = ROW_COUNT;
    RETURN v_deleted > 0;
END;
$$;

-- Clear selected filter (reset to no filter selected)
CREATE OR REPLACE FUNCTION clear_selected_filter(
    p_screen TEXT,
    p_username TEXT
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE filter
    SET filter_selected = false
    WHERE filter_screen = p_screen
      AND filter_username = p_username;
END;
$$;

COMMENT ON FUNCTION save_user_filter IS 'Save or update a user filter preset with JSONB values';
COMMENT ON FUNCTION set_selected_filter IS 'Set a filter as the currently selected one for user/screen';
COMMENT ON FUNCTION get_selected_filter IS 'Get the currently selected filter for user/screen';
COMMENT ON FUNCTION get_user_filters IS 'Get all saved filters for user/screen';
COMMENT ON FUNCTION delete_user_filter IS 'Delete a user filter preset';
COMMENT ON FUNCTION clear_selected_filter IS 'Clear the selected filter (reset to no filter)';
