CREATE OR REPLACE PROCEDURE purge_data(
    table_name VARCHAR,
    date_column VARCHAR,
    date_format VARCHAR,
    s3_path VARCHAR,
    days_to_retain INT DEFAULT NULL,
    months_to_retain INT DEFAULT NULL,
    test_mode BOOLEAN DEFAULT FALSE
)
AS $$
DECLARE
    cutoff_date DATE;
    where_clause VARCHAR;
    select_sql VARCHAR;
    delete_sql VARCHAR;
    unload_sql VARCHAR;
    row_count INT;
BEGIN
    -- Validate retention parameters
    IF days_to_retain IS NULL AND months_to_retain IS NULL THEN
        RAISE EXCEPTION 'Either days_to_retain or months_to_retain must be provided';
    END IF;

    -- Calculate cutoff date
    IF days_to_retain IS NOT NULL THEN
        cutoff_date := CURRENT_DATE - days_to_retain;
    ELSE
        cutoff_date := CURRENT_DATE - CAST(months_to_retain || ' months' AS INTERVAL);
    END IF;

    -- Build WHERE clause
    IF date_format IS NOT NULL THEN
        where_clause := 'TO_DATE(' || quote_ident(date_column) || ', ' || quote_literal(date_format) || ') < ' || quote_literal(cutoff_date);
    ELSE
        where_clause := quote_ident(date_column) || ' < ' || quote_literal(cutoff_date);
    END IF;

    -- Get count of affected rows
    select_sql := 'SELECT COUNT(*) FROM ' || quote_ident(table_name) || ' WHERE ' || where_clause;
    EXECUTE select_sql INTO row_count;
    RAISE INFO 'Rows to delete: %', row_count;

    IF test_mode THEN
        RAISE INFO 'Test mode enabled. No data will be deleted or exported.';
    ELSE
        -- Export data to S3
        unload_sql := 'UNLOAD (SELECT * FROM ' || quote_ident(table_name) || ' WHERE ' || where_clause || ') TO ' || quote_literal(s3_path) || ' iam_role default WITH (FORMAT CSV, PARALLEL OFF)';
        RAISE INFO 'Exporting data to S3: %', unload_sql;
        EXECUTE unload_sql;

        -- Delete data
        delete_sql := 'DELETE FROM ' || quote_ident(table_name) || ' WHERE ' || where_clause;
        RAISE INFO 'Deleting data: %', delete_sql;
        EXECUTE delete_sql;
    END IF;
END;
$$ LANGUAGE plpgsql;