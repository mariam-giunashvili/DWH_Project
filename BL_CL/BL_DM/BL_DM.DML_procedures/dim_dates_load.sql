-- 6. PROCEDURE bl_cl.dim_dates_load
-- The `dim_dates_load` procedure inserts date records into the `dim_dates` table from January 1, 2022, to December 31, 2025.  
-- It generates the date series and extracts various date components like day of the week, day number, month number, month name, quarter, and year.  
-- The procedure ensures no duplicate entries by checking if the date already exists in the table.  
-- After inserting, it logs the number of affected rows.  
-- If an error occurs, the procedure logs the error details and raises a warning with the error message.


CREATE OR REPLACE PROCEDURE bl_cl.load_dim_dates()
AS $$
DECLARE
    rows_inserted INT := 0;  
    rows_updated INT := 0; 
    context TEXT;
    context_short TEXT;
    err_code TEXT;
    err_msg TEXT;
    err_context TEXT;
    err_context_short TEXT;
    err_detail TEXT;
BEGIN
    -- Insert dates into the dim_dates table, ensuring no duplicates
    INSERT INTO BL_DM.dim_dates (order_dt, day_in_week, day_number_in_month, month_number, month_name, quarter_number, year)
    SELECT 
        d::DATE,                        -- Convert the generated series to DATE type
        TO_CHAR(d, 'FMDay'),            -- Get the full name of the day (e.g., 'Monday', 'Tuesday')
        EXTRACT(DAY FROM d),            -- Extract the day number from the date
        EXTRACT(MONTH FROM d),          -- Extract the month number from the date
        TO_CHAR(d, 'FMMonth'),          -- Get the full name of the month (e.g., 'January', 'February')
        EXTRACT(QUARTER FROM d),        -- Extract the quarter number (1, 2, 3, or 4)
        EXTRACT(YEAR FROM d)            -- Extract the year from the date (e.g., 2022, 2023)
    FROM generate_series('2022-01-01'::DATE, '2025-12-31'::DATE, INTERVAL '1 day') AS d
    WHERE NOT EXISTS (  -- Check if the date already exists in the dim_dates table
        SELECT 1 
        FROM BL_DM.dim_dates dt
        WHERE dt.order_dt = d::DATE
    );

    -- Logging successful insert
    GET DIAGNOSTICS 
        context := PG_CONTEXT,
        rows_inserted := ROW_COUNT;
    context_short := SUBSTRING(context FROM 'function (.*?) line');

    CALL BL_CL.LOAD_LOGGER(context_short, rows_inserted, rows_updated);

EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        err_context := PG_EXCEPTION_CONTEXT,
        err_detail := PG_EXCEPTION_DETAIL;
    err_context_short := SUBSTRING(err_context FROM 'function (.*?) line');

    -- Logging the error
    CALL BL_CL.LOAD_LOGGER(err_context_short, 0, FORMAT('ERROR %s: %s. Details: %s', SQLSTATE, SQLERRM, err_detail));
    RAISE WARNING 'STATE: %, ERRM: %', SQLSTATE, SQLERRM;
END;
$$ LANGUAGE plpgsql;





CALL bl_cl.dim_dates_load();

SELECT * FROM BL_DM.DIM_DATES;

SELECT * FROM bl_cl.mta_load_logs;