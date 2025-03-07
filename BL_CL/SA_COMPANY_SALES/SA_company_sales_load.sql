-- *loading data from the first source: MiniMotors_sales_companies.csv 

-- - **Creates external tables**: The script first creates an external table to read the CSV file using the `CREATE FOREIGN TABLE` command.
-- - **Ensures the source table exists**: If the source table does not exist, it is created to match the structure of the external table.
-- - **Adds refresh timestamp**: A new column `refresh_dt` is added if it doesn't already exist, to track when the data was refreshed.
-- - **Inserts new data**: Data is loaded into the source table from the external table, but only new records (those that don't already exist in the source table).
-- - **Updates data**: data is updated if products' price or cost is updated in source, which helps to optimize the process of scd2
-- - **Logging**: The procedure logs both successful and failed attempts to load data by calling the `load_logger` procedure. In case of failure, detailed error information is captured and logged.
--  procedure is rerunnable, meaning it can be executed multiple times without causing duplication or data inconsistency.  





CREATE OR REPLACE PROCEDURE sa_company_sales.src_company_sales_load_csv(IN file_abspath TEXT)
AS $$
DECLARE
    context TEXT;
    context_short TEXT;
    row_count INT;
    rows_inserted INT := 0;
    rows_updated INT := 0;
    err_code TEXT;
    err_context TEXT;
    err_context_short TEXT;
    err_detail TEXT;
    err_msg TEXT;
    ext_company_sales_qs TEXT := FORMAT(
        'CREATE FOREIGN TABLE IF NOT EXISTS sa_company_sales.ext_company_sales (
            ordernumber VARCHAR,
            quantityordered VARCHAR,
            price_each VARCHAR,
            cogs_each VARCHAR,
            total_sales VARCHAR,
            orderdate VARCHAR,
            status VARCHAR,
            qtr VARCHAR,
            month VARCHAR,
            year VARCHAR,
            product_group VARCHAR,
            product_group_code VARCHAR,
            productcode VARCHAR,
            customername VARCHAR,
            customercode VARCHAR,
            customertype VARCHAR,
            postalcode VARCHAR,
            address VARCHAR,
            city VARCHAR,
            country VARCHAR,
            discount_code VARCHAR,
            discount_rate VARCHAR,
            payment_code VARCHAR,
            payment_method VARCHAR
        )
        SERVER minimotors_sales_external_server
        OPTIONS (
            FILENAME %L,
            FORMAT %L,
            HEADER %L
        );', $1, 'csv', 'true');
BEGIN
    RAISE INFO 'Preparing Tables...';

    -- Drop the external table for fresh loading
    DROP FOREIGN TABLE IF EXISTS sa_company_sales.ext_company_sales;
    EXECUTE ext_company_sales_qs;

    -- Ensure the source table exists
    CREATE TABLE IF NOT EXISTS sa_company_sales.src_company_sales (LIKE sa_company_sales.ext_company_sales);

    -- Add refresh timestamp column if not exists
    ALTER TABLE sa_company_sales.src_company_sales
    ADD COLUMN IF NOT EXISTS refresh_dt TIMESTAMPTZ DEFAULT NOW();

    RAISE INFO 'Loading data into src_company_sales...';

    -- Step 1: Update existing records if price_each, or cogs_each have changed
    -- Update with RETURNING to track the updated rows count
    WITH updated_rows AS (
        UPDATE sa_company_sales.src_company_sales src
        SET
            price_each = ext.price_each,
            cogs_each = ext.cogs_each,
            refresh_dt = NOW()  -- Update refresh timestamp
        FROM sa_company_sales.ext_company_sales ext
        WHERE src.ordernumber = ext.ordernumber
          AND (
            src.price_each != ext.price_each
            OR src.cogs_each != ext.cogs_each
          )
        RETURNING 1
    )
    SELECT COUNT(*) INTO rows_updated FROM updated_rows;

    -- Step 2: Insert only the records that do not already exist in the source table
    -- Insert with RETURNING to track the inserted rows count
    WITH inserted_rows AS (
        INSERT INTO sa_company_sales.src_company_sales
        SELECT
            ordernumber, quantityordered, price_each, cogs_each, total_sales,
            orderdate, status, qtr, month, year, product_group, product_group_code,
            productcode, customername, customercode, customertype, postalcode,
            address, city, country, discount_code, discount_rate, payment_code,
            payment_method, NOW()
        FROM sa_company_sales.ext_company_sales ext
        WHERE NOT EXISTS (
            SELECT 1
            FROM sa_company_sales.src_company_sales src
            WHERE src.ordernumber = ext.ordernumber
        )
        RETURNING 1
    )
    SELECT COUNT(*) INTO rows_inserted FROM inserted_rows;

    -- Logging
    GET DIAGNOSTICS
        context := PG_CONTEXT;
    context_short := SUBSTRING(context FROM 'function (.*?) line');

    -- Log success with separate counts for inserts and updates
    CALL bl_cl.load_logger(context_short, rows_inserted, rows_updated);

    RAISE INFO 'Successfully Inserted % rows and Updated % rows in src_company_sales.',  rows_inserted, rows_updated;

EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        err_code := RETURNED_SQLSTATE,
        err_msg := MESSAGE_TEXT,
        err_context := PG_EXCEPTION_CONTEXT,
        err_detail := PG_EXCEPTION_DETAIL;
    err_context_short := SUBSTRING(err_context FROM 'function (.*?) line');

    -- Log failure
    CALL bl_cl.load_logger(err_context_short, 0, 0, FORMAT('ERROR %s: %s. Details: %s', err_code, err_msg, err_detail));

    RAISE EXCEPTION 'STATE: %, ERROR: %, DETAILS: %', err_code, err_msg, err_detail;
END;
$$ LANGUAGE plpgsql;





CALL sa_company_sales.src_company_sales_load_csv('C:\\Users\\Meri\\Desktop\\Mari\\MiniMotors_sales_companies.csv');

--call logger
SELECT * FROM bl_cl.mta_load_logs;



