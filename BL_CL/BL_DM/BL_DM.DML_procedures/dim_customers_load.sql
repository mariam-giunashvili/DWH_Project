-- 1.PROCEDURE bl_cl.dim_customers_load
-- Dropped the existing unique constraint to ensure proper constraint handling.  
-- Added a new unique constraint on `customers_src_id` and `cust_last_name` to maintain data integrity.  
-- The `dim_customers_load` procedure loads customer data from source tables into `dim_customers`.  
-- It checks if records exist, inserting new ones or updating existing ones to keep data current.  
-- The procedure tracks inserted and updated rows for logging and ensures no duplicates are created.  
-- Error handling captures and logs any issues during the process, ensuring robustness.  
-- Overall, this ensures up-to-date and consistent customer data in the dimension table.


-- Drop the unique constraint if it exists
ALTER TABLE bl_dm.dim_customers 
DROP CONSTRAINT IF EXISTS dim_customers_unique_constraint;

-- Add the unique constraint if it doesn't exist
ALTER TABLE bl_dm.dim_customers 
ADD CONSTRAINT dim_customers_unique_constraint UNIQUE (customers_src_id);

CREATE OR REPLACE PROCEDURE bl_cl.dim_customers_load()
AS $load_customers$
DECLARE
    context TEXT;
    context_short TEXT;
    row_count INT;
    rows_inserted INT := 0;  -- Variable to accumulate the total inserted rows
    rows_updated INT := 0;   -- Variable to accumulate the total updated rows
    err_code TEXT;
    err_msg TEXT;
    err_context TEXT;
    err_context_short TEXT;
    err_detail TEXT;
    procedure_start_time TIMESTAMP := NOW();

    query TEXT := FORMAT(
        'INSERT INTO bl_dm.dim_customers 
        SELECT nextval(%L), $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NOW(), NOW(), $12, $13, $14
        ON CONFLICT (customers_src_id) 
        DO UPDATE SET 
            cust_last_name = EXCLUDED.cust_last_name, 
            update_dt = NOW()
         WHERE dim_customers.cust_last_name <> EXCLUDED.cust_last_name;',
        'BL_DM.SEQ_DIM_CUSTOMERS'
);
    
    cust_cursor CURSOR FOR 
        SELECT 
            cu.cust_first_name, cu.cust_last_name, cu.cust_company_name, cu.cust_type, cu.customers_src_id,
            a.address_id, a.address, a.postal_code,
            c.city_id, c.city,
            co.country_id, co.country,
            cu.customers_src_id,  
            cu.source_system, cu.source_entity
        FROM bl_3nf.ce_customers cu
        JOIN bl_3nf.ce_addresses a ON cu.address_id = a.address_id
        JOIN bl_3nf.ce_cities c ON a.city_id = c.city_id
        JOIN bl_3nf.ce_countries co ON c.country_id = co.country_id
        WHERE cu.cust_id IS NOT NULL;
    
    recordvar RECORD;

BEGIN
    row_count := 0;

    FOR recordvar IN cust_cursor LOOP
        -- Check if the customer already exists in the DIM table by customers_src_id
        IF NOT EXISTS (
            SELECT 1 FROM bl_dm.dim_customers 
            WHERE customers_src_id = recordvar.customers_src_id
        ) THEN
            -- If the record doesn't exist, insert it
            EXECUTE query USING 
                recordvar.cust_first_name, recordvar.cust_last_name, recordvar.cust_company_name, recordvar.cust_type,
                recordvar.address_id, recordvar.address, recordvar.postal_code,
                recordvar.city_id, recordvar.city, recordvar.country_id, recordvar.country,
                recordvar.customers_src_id,  
                recordvar.source_system, recordvar.source_entity;

            -- Capture the number of rows affected by the insert
            GET DIAGNOSTICS row_count = ROW_COUNT;
            rows_inserted := rows_inserted + row_count;
        ELSE
            -- If the record exists, update only the cust_last_name
            EXECUTE 'UPDATE bl_dm.dim_customers 
            SET cust_last_name = $1, update_dt = NOW() 
            WHERE customers_src_id = $2 
            AND cust_last_name <> $1'
            USING recordvar.cust_last_name, recordvar.customers_src_id;
       
                    -- Capture update count
            GET DIAGNOSTICS row_count = ROW_COUNT;
            rows_updated := rows_updated + row_count;

        END IF;
    END LOOP;

    -- Log the total inserted rows after the loop ends
    context := 'dim_customers_load()';  -- Context for the log
    context_short := 'dim_customers_load';  -- Short context for the log
    CALL bl_cl.load_logger(context_short, rows_inserted, rows_updated);

    GET DIAGNOSTICS 
        context := PG_CONTEXT;
        context_short := SUBSTRING(context FROM 'function (.*?) line');


EXCEPTION WHEN OTHERS THEN
    -- Error handling if something goes wrong
    GET STACKED DIAGNOSTICS
        err_context := PG_EXCEPTION_CONTEXT,
        err_detail  := PG_EXCEPTION_DETAIL;
        err_context_short := SUBSTRING(err_context FROM 'function (.*?) line');

    CALL bl_cl.load_logger(err_context_short, 0, 0, FORMAT('ERROR %s: %s. Details: %s', SQLSTATE, SQLERRM, err_detail));
    RAISE WARNING 'STATE: %, ERRM: %', SQLSTATE, SQLERRM;
    RETURN;
END;
$load_customers$ LANGUAGE plpgsql;

-- Call the procedure
CALL bl_cl.dim_customers_load();

-- Check the results
SELECT * FROM BL_DM.DIM_CUSTOMERS;

SELECT * FROM bl_cl.mta_load_logs;


