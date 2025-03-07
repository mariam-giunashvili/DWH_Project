
-- 4. PROCEDURE bl_cl.dim_order_details_load
-- The `dim_order_details_load` procedure loads new order details into the `DIM_ORDER_DETAILS` table.  
-- It inserts records from the `CE_ORDERS` table that don't already exist in `DIM_ORDER_DETAILS`, ensuring no duplicates based on `ORDERS_SRC_ID`.  
-- After inserting, it logs the number of rows affected and the execution context for tracking.  
-- If any error occurs, the procedure logs the error details and raises a warning to notify users.  
-- This procedure helps to keep order details up-to-date while tracking and handling potential errors during the process.

CREATE OR REPLACE PROCEDURE bl_cl.dim_order_details_load()
AS $load_order_details$
DECLARE
    row_count INT;  -- Store the row count for logging
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
    -- Insert new order details if they don't already exist
    INSERT INTO BL_DM.DIM_ORDER_DETAILS (
        order_surr_id, 
        order_status, 
        insert_dt, 
        update_dt, 
        orders_src_id, 
        source_system, 
        source_entity
    )
    SELECT 
        nextval('BL_DM.SEQ_DIM_ORDER_DETAILS'),  
        co.ORDER_STATUS, 
        NOW(), 
        NOW(), 
        co.ORDERS_SRC_ID, 
        co.SOURCE_SYSTEM, 
        co.SOURCE_ENTITY
    FROM BL_3NF.CE_ORDERS co
    WHERE NOT EXISTS (
        SELECT 1 
        FROM BL_DM.DIM_ORDER_DETAILS d
        WHERE d.orders_src_id = co.ORDERS_SRC_ID
    );

    -- Get inserted row count
    GET DIAGNOSTICS 
        context := PG_CONTEXT,
        rows_inserted := ROW_COUNT;
        context_short := SUBSTRING(context FROM 'function (.*?) line');

    -- Log the operation 
    CALL bl_cl.load_logger(context_short, rows_inserted, rows_updated);

EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        err_context := PG_EXCEPTION_CONTEXT,
        err_detail 	:= PG_EXCEPTION_DETAIL;
        err_context_short := SUBSTRING(err_context FROM 'function (.*?) line');

    -- Call logger in case of exception
    CALL bl_cl.load_logger(err_context_short, 0, 0, FORMAT('ERROR %s: %s. Details: %s', SQLSTATE, SQLERRM, err_detail));
    RAISE WARNING 'STATE: %, ERRM: %', SQLSTATE, SQLERRM;
END;
$load_order_details$ LANGUAGE plpgsql;


-- Execute the procedure
CALL bl_cl.dim_order_details_load();

-- Verify the inserted data
SELECT * FROM BL_DM.DIM_ORDER_DETAILS;

SELECT * FROM bl_cl.mta_load_logs;