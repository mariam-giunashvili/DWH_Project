-- 5.PROCEDURE BL_CL.DIM_PAYMENTS_LOAD
-- The `DIM_PAYMENTS_LOAD` procedure inserts new payment records into the `DIM_PAYMENTS` table, ensuring no duplicates based on `PAYMENTS_SRC_ID`.  
-- It calculates the payment amount using the `price_each` from `CE_PRODUCTS_SCD` and the quantity from `CE_ORDERS`.  
-- The procedure logs the number of inserted rows and the context of the operation for tracking.  
-- In case of any errors, it captures and logs the error details, raising a warning with the error state and message.  
-- This procedure ensures the payment records are accurately loaded and helps with monitoring potential issues during execution.


CREATE OR REPLACE PROCEDURE BL_CL.DIM_PAYMENTS_LOAD()
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
    -- Insert new payment records into DIM_PAYMENTS
    INSERT INTO BL_DM.DIM_PAYMENTS (
        payment_surr_id,
        payment_type,
        amount,
        insert_dt,
        update_dt,
        payments_src_id,
        source_system,
        source_entity
    )
    SELECT 
        nextval('BL_DM.SEQ_DIM_PAYMENTS'),  -- Generate surrogate key
        COALESCE(p.PAYMENT_TYPE, 'UNKNOWN'),
        COALESCE(pr.price_each * o.quantity, 0.00) AS amount,  -- Calculate amount
        NOW(),  -- Insertion timestamp
        NOW(),  -- Update timestamp
        p.PAYMENTS_SRC_ID,
        p.SOURCE_SYSTEM,
        p.SOURCE_ENTITY
    FROM BL_3NF.CE_PAYMENTS p
    JOIN BL_3NF.CE_ORDERS o 
        ON o.ORDER_ID = p.ORDER_ID
    JOIN BL_3NF.CE_PRODUCTS_SCD pr
        ON o.PROD_ID = pr.PROD_ID  -- Join to get price_each
    WHERE p.payment_id!= -1
    AND NOT EXISTS (
        SELECT 1 FROM BL_DM.DIM_PAYMENTS dp 
        WHERE dp.payments_src_id = p.PAYMENTS_SRC_ID
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




-- Execute the procedure
CALL bl_cl.dim_payments_load();

-- Verify the inserted data
SELECT * FROM BL_DM.DIM_PAYMENTS;

SELECT * FROM bl_cl.mta_load_logs;

