-- 3. PROCEDURE bl_cl.dim_discounts_load
-- The `dim_discounts_load` procedure loads new discount records into the `dim_discounts` table.  
-- It inserts records that don't already exist based on `DISCOUNTS_SRC_ID` to avoid duplicates.  
-- After the insertions, it logs the number of rows affected to track the process.  
-- Error handling is included to log any issues and raise warnings if an error occurs during execution.  
-- This procedure ensures that discount records are correctly inserted and tracked while logging the result of the operation.


CREATE OR REPLACE PROCEDURE bl_cl.dim_discounts_load()
AS $load_discounts$
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
    -- Insert new discounts if they don't already exist
    INSERT INTO bl_dm.dim_discounts (
        DISCOUNT_SURR_ID, 
        DISCOUNT_CODE,
        DISCOUNT_RATE, 
        DISCOUNTS_SRC_ID, 
        SOURCE_SYSTEM, 
        SOURCE_ENTITY, 
        INSERT_DT, 
        UPDATE_DT
    )
    SELECT 
        nextval('BL_DM.SEQ_DIM_DISCOUNTS'),  
        FD.DISCOUNTS_SRC_ID, 
        FD.DISCOUNT_RATE, 
        FD.DISCOUNTS_SRC_ID, 
        FD.SOURCE_SYSTEM, 
        FD.SOURCE_ENTITY,  
        NOW(), 
        NOW()
    FROM BL_3NF.CE_DISCOUNTS FD
    WHERE NOT EXISTS (
        SELECT 1 
        FROM BL_DM.DIM_DISCOUNTS D
        WHERE D.DISCOUNTS_SRC_ID = FD.DISCOUNTS_SRC_ID
    );

    -- Logging the number of inserted rows
    GET DIAGNOSTICS rows_inserted = ROW_COUNT;
    GET DIAGNOSTICS context = PG_CONTEXT;
    context_short := SUBSTRING(context FROM 'function (.*?) line');
    
    -- call Logger 
    CALL bl_cl.load_logger(context_short, rows_inserted, rows_updated);

EXCEPTION WHEN OTHERS THEN
    -- Handle and log exceptions
    GET STACKED DIAGNOSTICS
        err_context := PG_EXCEPTION_CONTEXT,
        err_detail := PG_EXCEPTION_DETAIL;
    err_context_short := SUBSTRING(err_context FROM 'function (.*?) line');
    
    CALL bl_cl.load_logger(err_context_short, 0, 0, FORMAT('ERROR %s: %s. Details: %s', SQLSTATE, SQLERRM, err_detail));
    RAISE WARNING 'STATE: %, ERRM: %', SQLSTATE, SQLERRM;

END;
$load_discounts$ LANGUAGE plpgsql;




CALL bl_cl.dim_discounts_load();

SELECT * FROM BL_dm.DIM_DISCOUNTS;

SELECT * FROM bl_cl.mta_load_logs;
