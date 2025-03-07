-- Procedure 7: `bl_cl.ce_discounts_load` 
-- Combines discount data from individual and company sales, ranking by `discount_rate` and ensuring global uniqueness
-- Only the first occurrence each `discount_code` is inserted into `CE_DISCOUNTS`
-- The procedure tracks and logs the number of rows inserted and the context of the operation via `load_logger`
-- In case of errors, detailed exception information is logged, including error context and message


CREATE OR REPLACE PROCEDURE bl_cl.ce_discounts_load()
AS $load_discounts$
DECLARE
    context TEXT;
    context_short TEXT;
    row_count INT;
    rows_updated INT := 0;
    rows_inserted INT := 0;
    err_code TEXT;
    err_msg TEXT;
    err_context TEXT;
    err_context_short TEXT;
    err_detail TEXT;
BEGIN
    -- Insert new discount data, ensuring global uniqueness
    WITH ranked_data AS (
        -- Combine both sources and rank by discount_rate
        SELECT 
            discount_code AS DISCOUNTS_SRC_ID,  
            CAST(discount_rate AS DECIMAL) AS DISCOUNT_RATE,  
            'SA_SALES_INDIVIDUALS' AS source_system,  
            'SRC_INDIVIDUAL_SALES' AS source_entity,
            ROW_NUMBER() OVER (PARTITION BY discount_code ORDER BY discount_rate DESC) AS rn
        FROM sa_individual_sales.src_individual_sales
        WHERE discount_code IS NOT NULL AND discount_rate IS NOT NULL
        
        UNION ALL

        SELECT 
            discount_code AS DISCOUNTS_SRC_ID,  
            CAST(discount_rate AS DECIMAL) AS DISCOUNT_RATE,  
            'SA_SALES_COMPANIES' AS source_system,  
            'SRC_COMPANY_SALES' AS source_entity,
            ROW_NUMBER() OVER (PARTITION BY discount_code ORDER BY discount_rate DESC) AS rn
        FROM sa_company_sales.src_company_sales
        WHERE discount_code IS NOT NULL AND discount_rate IS NOT NULL
    )

    -- Insert only the first occurrence of each discount_code
    INSERT INTO BL_3NF.CE_DISCOUNTS (
        DISCOUNT_ID, 
        DISCOUNTS_SRC_ID, 
        DISCOUNT_RATE, 
        SOURCE_SYSTEM, 
        SOURCE_ENTITY, 
        INSERT_DT, 
        UPDATE_DT
    )
    SELECT 
        nextval('BL_3NF.SEQ_CE_DISCOUNTS'),  
        r.DISCOUNTS_SRC_ID, 
        r.DISCOUNT_RATE, 
        r.source_system, 
        r.source_entity,  
        NOW(), 
        NOW()
    FROM ranked_data r
    WHERE r.rn = 1  -- Keep only the first occurrence per discount_code
    AND NOT EXISTS (
        SELECT 1 
        FROM BL_3NF.CE_DISCOUNTS d 
        WHERE d.DISCOUNTS_SRC_ID = r.DISCOUNTS_SRC_ID
    );

    -- Logging the number of inserted rows
    GET DIAGNOSTICS rows_inserted = ROW_COUNT;
    GET DIAGNOSTICS context = PG_CONTEXT;
    context_short := SUBSTRING(context FROM 'function (.*?) line');
    
    -- Log success or no insert message
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




CALL bl_cl.ce_discounts_load();

SELECT * FROM BL_3NF.CE_DISCOUNTS;

SELECT * FROM bl_cl.mta_load_logs;
