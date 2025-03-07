-- Procedure 6: `bl_cl.ce_orders_load` 
-- This procedure aggregates order data from both individual and company sales, joining with `CE_PRODUCTS_SCD` for product IDs
-- It inserts new orders that are not already present in `CE_ORDERS`, ensuring missing values are replaced with defaults like 'n.a.' and '1900-01-01'
-- update happens if changes are made in products_scd table due to deactivation of records
-- The procedure tracks and logs row counts and context after each successful insert and update via the `load_logger` function
-- In case of errors, it captures and logs detailed exception information, including the error context and message



CREATE OR REPLACE PROCEDURE bl_cl.ce_orders_load()
AS $$
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
    -- Create a temporary table to hold final data
    CREATE TEMP TABLE temp_final_data AS
    SELECT 
        src.ORDERS_SRC_ID, 
        COALESCE(lp.PROD_ID, -1) AS PROD_ID,  
        COALESCE(CAST(src.QUANTITY AS INTEGER), -1) AS QUANTITY, 
        COALESCE(CAST(src.ORDER_DT AS DATE), '1900-01-01'::DATE) AS ORDER_DT,  
        COALESCE(src.ORDER_STATUS, 'n.a') AS ORDER_STATUS, 
        COALESCE(src.source_system, 'n.a') AS source_system, 
        COALESCE(src.source_entity, 'n.a') AS source_entity
    FROM (
        SELECT 
            ordernumber AS ORDERS_SRC_ID, 
            productcode, 
            quantityordered AS QUANTITY,  
            orderdate AS ORDER_DT,  
            status AS ORDER_STATUS,  
            'SA_INDIVIDUAL_SALES' AS source_system,  
            'SRC_INDIVIDUAL_SALES' AS source_entity
        FROM sa_individual_sales.src_individual_sales
        WHERE ordernumber IS NOT NULL

        UNION ALL

        SELECT 
            ordernumber, 
            productcode,  
            quantityordered,  
            orderdate,  
            status,  
            'SA_COMPANY_SALES',  
            'SRC_COMPANY_SALES'  
        FROM sa_company_sales.src_company_sales
        WHERE ordernumber IS NOT NULL
    ) src
    LEFT JOIN (
        SELECT DISTINCT ON (products_src_id) 
            products_src_id, 
            prod_id
        FROM bl_3nf.ce_products_scd 
        WHERE is_active = 'Y'
        ORDER BY products_src_id, update_dt DESC
    ) lp ON src.productcode = lp.products_src_id;

    -- Update existing orders and track count
    WITH updated_rows AS (
        UPDATE BL_3NF.CE_ORDERS co
        SET 
            PROD_ID = fd.PROD_ID,
            UPDATE_DT = NOW()
        FROM temp_final_data fd
        JOIN bl_3nf.ce_products_scd ps ON fd.PROD_ID = ps.PROD_ID
        WHERE co.ORDERS_SRC_ID = fd.ORDERS_SRC_ID
          AND co.PROD_ID <> fd.PROD_ID
          AND co.UPDATE_DT < ps.UPDATE_DT
        RETURNING co.ORDERS_SRC_ID
    )
    SELECT COUNT(*) INTO rows_updated FROM updated_rows;

    -- Insert new orders and track count
    WITH inserted_rows AS (
        INSERT INTO BL_3NF.CE_ORDERS (
            ORDER_ID, ORDERS_SRC_ID, PROD_ID, QUANTITY, ORDER_DT, 
            ORDER_STATUS, SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT, UPDATE_DT
        )
        SELECT 
            nextval('BL_3NF.SEQ_CE_ORDERS'), ORDERS_SRC_ID, PROD_ID, 
            QUANTITY, ORDER_DT, ORDER_STATUS, source_system, source_entity, 
            NOW(), NOW()
        FROM temp_final_data fd
        WHERE NOT EXISTS (
            SELECT 1 FROM BL_3NF.CE_ORDERS co
            WHERE co.ORDERS_SRC_ID = fd.ORDERS_SRC_ID
        )
        RETURNING ORDERS_SRC_ID
    )
    SELECT COUNT(*) INTO rows_inserted FROM inserted_rows;

    -- Logging
    GET DIAGNOSTICS context := PG_CONTEXT;
    context_short := SUBSTRING(context FROM 'function (.*?) line');
    CALL bl_cl.load_logger(context_short, rows_inserted, rows_updated);
 

    -- Cleanup
    DROP TABLE temp_final_data;

EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS err_context := PG_EXCEPTION_CONTEXT, err_detail := PG_EXCEPTION_DETAIL;
    err_context_short := SUBSTRING(err_context FROM 'function (.*?) line');
    CALL bl_cl.load_logger(err_context_short, 0, FORMAT('ERROR %s: %s. Details: %s', SQLSTATE, SQLERRM, err_detail));
    RAISE WARNING 'STATE: %, ERRM: %', SQLSTATE, SQLERRM;
END;
$$ LANGUAGE plpgsql;




CALL bl_cl.ce_orders_load();

SELECT * FROM BL_3NF.CE_ORDERS;

SELECT * FROM bl_cl.mta_load_logs;

