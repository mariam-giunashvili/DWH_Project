-- Procedure 9: `bl_cl.ce_payments_load` 
-- Combines payment data from individual and company sales, ensuring valid order references
-- Joins the data with customer and discount information, assigning appropriate discount IDs
-- Inserts only the first payment record for each order, ensuring no duplicates for the same payment
-- Logs the number of inserted rows and the operation context, handling exceptions and errors if they occur


CREATE OR REPLACE PROCEDURE bl_cl.ce_payments_load()
AS $load_payments$
DECLARE
    context TEXT;
    context_short TEXT;
    rows_inserted INT;
    rows_updated INT := 0;  -- Default to 0 since there's no update logic
    err_code TEXT;
    err_msg TEXT;
    err_context TEXT;
    err_context_short TEXT;
    err_detail TEXT;
BEGIN

    -- Source data from both individual and company sales
    WITH src_data AS (
        -- Individual sales payments
        SELECT 
            s.customercode AS customers_src_id,
            s.ordernumber AS ordernumber,  
            s.payment_code AS payments_src_id,
            s.payment_method AS payment_type,
            s.discount_code,  
            'SA_INDIVIDUAL_SALES' AS source_system,
            'SRC_INDIVIDUAL_SALES' AS source_entity
        FROM sa_individual_sales.src_individual_sales s
        WHERE EXISTS (
            SELECT 1 FROM BL_3NF.CE_ORDERS o WHERE o.orders_src_id = s.ordernumber
        )
        UNION ALL
        -- Company sales payments
        SELECT 
            s.customercode AS customers_src_id,
            s.ordernumber AS ordernumber,  
            s.payment_code AS payments_src_id,
            s.payment_method AS payment_type,
            s.discount_code,  
            'SA_COMPANY_SALES' AS source_system,
            'SRC_COMPANY_SALES' AS source_entity
        FROM sa_company_sales.src_company_sales s
        WHERE EXISTS (
            SELECT 1 FROM BL_3NF.CE_ORDERS o WHERE o.orders_src_id = s.ordernumber
        )
    ),
    final_data AS (
        -- Join the data with CE_CUSTOMERS and CE_DISCOUNTS
        SELECT 
            src.customers_src_id, 
            src.payments_src_id, 
            src.ordernumber, 
            src.payment_type, 
            src.discount_code, 
            src.source_system, 
            src.source_entity,
            c.CUST_ID,  
            COALESCE(d.DISCOUNT_ID, -1) AS DISCOUNT_ID, 
            ROW_NUMBER() OVER (PARTITION BY src.ordernumber ORDER BY src.source_system, src.source_entity) AS rn
        FROM src_data src
        JOIN BL_3NF.CE_CUSTOMERS c 
            ON c.CUSTOMERS_SRC_ID = src.customers_src_id
        LEFT JOIN BL_3NF.CE_DISCOUNTS d 
            ON src.discount_code IS NOT NULL  
            AND d.DISCOUNTS_SRC_ID = src.discount_code
    )
    
    -- Insert new payment data into CE_PAYMENTS
    INSERT INTO BL_3NF.CE_PAYMENTS (
        PAYMENT_ID, 
        PAYMENTS_SRC_ID, 
        ORDER_ID, 
        CUST_ID, 
        PAYMENT_TYPE, 
        DISCOUNT_ID, 
        SOURCE_SYSTEM, 
        SOURCE_ENTITY, 
        INSERT_DT, 
        UPDATE_DT
    )
    SELECT 
        nextval('BL_3NF.SEQ_CE_PAYMENTS') AS PAYMENT_ID,  
        COALESCE(fd.payments_src_id, 'n.a'), 
        COALESCE(o.ORDER_ID, -1),  
        COALESCE(fd.CUST_ID, -1),  
        COALESCE(fd.payment_type, 'n.a'),  
        fd.DISCOUNT_ID,  
        COALESCE(fd.source_system, 'MANUAL'),  
        COALESCE(fd.source_entity, 'MANUAL'),  
        NOW(),  
        NOW()
    FROM final_data fd
    JOIN BL_3NF.CE_ORDERS o 
        ON o.ORDERS_SRC_ID = fd.ordernumber 
    WHERE fd.rn = 1  -- Ensure only the first row for each ordernumber is inserted
    AND NOT EXISTS (
        SELECT 1
        FROM BL_3NF.CE_PAYMENTS p
        WHERE p.PAYMENTS_SRC_ID = fd.payments_src_id
              AND p.ORDER_ID = o.ORDER_ID  -- Ensures no duplicate payments for the same order
    );

    -- Step 4: Logging the result
    GET DIAGNOSTICS 
        context := PG_CONTEXT,
        rows_inserted := ROW_COUNT;
        context_short := SUBSTRING(context FROM 'function (.*?) line');

    -- Call logger with rows_updated = 0
    CALL bl_cl.load_logger(context_short, rows_inserted, rows_updated);

EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        err_context := PG_EXCEPTION_CONTEXT,
        err_detail := PG_EXCEPTION_DETAIL;
        err_context_short := SUBSTRING(err_context FROM 'function (.*?) line');

    -- Log the error with 0 inserted and 0 updated rows
    CALL bl_cl.load_logger(err_context_short, 0, 0, FORMAT('ERROR %s: %s. Details: %s', SQLSTATE, SQLERRM, err_detail));
    RAISE WARNING 'STATE: %, ERRM: %', SQLSTATE, SQLERRM;
END;
$load_payments$ LANGUAGE plpgsql;






CALL bl_cl.ce_payments_load();

SELECT * FROM BL_3NF.CE_PAYMENTS;

SELECT * FROM bl_cl.mta_load_logs;
