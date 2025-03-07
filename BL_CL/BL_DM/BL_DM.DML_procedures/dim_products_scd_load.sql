-- 2.PROCEDURE bl_cl.dim_products_scd_load
-- The procedure `load_dim_products_scd` handles slowly changing dimensions for product data.  
-- Step 1: It deactivates old records if there are changes in price or cost, keeping historical data.  
-- Step 2: Inserts a new version of the product, marking it as active and preserving the history of the product.  
-- Step 3: The number of affected rows is captured for logging to track the procedure's performance.  
-- Error handling is implemented to log any issues and raise a warning if something goes wrong.  
-- This ensures accurate tracking of product data changes while maintaining historical records in the dimension table.




CREATE OR REPLACE PROCEDURE bl_cl.dim_products_scd_load()
AS $$ 
DECLARE
    row_count INT;  
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
    -- Step 1: Deactivate old records only if price or cost has changed
    UPDATE BL_DM.DIM_PRODUCTS_SCD d
    SET 
        is_active = 'N',
        end_dt = NOW()
    WHERE is_active = 'Y'  
    AND EXISTS (
        SELECT 1
        FROM BL_3NF.CE_PRODUCTS_SCD s
        WHERE s.products_src_id = d.products_src_id
        AND s.source_system = d.source_system
        AND s.is_active = 'Y'
        AND (s.price_each <> d.price_each OR s.cost_each <> d.cost_each)
    );

    -- Step 2: Get the number of updated rows
    GET DIAGNOSTICS rows_updated = ROW_COUNT;

    -- Step 3: Insert the new version while keeping history
    WITH inserted_rows AS (
        INSERT INTO BL_DM.DIM_PRODUCTS_SCD (
            prod_surr_id, 
            prod_category_id, 
            prod_category, 
            price_each, 
            cost_each, 
            insert_dt, 
            update_dt, 
            is_active, 
            end_dt, 
            products_src_id, 
            source_system, 
            source_entity
        )
        SELECT 
            nextval('BL_DM.SEQ_DIM_PRODUCTS_SCD'),  
            COALESCE(s.prod_category_id, 0),     
            COALESCE(cat.prod_category, 'N.A.'),  
            s.price_each, 
            s.cost_each, 
            NOW(), 
            NOW(), 
            'Y',               
            '9999-12-31',       
            s.products_src_id, 
            s.source_system, 
            s.source_entity
        FROM BL_3NF.CE_PRODUCTS_SCD s
        LEFT JOIN BL_3NF.CE_PRODUCT_CATEGORIES cat 
            ON s.prod_category_id = cat.prod_category_id
        WHERE s.is_active = 'Y'
        AND NOT EXISTS (
            -- Prevent inserting if an active record already exists for the same product and source system
            SELECT 1 
            FROM BL_DM.DIM_PRODUCTS_SCD d
            WHERE d.products_src_id = s.products_src_id
            AND d.source_system = s.source_system
            AND d.is_active = 'Y'
        )
        AND (
            -- Allow inserts only if there is no active record, or an old version has been deactivated
            NOT EXISTS (
                SELECT 1 
                FROM BL_DM.DIM_PRODUCTS_SCD d
                WHERE d.products_src_id = s.products_src_id
                AND d.source_system = s.source_system
                AND d.is_active = 'N'
            ) 
            OR EXISTS (
                -- Allow inserts when price or cost has changed and the old version is deactivated
                SELECT 1 
                FROM BL_DM.DIM_PRODUCTS_SCD d
                WHERE d.products_src_id = s.products_src_id
                AND d.source_system = s.source_system
                AND d.is_active = 'N'
                AND (d.price_each <> s.price_each OR d.cost_each <> s.cost_each)
            )
        )
        RETURNING products_src_id
    )
    SELECT COUNT(*) INTO rows_inserted FROM inserted_rows;

    GET DIAGNOSTICS context := PG_CONTEXT;
    context_short := SUBSTRING(context FROM 'function (.*?) line');
    -- Log the operation with rows_updated set to 0
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
$$ LANGUAGE plpgsql;



CALL bl_cl.dim_products_scd_load();


SELECT * FROM bl_3nf.ce_products_scd cps ;

-- Log the procedure execution
SELECT * FROM bl_cl.mta_load_logs mll;
