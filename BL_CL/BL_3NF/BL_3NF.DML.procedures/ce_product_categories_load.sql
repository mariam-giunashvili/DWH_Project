-- Procedure 4: ce_product_categories_load
-- This procedure inserts unique product categories into `BL_3NF.CE_PRODUCT_CATEGORIES` from individual and company sales sources.  
-- It loops through distinct `product_group_code` and `product_group` values from both sources.  
-- `NOT EXISTS` ensures no duplicate product categories are inserted.  
-- Default values are assigned for missing data.  
-- The `ROW_COUNT` tracks affected rows, and `bl_cl.load_logger` logs the count.  
-- Errors are caught, logged, and displayed as warnings.


CREATE OR REPLACE PROCEDURE bl_cl.ce_product_categories_load()
AS $$
DECLARE
    row_count INT := 0;
    context TEXT;
    context_short TEXT;
    rows_inserted INT := 0;
    rows_updated INT := 0;  
    err_code TEXT;
    err_msg TEXT;
    err_context TEXT;
    err_context_short TEXT;
    err_detail TEXT;
    rec RECORD;
BEGIN
    -- Loop through both sources (SA_INDIVIDUAL_SALES and SA_COMPANY_SALES)
    FOR rec IN 
        -- First Source: Product Categories from Individual Sales Data
        SELECT DISTINCT product_group_code, product_group, 'SA_INDIVIDUAL_SALES' AS source_system, 'SRC_INDIVIDUAL_SALES' AS source_entity
        FROM sa_individual_sales.src_individual_sales
        WHERE product_group IS NOT NULL

        UNION ALL
        
        -- Second Source: Product Categories from Company Sales Data
        SELECT DISTINCT product_group_code, product_group, 'SA_COMPANY_SALES' AS source_system, 'SRC_COMPANY_SALES' AS source_entity
        FROM sa_company_sales.src_company_sales
        WHERE product_group IS NOT NULL
    LOOP
        -- Insert each record into CE_PRODUCT_CATEGORIES if it doesn't already exist
        INSERT INTO BL_3NF.CE_PRODUCT_CATEGORIES (
            PROD_CATEGORY_ID,
            PRODUCT_CATEGORIES_SRC_ID,
            PROD_CATEGORY,
            SOURCE_SYSTEM,
            SOURCE_ENTITY,
            INSERT_DT,
            UPDATE_DT
        )
        SELECT 
            nextval('BL_3NF.SEQ_CE_PRODUCT_CATEGORIES'),
            COALESCE(rec.product_group_code, 'n.a.') AS PRODUCT_CATEGORIES_SRC_ID,  -- Default 'n.a.' if NULL
            COALESCE(rec.product_group, 'n.a') AS PROD_CATEGORY,  -- Default 'Unknown' if NULL
            COALESCE(rec.source_system, 'MANUAL') AS SOURCE_SYSTEM,  -- Default 'MANUAL' if NULL
            COALESCE(rec.source_entity, 'MANUAL') AS SOURCE_ENTITY,  -- Default 'MANUAL' if NULL
            COALESCE(NOW(), '1900-01-01'::timestamp) AS INSERT_DT,  -- Default insert date if NULL
            COALESCE(NOW(), '1900-01-01'::timestamp) AS UPDATE_DT  -- Default update date if NULL
        WHERE NOT EXISTS (
            -- Ensure no duplicate categories are inserted based on product_group
            SELECT 1 
            FROM BL_3NF.CE_PRODUCT_CATEGORIES dest
            WHERE dest.PROD_CATEGORY = rec.product_group
        );

        -- Capture row count for each insert operation
        GET DIAGNOSTICS 
        context := PG_CONTEXT,
        row_count = ROW_COUNT;
        rows_inserted := rows_inserted + row_count;
        context_short := SUBSTRING(context FROM 'function (.*?) line');

        -- Log the operation 
    END LOOP;
   
    CALL bl_cl.load_logger(context_short, rows_inserted, rows_updated);

EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        err_context := PG_EXCEPTION_CONTEXT,
        err_detail := PG_EXCEPTION_DETAIL;
        err_context_short := SUBSTRING(err_context FROM 'function (.*?) line');

    -- Log the error 
    CALL bl_cl.load_logger(err_context_short, 0, 0, FORMAT('ERROR %s: %s. Details: %s', SQLSTATE, SQLERRM, err_detail));
    RAISE WARNING 'STATE: %, ERRM: %', SQLSTATE, SQLERRM;
END;
$$ LANGUAGE plpgsql;



CALL bl_cl.ce_product_categories_load();

SELECT * FROM bl_3nf.ce_product_categories cpc ;

SELECT * FROM bl_cl.mta_load_logs;

