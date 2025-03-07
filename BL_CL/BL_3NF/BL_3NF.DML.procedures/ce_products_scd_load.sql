-- Procedure 5: `bl_cl.ce_products_scd_load` 
-- This procedure aggregates data from `SA_INDIVIDUAL_SALES` and `SA_COMPANY_SALES` by joining with product categories.
-- It inserts new products (those not already present in `CE_PRODUCTS_SCD`) while ensuring missing values are replaced with defaults like 'MANUAL' for the source system and '0' for price and cost.
-- The products are ranked by `PRODUCTS_SRC_ID` and `SOURCE_SYSTEM` to ensure only one record per product is inserted.
-- Any changes in product data (e.g., price or cost) will deactivate the old records, marking them with an `END_DT` timestamp.
-- New product data will be inserted with an `INSERT_DT` timestamp, set to active ('Y'), and a default `END_DT` of '9999-12-31'.
-- The procedure logs the row count and context after each successful insert and update using the `load_logger` function for tracking.
-- In case of errors, detailed exception handling captures the error context and logs it using `load_logger`.

DROP TABLE IF EXISTS temp_ranked_products;

CREATE TEMPORARY TABLE IF NOT EXISTS temp_ranked_products (
    PRODUCTS_SRC_ID TEXT,
    PROD_CATEGORY_ID INT, 
    PRICE_EACH DECIMAL(10, 2),
    COST_EACH DECIMAL(10, 2),
    SOURCE_SYSTEM TEXT,
    SOURCE_ENTITY TEXT,
    row_num INT
);




CREATE OR REPLACE PROCEDURE bl_cl.ce_products_scd_load()
AS $load_products$
DECLARE
    context TEXT;
    context_short TEXT;
    err_code TEXT;
    err_msg TEXT;
    err_context TEXT;
    err_context_short TEXT;
    err_detail TEXT;
    row_count INT;
    rows_inserted INT;
    rows_updated INT := 0;   
BEGIN
    -- Step 1: Deactivate all existing active records where data has changed
    -- Insert the ranked products into the temporary table
    TRUNCATE TABLE temp_ranked_products; -- Clear the previous data from the temp table

    INSERT INTO temp_ranked_products (PRODUCTS_SRC_ID, PROD_CATEGORY_ID, PRICE_EACH, COST_EACH, SOURCE_SYSTEM, SOURCE_ENTITY, row_num)
    SELECT 
        src.PRODUCTS_SRC_ID, 
        src.PROD_CATEGORY_ID, 
        src.PRICE_EACH, 
        src.COST_EACH,
        src.SOURCE_SYSTEM,
        src.SOURCE_ENTITY,
        ROW_NUMBER() OVER (PARTITION BY src.PRODUCTS_SRC_ID ORDER BY src.SOURCE_SYSTEM ASC) AS row_num
    FROM (
        -- First source: sa_individual_sales
        SELECT 
            src.productcode AS PRODUCTS_SRC_ID,
            COALESCE(cat.prod_category_id, -1) AS PROD_CATEGORY_ID, 
            src.price_each::DECIMAL(10, 2) AS PRICE_EACH,
            src.cogs_each::DECIMAL(10, 2) AS COST_EACH,
            'SA_INDIVIDUAL_SALES' AS SOURCE_SYSTEM,
            'SRC_INDIVIDUAL_SALES' AS SOURCE_ENTITY
        FROM sa_individual_sales.src_individual_sales src
        LEFT JOIN BL_3NF.CE_PRODUCT_CATEGORIES cat 
            ON src.product_group = cat.PROD_CATEGORY  -- Correctly matching the categories

        UNION ALL

        -- Second source: sa_company_sales
        SELECT  
            src.productcode AS PRODUCTS_SRC_ID,
            COALESCE(cat.prod_category_id, -1) AS PROD_CATEGORY_ID,  
            src.price_each::DECIMAL(10, 2) AS PRICE_EACH,
            src.cogs_each::DECIMAL(10, 2) AS COST_EACH,
            'SA_COMPANY_SALES' AS SOURCE_SYSTEM,
            'SRC_COMPANY_SALES' AS SOURCE_ENTITY
        FROM sa_company_sales.src_company_sales src
        LEFT JOIN BL_3NF.CE_PRODUCT_CATEGORIES cat 
            ON src.product_group = cat.PROD_CATEGORY  -- Correctly matching the categories
    ) src;

    -- Step 2: Deactivate the old records if any data has changed (price or cost)
    -- Use RETURNING to capture the updated rows and count them
    WITH updated_rows AS (
        UPDATE BL_3NF.CE_PRODUCTS_SCD p
        SET 
            IS_ACTIVE = 'N',           -- Set all records to inactive
            END_DT = NOW()             -- Set END_DT to current timestamp
        FROM temp_ranked_products rp
        WHERE p.PRODUCTS_SRC_ID = rp.PRODUCTS_SRC_ID
          AND p.SOURCE_SYSTEM = rp.SOURCE_SYSTEM
          AND (p.PRICE_EACH <> rp.PRICE_EACH OR p.COST_EACH <> rp.COST_EACH
               OR p.PROD_CATEGORY_ID <> rp.PROD_CATEGORY_ID)  
          AND p.IS_ACTIVE = 'Y'     -- Only deactivate if it is currently active
        RETURNING p.PRODUCTS_SRC_ID  -- Return the updated rows
    )
    SELECT COUNT(*) INTO rows_updated FROM updated_rows;  -- Count how many rows were updated

    -- Step 3: Insert new product data into CE_PRODUCTS_SCD (if there are changes)
    -- Use RETURNING to capture the inserted rows and count them
    WITH inserted_rows AS (
        INSERT INTO BL_3NF.CE_PRODUCTS_SCD (
            PROD_ID, PRODUCTS_SRC_ID, PROD_CATEGORY_ID, PRICE_EACH, COST_EACH, 
            SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT, UPDATE_DT, IS_ACTIVE, END_DT
        )
        SELECT 
            nextval('BL_3NF.SEQ_CE_PRODUCTS_SCD'),
            rp.PRODUCTS_SRC_ID, 
            rp.PROD_CATEGORY_ID,
            COALESCE(rp.PRICE_EACH, 0),
            COALESCE(rp.COST_EACH, 0),
            rp.SOURCE_SYSTEM,
            rp.SOURCE_ENTITY,
            NOW(),
            NOW(),
            'Y',                         -- Set to active
            '9999-12-31'::TIMESTAMP      -- Default end date, which will be updated later
        FROM temp_ranked_products rp
        WHERE rp.row_num = 1
          AND NOT EXISTS (
            SELECT 1 
            FROM BL_3NF.CE_PRODUCTS_SCD p 
            WHERE p.PRODUCTS_SRC_ID = rp.PRODUCTS_SRC_ID
              AND p.SOURCE_SYSTEM = rp.SOURCE_SYSTEM
              AND p.PROD_CATEGORY_ID = rp.PROD_CATEGORY_ID 
              AND p.IS_ACTIVE = 'Y'   -- Ensure we don't insert duplicates
        )
        RETURNING PRODUCTS_SRC_ID  -- Return the inserted rows
    )
    SELECT COUNT(*) INTO rows_inserted FROM inserted_rows;  -- Count how many rows were inserted


     -- Logging
    GET DIAGNOSTICS 
        context := PG_CONTEXT;
        row_count := ROW_COUNT;
        context_short := SUBSTRING(context FROM 'function (.*?) line');
    
    -- Call logger on successful insert
    CALL bl_cl.load_logger(context_short, rows_inserted, rows_updated);

EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        err_context := PG_EXCEPTION_CONTEXT,
        err_detail := PG_EXCEPTION_DETAIL;
        err_context_short := SUBSTRING(err_context FROM 'function (.*?) line');
    -- Call logger in case of exception
    CALL bl_cl.load_logger(err_context_short, 0, FORMAT('ERROR %s: %s. Details: %s', SQLSTATE, SQLERRM, err_detail));
    RAISE WARNING 'STATE: %, ERRM: %', SQLSTATE, SQLERRM;
END;
$load_products$ LANGUAGE plpgsql;




CALL bl_cl.ce_products_scd_load();

SELECT * FROM BL_3NF.CE_PRODUCTS_SCD;

SELECT * FROM bl_cl.mta_load_logs;
