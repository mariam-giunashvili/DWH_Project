-- Procedure 1: bl_cl.ce_countries_load  
-- I created this procedure to load distinct countries from individual and company sales sources into `bl_3nf.ce_countries`.  
-- To ensure uniqueness, I used `ROW_NUMBER()` to select only the first occurrence of each country per source system.  
-- I used `NOT EXISTS` to prevent inserting duplicate countries with the same `countries_src_id`, `source_system`, and `source_entity`.  
-- Default values are assigned using `COALESCE` to handle missing data, ensuring data consistency.  
-- Logging is implemented to track successful inserts and exceptions, using `bl_cl.load_logger`.


CREATE OR REPLACE PROCEDURE bl_cl.ce_countries_load()
AS $load_countries$
DECLARE
    context TEXT;
    context_short TEXT;
    rows_inserted INT;
    rows_updated INT := 0;  
    err_code TEXT;
    err_msg TEXT;
    err_context TEXT;
    err_context_short TEXT;
    err_detail TEXT;
BEGIN
    -- Original INSERT operation
    WITH src_data AS (
        -- First Source: Sales Data for Individual Customers
        SELECT DISTINCT country, 'SA_INDIVIDUAL_SALES' AS source_system, 'SRC_INDIVIDUAL_SALES' AS source_entity
        FROM sa_individual_sales.src_individual_sales
        WHERE country IS NOT NULL
        UNION
        -- Second Source: Sales Data for Company Customers
        SELECT DISTINCT country, 'SA_COMPANY_SALES' AS source_system, 'SRC_COMPANY_SALES' AS source_entity
        FROM sa_company_sales.src_company_sales
        WHERE country IS NOT NULL
    ),
    ranked_data AS (
        SELECT src.country,
               src.source_system,
               src.source_entity,
               ROW_NUMBER() OVER (PARTITION BY src.country ORDER BY src.source_system) AS row_num
        FROM src_data src
    )
    INSERT INTO bl_3nf.ce_countries (
        country_id,
        countries_src_id,
        country,
        insert_dt,
        update_dt,
        source_system,
        source_entity
    )
    SELECT 
        nextval('bl_3NF.SEQ_CE_COUNTRIES'), -- This will auto-generate country_id
        COALESCE(ranked_data.country, 'n.a') AS countries_src_id,  -- Default 'n.a' if country is NULL
        COALESCE(ranked_data.country, 'n.a') AS country, -- Default if country is NULL
        COALESCE(NOW(), '1900-01-01'::timestamp) AS insert_dt,  -- Default insert date if NULL
        COALESCE(NOW(), '1900-01-01'::timestamp) AS update_dt,  -- Default update date if NULL
        COALESCE(ranked_data.source_system, 'MANUAL') AS source_system, -- Default source_system if NULL
        COALESCE(ranked_data.source_entity, 'MANUAL') AS source_entity  -- Default source_entity if NULL
    FROM ranked_data
    WHERE ranked_data.row_num = 1  -- Only insert the first occurrence of each country
      AND NOT EXISTS (
        SELECT 1
        FROM bl_3nf.ce_countries cc
        WHERE cc.countries_src_id = ranked_data.country
              AND cc.source_system = ranked_data.source_system
              AND cc.source_entity = ranked_data.source_entity
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
$load_countries$ LANGUAGE plpgsql;


CALL bl_cl.ce_countries_load();

SELECT * FROM bl_3nf.ce_countries cc ;

SELECT * FROM bl_cl.mta_load_logs;
