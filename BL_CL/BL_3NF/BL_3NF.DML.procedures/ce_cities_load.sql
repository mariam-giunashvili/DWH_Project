-- Function 2: `bl_cl.ce_cities_load`   
-- This function is created to load distinct city data into the `bl_3nf.ce_cities` table, 
-- aggregating city information from two different sales data sources: individual and company sales.  
-- To ensure uniqueness, the function uses `ROW_NUMBER()` to rank cities and selects only the first occurrence 
-- of each city per source system. A `LEFT JOIN` is performed with the `bl_3nf.ce_countries` table 
-- to fetch the `country_id` for each city, defaulting to `-1` when no match is found. 
-- This process ensures no duplicate cities are inserted, and logging is implemented to track both successful inserts and errors.

CREATE OR REPLACE FUNCTION bl_cl.ce_cities_load()
RETURNS TABLE (
    city_id INT,
    cities_src_id TEXT,
    city TEXT,
    country_id INT,
    source_system TEXT,
    source_entity TEXT,
    insert_dt TIMESTAMP,
    update_dt TIMESTAMP
) AS $load_cities$
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
    -- Step 1: Extract distinct cities from sales data
    WITH src_data AS (
        -- Individual Sales Data Source
        SELECT DISTINCT 
            sa.city AS city, 
            'SA_INDIVIDUAL_SALES' AS source_system, 
            'SRC_INDIVIDUAL_SALES' AS source_entity, 
            sa.country AS country
        FROM sa_individual_sales.src_individual_sales sa
        WHERE sa.city IS NOT NULL
        
        UNION
        
        -- Company Sales Data Source
        SELECT DISTINCT 
            sc.city AS city, 
            'SA_COMPANY_SALES' AS source_system, 
            'SRC_COMPANY_SALES' AS source_entity, 
            sc.country AS country
        FROM sa_company_sales.src_company_sales sc
        WHERE sc.city IS NOT NULL
    ),
    
    -- Step 2: Rank the cities by source
    ranked_data AS (
        SELECT 
            sd.city, 
            sd.country, 
            sd.source_system, 
            sd.source_entity, 
            ROW_NUMBER() OVER (PARTITION BY sd.city ORDER BY sd.source_system) AS row_num
        FROM src_data sd
    )

    -- Step 3: Insert the data into ce_cities
    INSERT INTO bl_3nf.ce_cities (
        city_id,
        cities_src_id,
        city,
        country_id,
        source_system,
        source_entity,
        insert_dt,
        update_dt
    )
    SELECT 
        nextval('BL_3NF.SEQ_CE_CITIES'), -- Auto-generate city_id
        COALESCE(rd.city, 'n.a') AS cities_src_id, -- Default 'n.a' if city is NULL
        COALESCE(rd.city, 'n.a') AS city, -- Default 'n.a' if city is NULL
        COALESCE(cc.country_id, -1) AS country_id, -- Default -1 if country_id is NULL
        COALESCE(rd.source_system, 'MANUAL') AS source_system, -- Default 'MANUAL' if NULL
        COALESCE(rd.source_entity, 'MANUAL') AS source_entity, -- Default 'MANUAL' if NULL
        COALESCE(NOW(), '1900-01-01'::timestamp) AS insert_dt, -- Default insert date if NULL
        COALESCE(NOW(), '1900-01-01'::timestamp) AS update_dt -- Default update date if NULL
    FROM ranked_data rd
    LEFT JOIN bl_3nf.ce_countries cc 
        ON cc.countries_src_id = rd.country -- Get country_id if available
    WHERE rd.row_num = 1 -- Only insert the first occurrence of each city
      AND NOT EXISTS (
        SELECT 1
        FROM bl_3nf.ce_cities c
        WHERE c.cities_src_id = rd.city
              AND c.source_system = rd.source_system
              AND c.source_entity = rd.source_entity
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
$load_cities$ LANGUAGE plpgsql;




SELECT * FROM bl_cl.ce_cities_load();


SELECT * FROM bl_3nf.ce_cities;


SELECT * FROM bl_cl.mta_load_logs;

