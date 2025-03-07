
-- Procedure 3: ce_addresses_load
-- This procedure inserts unique addresses into `bl_3nf.ce_addresses` from individual and company sales sources.  
-- `ROW_NUMBER()` is used to ensure only the first occurrence of each postal code is inserted.  
-- A `NOT EXISTS` condition prevents duplicate postal codes with the same `source_system` and `source_entity`.  
-- A `LEFT JOIN` maps the correct `city_id`, defaulting to `-1` if not found.  
-- Logging is included to track successful inserts and handle errors using `bl_cl.load_logger`.


CREATE OR REPLACE PROCEDURE bl_cl.ce_addresses_load()
AS $$
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
    -- Fill with data from external sources
    WITH src_data AS (
        -- First Source: Addresses from Sales Data for Individual Customers
        SELECT DISTINCT postalcode, address, city, 'SA_INDIVIDUAL_SALES' AS source_system, 'SRC_INDIVIDUAL_SALES' AS source_entity
        FROM sa_individual_sales.src_individual_sales
        WHERE address IS NOT NULL AND city IS NOT NULL
        UNION ALL
        -- Second Source: Addresses from Sales Data for Company Customers
        SELECT DISTINCT postalcode, address, city, 'SA_COMPANY_SALES' AS source_system, 'SRC_COMPANY_SALES' AS source_entity
        FROM sa_company_sales.src_company_sales
        WHERE address IS NOT NULL AND city IS NOT NULL
    ),
    ranked_data AS (
        SELECT src.postalcode,
               src.address,
               src.city,
               src.source_system,
               src.source_entity,
               ROW_NUMBER() OVER (PARTITION BY src.postalcode ORDER BY src.source_system) AS row_num
        FROM src_data src
    )
    -- Insert new addresses into CE_ADDRESSES
    INSERT INTO bl_3nf.ce_addresses (
        ADDRESS_ID,
        ADDRESSES_SRC_ID,  -- Now correctly use postalcode for ADDRESSES_SRC_ID
        POSTAL_CODE,
        ADDRESS,
        CITY_ID,
        SOURCE_SYSTEM,
        SOURCE_ENTITY,
        INSERT_DT,
        UPDATE_DT
    )
    SELECT nextval('BL_3NF.SEQ_CE_ADDRESSES'),
           COALESCE(ranked_data.postalcode, 'n.a') AS ADDRESSES_SRC_ID,  -- postalcode for ADDRESSES_SRC_ID
           COALESCE(ranked_data.postalcode, 'n.a') AS POSTAL_CODE,  -- Default postal code if NULL
           COALESCE(ranked_data.address, 'n.a') AS ADDRESS,  -- Default address if NULL
           COALESCE(cc.city_id, -1) AS CITY_ID,  -- Default -1 for city_id if NULL
           COALESCE(ranked_data.source_system, 'MANUAL') AS SOURCE_SYSTEM,  -- Default source_system if NULL
           COALESCE(ranked_data.source_entity, 'MANUAL') AS SOURCE_ENTITY,  -- Default source_entity if NULL
           COALESCE(NOW(), '1900-01-01'::timestamp) AS INSERT_DT,  -- Default insert date if NULL
           COALESCE(NOW(), '1900-01-01'::timestamp) AS UPDATE_DT  -- Default update date if NULL
    FROM ranked_data
    LEFT JOIN bl_3nf.ce_cities cc ON cc.cities_src_id = ranked_data.city  -- LEFT JOIN to ensure we get all cities, even if country is missing
    WHERE ranked_data.row_num = 1  -- Only insert the first occurrence of each postal code
    AND NOT EXISTS (
        -- Check if the postal code already exists in the target table (for the same source and entity)
        SELECT 1
        FROM bl_3nf.ce_addresses ca
        WHERE ca.postal_code = ranked_data.postalcode  -- Matching postal code as the key
        AND ca.source_system = ranked_data.source_system
        AND ca.source_entity = ranked_data.source_entity
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
        err_detail := PG_EXCEPTION_DETAIL;
        err_context_short := SUBSTRING(err_context FROM 'function (.*?) line');

    -- Log the error 
    CALL bl_cl.load_logger(err_context_short, 0, 0, FORMAT('ERROR %s: %s. Details: %s', SQLSTATE, SQLERRM, err_detail));
    RAISE WARNING 'STATE: %, ERRM: %', SQLSTATE, SQLERRM;
END;
$$ LANGUAGE plpgsql;


CALL bl_cl.ce_addresses_load();

SELECT * FROM bl_3nf.ce_addresses;

SELECT * FROM bl_cl.mta_load_logs;