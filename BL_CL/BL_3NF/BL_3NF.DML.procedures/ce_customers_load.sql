-- Procedure 8: `bl_cl.ce_customers_load`
-- It combines customer data from individual and company sales, handling both types separately
-- Matches customers to addresses based on postalcode and assigns appropriate address_id
-- Only inserts the first occurrence of each customer per source system/entity combination
-- This procedure operates according to SCD1: if customer surname gets updated in the source the code ensures that correspondng update is made at bl_3nf level as well
-- Logs the number of inserted and updated rows and the operation context, handling exceptions and errors if they occur




CREATE OR REPLACE PROCEDURE bl_cl.ce_customers_load()
AS $$
DECLARE
    context TEXT;
    context_short TEXT;
    row_count INT;
    err_code TEXT;
    err_msg TEXT;
    err_context TEXT;
    err_context_short TEXT;
    err_detail TEXT;
    rows_updated INT;
    rows_inserted INT;
BEGIN
    -- Update existing customers (surname mismatch) only for src_individual_sales
    UPDATE BL_3NF.CE_CUSTOMERS c
    SET 
        CUST_LAST_NAME = updates.cust_last_name,  -- Update surname
        UPDATE_DT = NOW()  -- Update the timestamp
    FROM (
        SELECT 
            customercode AS customers_src_id, 
            customer_surname AS cust_last_name
        FROM sa_individual_sales.src_individual_sales
    ) updates
    WHERE c.CUSTOMERS_SRC_ID = updates.customers_src_id
    AND c.CUST_LAST_NAME != updates.cust_last_name  -- Only update if surname is different
    AND c.source_system = 'SA_INDIVIDUAL_SALES';

    -- Get number of rows affected
    GET DIAGNOSTICS rows_updated = ROW_COUNT;


    -- Source data from both individual and company sales
    WITH src_data AS (
        -- Company customers
        SELECT 
            customercode AS customers_src_id,
            'n.a' AS cust_first_name,  -- No first name for companies
            'n.a' AS cust_last_name,   -- No last name for companies
            customername AS cust_company_name,  -- Company name
            customertype AS cust_type,
            postalcode AS postalcode,  -- Postalcode for matching
            'SA_COMPANY_SALES' AS source_system,
            'SRC_COMPANY_SALES' AS source_entity
        FROM sa_company_sales.src_company_sales
        WHERE customercode IS NOT NULL
        
        UNION ALL
        
        -- Individual customers
        SELECT 
            customercode AS customers_src_id,
            customer_name AS cust_first_name,  -- First name for individuals
            customer_surname AS cust_last_name,  -- Last name for individuals
            'n.a' AS cust_company_name,  -- 'n.a' for company name in individual customer case
            customertype AS cust_type,
            postalcode AS postalcode,  -- Postalcode for matching
            'SA_INDIVIDUAL_SALES' AS source_system,
            'SRC_INDIVIDUAL_SALES' AS source_entity
        FROM sa_individual_sales.src_individual_sales
        WHERE customercode IS NOT NULL
    ),
    address_match AS (
        -- Match postalcode with address_src_id to get address_id
        SELECT 
            a.addresses_src_id, 
            a.address_id
        FROM BL_3NF.CE_ADDRESSES a
    ),
    ranked_data AS (
        -- Rank the data by customers_src_id and source_system
        SELECT src.*,
               ROW_NUMBER() OVER (PARTITION BY src.customers_src_id ORDER BY src.source_system) AS row_num
        FROM src_data src
    )
    -- Insert customers with correct address_id
    INSERT INTO BL_3NF.CE_CUSTOMERS (
        CUST_ID,
        CUSTOMERS_SRC_ID,
        CUST_FIRST_NAME,
        CUST_LAST_NAME,
        CUST_COMPANY_NAME,
        CUST_TYPE,
        ADDRESS_ID,
        SOURCE_SYSTEM,
        SOURCE_ENTITY,
        INSERT_DT,
        UPDATE_DT
    )
    SELECT 
        nextval('BL_3NF.SEQ_CE_CUSTOMERS'),  -- Auto-generate CUST_ID
        COALESCE(ranked_data.customers_src_id, 'n.a'), 
        COALESCE(ranked_data.cust_first_name, 'n.a'), 
        COALESCE(ranked_data.cust_last_name, 'n.a'), 
        COALESCE(ranked_data.cust_company_name, 'n.a'), 
        COALESCE(ranked_data.cust_type, 'n.a'), 
        -- Use CASE WHEN to insert address_id based on postalcode match
        CASE 
            WHEN ranked_data.postalcode = am.addresses_src_id THEN am.address_id
            ELSE -1
        END AS address_id,  -- Default to -1 if no match found
        COALESCE(ranked_data.source_system, 'MANUAL'), 
        COALESCE(ranked_data.source_entity, 'MANUAL'), 
        COALESCE(NOW(), '1900-01-01'::timestamp), 
        COALESCE(NOW(), '1900-01-01'::timestamp)
    FROM ranked_data
    LEFT JOIN address_match am ON ranked_data.postalcode = am.addresses_src_id  -- Matching postalcode to get address_id
    WHERE ranked_data.row_num = 1  -- Insert only the first occurrence of each customer
    AND NOT EXISTS (
        SELECT 1 
        FROM BL_3NF.CE_CUSTOMERS c 
        WHERE c.CUSTOMERS_SRC_ID = ranked_data.customers_src_id
              AND c.SOURCE_SYSTEM = ranked_data.source_system
              AND c.SOURCE_ENTITY = ranked_data.source_entity);

    -- Step 4: Logging the result
    GET DIAGNOSTICS 
        context := PG_CONTEXT,
        rows_inserted := ROW_COUNT;
        context_short := SUBSTRING(context FROM 'function (.*?) line');

    -- Call logger 
    CALL bl_cl.load_logger(context_short, rows_inserted, rows_updated);

EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        err_context := PG_EXCEPTION_CONTEXT,
        err_detail := PG_EXCEPTION_DETAIL;
        err_context_short := SUBSTRING(err_context FROM 'function (.*?) line');

    -- Log the error 
    CALL bl_cl.load_logger(err_context_short, 0, 0 , FORMAT('ERROR %s: %s. Details: %s', SQLSTATE, SQLERRM, err_detail));
    RAISE WARNING 'STATE: %, ERRM: %', SQLSTATE, SQLERRM;
END;
$$ LANGUAGE plpgsql;



CALL bl_cl.ce_customers_load();

SELECT * FROM BL_3NF.CE_CUSTOMERS;

SELECT * FROM bl_cl.mta_load_logs;

