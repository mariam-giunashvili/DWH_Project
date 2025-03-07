-- Table for test groups

CREATE TABLE IF NOT EXISTS bl_cl.data_quality_checks (
    check_id VARCHAR(50) PRIMARY KEY,
    check_title VARCHAR(50) NOT NULL UNIQUE,
    check_purpose TEXT NOT NULL,
    check_query TEXT NOT NULL,
    last_executed TIMESTAMPTZ DEFAULT NOW(),
    run_status VARCHAR(20) DEFAULT 'Pending'
);


-- Now, we insert our given two test groups into the table

INSERT INTO bl_cl.data_quality_checks (check_id, check_title, check_purpose, check_query)
VALUES 
    (
        'NO_DUPLICATES',
        'No Duplicates Validation',
        'Ensures that target tables in BL_3NF and BL_DM layers contain no duplicate records based on unique keys.',
        'CALL bl_cl.validate_no_duplicates();'
    ),
    (
        'FULL_DATA_TRANSFER',
        'Full Data Transfer Check',
        'Verifies that all records from the SA layer are present in the BL_3NF and BL_DM layers.',
        'CALL bl_cl.validate_data_transfer();'
    );


-- procedure below will run all the tests from the table

CREATE OR REPLACE PROCEDURE bl_cl.run_all_quality_checks() AS 
$run_all_quality_checks$
DECLARE
    check_record RECORD;
BEGIN
    FOR check_record IN SELECT * FROM bl_cl.data_quality_checks LOOP
        RAISE NOTICE 'Running check: % (ID: %)', check_record.check_title, check_record.check_id;
        EXECUTE check_record.check_query;
    END LOOP;
END;
$run_all_quality_checks$ LANGUAGE plpgsql;




-- this procedure checks for duplicates in both bl_3nf and bl_dm tables

CREATE OR REPLACE PROCEDURE bl_cl.validate_no_duplicates() AS 
$$
DECLARE
    table_record RECORD;  -- to hold table name and unique keys during iteration
    query_text TEXT;      -- to store dynamically constructed SQL query
    duplicate_count INT;  -- to store the count of duplicate groups
    total_duplicates INT := 0; -- Tracks if any duplicates were found
BEGIN
    FOR table_record IN (
        SELECT table_name, unique_keys
        FROM (
            VALUES
                -- BL_3NF schema tables
                ('bl_3nf.ce_countries', ARRAY['countries_src_id', 'source_system', 'source_entity']),
                ('bl_3nf.ce_cities', ARRAY['cities_src_id', 'source_system', 'source_entity']),
                ('bl_3nf.ce_addresses', ARRAY['addresses_src_id', 'source_system', 'source_entity']),
                ('bl_3nf.ce_customers', ARRAY['customers_src_id', 'source_system', 'source_entity']),
                ('bl_3nf.ce_discounts', ARRAY['discounts_src_id', 'source_system', 'source_entity']),
                ('bl_3nf.ce_orders', ARRAY['orders_src_id', 'source_system', 'source_entity']),
                ('bl_3nf.ce_products_scd', ARRAY['products_src_id', 'source_system', 'source_entity', 'is_active']),
                ('bl_3nf.ce_payments', ARRAY['payments_src_id', 'source_system', 'source_entity']),
                ('bl_3nf.ce_product_categories', ARRAY['product_categories_src_id', 'source_system', 'source_entity']),
               
             
                -- BL_DM schema tables
                ('bl_dm.dim_customers', ARRAY['customers_src_id', 'source_system', 'source_entity']),
                ('bl_dm.dim_order_details', ARRAY['orders_src_id', 'source_system', 'source_entity']),
                ('bl_dm.dim_payments', ARRAY['payments_src_id', 'source_system', 'source_entity']),
                ('bl_dm.dim_products_scd', ARRAY['products_src_id', 'source_system', 'source_entity', 'is_active']),
                ('bl_dm.dim_discounts', ARRAY['discounts_src_id', 'source_system', 'source_entity']),
                ('bl_dm.dim_dates', ARRAY['order_dt']),
                ('bl_dm.fct_orders', ARRAY['order_surr_id', 'order_dt'])
        ) AS t(table_name, unique_keys)
    ) LOOP
        query_text := format(
            'SELECT COUNT(*) FROM (
                SELECT %s
                FROM %s
                GROUP BY %s
                HAVING COUNT(*) > 1
            ) AS duplicates',
            array_to_string(table_record.unique_keys, ', '),
            table_record.table_name,
            array_to_string(table_record.unique_keys, ', ')
        );
        
        EXECUTE query_text INTO duplicate_count;
        
        IF duplicate_count > 1 THEN
            RAISE WARNING 'Table % has % duplicate records!', table_record.table_name, duplicate_count;
            total_duplicates := total_duplicates + duplicate_count; --tracker for status
        ELSE
            RAISE NOTICE 'Table % has no duplicates.', table_record.table_name;
        END IF;
    END LOOP;

    -- If no duplicates were found in any table, update the check status to 'Completed'
    IF total_duplicates = 0 THEN
        UPDATE bl_cl.data_quality_checks
        SET run_status = 'Completed', last_executed = NOW()
        WHERE check_id = 'NO_DUPLICATES';
        
        RAISE NOTICE 'No duplicates found across all tables. Test status updated to COMPLETED.';
    END IF;
END;
$$ LANGUAGE plpgsql;







-- This procedure checks whether all necessary data was properly fetched from the sources and that no records were lost.
CREATE OR REPLACE PROCEDURE bl_cl.validate_data_transfer() AS 
$$
DECLARE
    missing_count INT;          
    total_missing INT := 0;    
    order_count_3nf INT;       
    order_count_dm INT;        
BEGIN
-- Check if all customers are in BL_3NF (CE_CUSTOMERS)
SELECT COUNT(*)
INTO missing_count
FROM (
    SELECT DISTINCT customercode, 'SA_INDIVIDUAL_SALES' AS source_system, 'SRC_INDIVIDUAL_SALES' AS source_entity
    FROM sa_individual_sales.src_individual_sales
    UNION ALL
    SELECT DISTINCT customercode, 'SA_COMPANY_SALES' AS source_system, 'SRC_COMPANY_SALES' AS source_entity
    FROM sa_company_sales.src_company_sales
) s
LEFT JOIN bl_3nf.ce_customers c
ON s.customercode = c.customers_src_id
AND (c.source_system = s.source_system OR c.source_system = 'SA_INDIVIDUAL_SALES' OR c.source_system = 'SA_COMPANY_SALES')
AND (c.source_entity = s.source_entity OR c.source_entity = 'SRC_INDIVIDUAL_SALES' OR c.source_entity = 'SRC_COMPANY_SALES')
WHERE c.cust_id IS NULL;

IF missing_count > 0 THEN
    RAISE WARNING 'There are % missing customers in BL_3NF.', missing_count;
    total_missing := total_missing + missing_count;
ELSE
    RAISE NOTICE 'All customers are present in BL_3NF.';
END IF;

-- Check if all countries are in BL_3NF (CE_COUNTRIES)
SELECT COUNT(*)
INTO missing_count
FROM (
    SELECT DISTINCT country, 'SA_INDIVIDUAL_SALES' AS source_system, 'SRC_INDIVIDUAL_SALES' AS source_entity
    FROM sa_individual_sales.src_individual_sales
    UNION ALL
    SELECT DISTINCT country, 'SA_COMPANY_SALES' AS source_system, 'SRC_COMPANY_SALES' AS source_entity
    FROM sa_company_sales.src_company_sales
) s
LEFT JOIN bl_3nf.ce_countries c
ON s.country = c.countries_src_id
AND (c.source_system = s.source_system OR c.source_system = 'SA_INDIVIDUAL_SALES' OR c.source_system = 'SA_COMPANY_SALES')
AND (c.source_entity = s.source_entity OR c.source_entity = 'SRC_INDIVIDUAL_SALES' OR c.source_entity = 'SRC_COMPANY_SALES')
WHERE c.country_id IS NULL;

IF missing_count > 0 THEN
    RAISE WARNING 'There are % missing countries in BL_3NF.', missing_count;
    total_missing := total_missing + missing_count;
ELSE
    RAISE NOTICE 'All countries are present in BL_3NF.';
END IF;

-- Check if all cities are in BL_3NF (CE_CITIES)
SELECT COUNT(*)
INTO missing_count
FROM (
    SELECT DISTINCT city, 'SA_INDIVIDUAL_SALES' AS source_system, 'SRC_INDIVIDUAL_SALES' AS source_entity
    FROM sa_individual_sales.src_individual_sales
    UNION ALL
    SELECT DISTINCT city, 'SA_COMPANY_SALES' AS source_system, 'SRC_COMPANY_SALES' AS source_entity
    FROM sa_company_sales.src_company_sales
) s
LEFT JOIN bl_3nf.ce_cities c
ON s.city = c.cities_src_id
AND (c.source_system = s.source_system OR c.source_system = 'SA_INDIVIDUAL_SALES' OR c.source_system = 'SA_COMPANY_SALES')
AND (c.source_entity = s.source_entity OR c.source_entity = 'SRC_INDIVIDUAL_SALES' OR c.source_entity = 'SRC_COMPANY_SALES')
WHERE c.city_id IS NULL;

IF missing_count > 0 THEN
    RAISE WARNING 'There are % missing cities in BL_3NF.', missing_count;
    total_missing := total_missing + missing_count;
ELSE
    RAISE NOTICE 'All cities are present in BL_3NF.';
END IF;

-- Check if all addresses are in BL_3NF (CE_ADDRESSES)
SELECT COUNT(*)
INTO missing_count
FROM (
    SELECT DISTINCT postalcode, 'SA_INDIVIDUAL_SALES' AS source_system, 'SRC_INDIVIDUAL_SALES' AS source_entity
    FROM sa_individual_sales.src_individual_sales
    UNION ALL
    SELECT DISTINCT postalcode, 'SA_COMPANY_SALES' AS source_system, 'SRC_COMPANY_SALES' AS source_entity
    FROM sa_company_sales.src_company_sales
) s
LEFT JOIN bl_3nf.ce_addresses a
ON s.postalcode = a.addresses_src_id
AND (a.source_system = s.source_system OR a.source_system = 'SA_INDIVIDUAL_SALES' OR a.source_system = 'SA_COMPANY_SALES')
AND (a.source_entity = s.source_entity OR a.source_entity = 'SRC_INDIVIDUAL_SALES' OR a.source_entity = 'SRC_COMPANY_SALES')
WHERE a.address_id IS NULL;

IF missing_count > 0 THEN
    RAISE WARNING 'There are % missing addresses in BL_3NF.', missing_count;
    total_missing := total_missing + missing_count;
ELSE
    RAISE NOTICE 'All addresses are present in BL_3NF.';
END IF;

-- Check if all products are in BL_3NF (CE_PRODUCTS_SCD)
SELECT COUNT(*)
INTO missing_count
FROM (
    SELECT DISTINCT productcode, 'SA_INDIVIDUAL_SALES' AS source_system, 'SRC_INDIVIDUAL_SALES' AS source_entity
    FROM sa_individual_sales.src_individual_sales
    UNION ALL
    SELECT DISTINCT productcode, 'SA_COMPANY_SALES' AS source_system, 'SRC_COMPANY_SALES' AS source_entity
    FROM sa_company_sales.src_company_sales
) s
LEFT JOIN bl_3nf.ce_products_scd p
ON s.productcode = p.products_src_id
AND (p.source_system = s.source_system OR p.source_system = 'SA_INDIVIDUAL_SALES' OR p.source_system = 'SA_COMPANY_SALES')
AND (p.source_entity = s.source_entity OR p.source_entity = 'SRC_INDIVIDUAL_SALES' OR p.source_entity = 'SRC_COMPANY_SALES')
WHERE p.prod_id IS NULL;

IF missing_count > 0 THEN
    RAISE WARNING 'There are % missing products in BL_3NF.', missing_count;
    total_missing := total_missing + missing_count;
ELSE
    RAISE NOTICE 'All products are present in BL_3NF.';
END IF;

-- Check if all product categories are in BL_3NF (CE_PRODUCT_CATEGORIES)
SELECT COUNT(*)
INTO missing_count
FROM (
    SELECT DISTINCT product_group_code, 'SA_INDIVIDUAL_SALES' AS source_system, 'SRC_INDIVIDUAL_SALES' AS source_entity
    FROM sa_individual_sales.src_individual_sales
    UNION ALL
    SELECT DISTINCT product_group_code, 'SA_COMPANY_SALES' AS source_system, 'SRC_COMPANY_SALES' AS source_entity
    FROM sa_company_sales.src_company_sales
) s
LEFT JOIN bl_3nf.ce_product_categories c
ON s.product_group_code = c.product_categories_src_id
AND (c.source_system = s.source_system OR c.source_system = 'SA_INDIVIDUAL_SALES' OR c.source_system = 'SA_COMPANY_SALES')
AND (c.source_entity = s.source_entity OR c.source_entity = 'SRC_INDIVIDUAL_SALES' OR c.source_entity = 'SRC_COMPANY_SALES')
WHERE c.prod_category_id IS NULL;

IF missing_count > 0 THEN
    RAISE WARNING 'There are % missing product categories in BL_3NF.', missing_count;
    total_missing := total_missing + missing_count;
ELSE
    RAISE NOTICE 'All product categories are present in BL_3NF.';
END IF;

-- Check if all orders are in BL_3NF (CE_ORDERS)
SELECT COUNT(*)
INTO missing_count
FROM (
    SELECT DISTINCT ordernumber, 'SA_INDIVIDUAL_SALES' AS source_system, 'SRC_INDIVIDUAL_SALES' AS source_entity
    FROM sa_individual_sales.src_individual_sales
    UNION ALL
    SELECT DISTINCT ordernumber, 'SA_COMPANY_SALES' AS source_system, 'SRC_COMPANY_SALES' AS source_entity
    FROM sa_company_sales.src_company_sales
) s
LEFT JOIN bl_3nf.ce_orders o
ON s.ordernumber = o.orders_src_id
AND (o.source_system = s.source_system OR o.source_system = 'SA_INDIVIDUAL_SALES' OR o.source_system = 'SA_COMPANY_SALES')
AND (o.source_entity = s.source_entity OR o.source_entity = 'SRC_INDIVIDUAL_SALES' OR o.source_entity = 'SRC_COMPANY_SALES')
WHERE o.order_id IS NULL;

IF missing_count > 0 THEN
    RAISE WARNING 'There are % missing orders in BL_3NF.', missing_count;
    total_missing := total_missing + missing_count;
ELSE
    RAISE NOTICE 'All orders are present in BL_3NF.';
END IF;

-- Check if all discounts are in BL_3NF (CE_DISCOUNTS)

SELECT COUNT(*)
INTO missing_count
FROM (
    SELECT DISTINCT discount_code, 'SA_INDIVIDUAL_SALES' AS source_system, 'SRC_INDIVIDUAL_SALES' AS source_entity
    FROM sa_individual_sales.src_individual_sales
    WHERE discount_code is not null
    UNION ALL
    SELECT DISTINCT discount_code, 'SA_COMPANY_SALES' AS source_system, 'SRC_COMPANY_SALES' AS source_entity
    FROM sa_company_sales.src_company_sales
    where discount_code is not null
) s
LEFT JOIN bl_3nf.ce_discounts d
ON s.discount_code = d.discounts_src_id
AND (d.source_system = s.source_system OR d.source_system = 'SA_INDIVIDUAL_SALES' OR d.source_system = 'SA_COMPANY_SALES')
AND (d.source_entity = s.source_entity OR d.source_entity = 'SRC_INDIVIDUAL_SALES' OR d.source_entity = 'SRC_COMPANY_SALES')
WHERE d.discount_id IS NULL;

IF missing_count > 0 THEN
    RAISE WARNING 'There are % missing discounts in BL_3NF.', missing_count;
    total_missing := total_missing + missing_count;
ELSE
    RAISE NOTICE 'All discounts are present in BL_3NF.';
END IF;


-- ### Final Status Update ###
IF total_missing = 0 THEN
    UPDATE bl_cl.data_quality_checks
    SET run_status = 'Completed', last_executed = NOW()
    WHERE check_id = 'FULL_DATA_TRANSFER';
    
    RAISE NOTICE 'All data transfer checks passed. Test status updated to COMPLETED.';
ELSE
    RAISE WARNING 'Data transfer checks failed with % discrepancies.', total_missing;
END IF;


END;
$$ LANGUAGE plpgsql;









-- To run every test group:
CALL bl_cl.run_all_quality_checks();


-- One by one:

CALL bl_cl.validate_no_duplicates();
CALL bl_cl.validate_data_transfer();




SELECT * FROM bl_cl.data_quality_checks dqc ;










-- check the partitions of fact table

SELECT child.relname AS partition_name
FROM pg_inherits AS pgin
JOIN pg_class AS parent ON parent.oid = pgin.inhparent
JOIN pg_class AS child ON child.oid = pgin.inhrelid
WHERE parent.relname = 'fct_orders'; 


-- check one of them
SELECT * FROM bl_dm.fct_orders_2023_01;
    







