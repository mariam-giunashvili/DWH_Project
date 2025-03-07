 -- This script creates the BL_DM schema and its tables, sequences, and default records.  
-- It supports an optional schema drop before creating objects.  
-- The procedure BL_DM_DDL_LOAD takes a BOOLEAN parameter with_drop.  
-- If with_drop is TRUE, the existing BL_DM schema is dropped and recreated.  
-- The script creates required tables and sequences inside the schema.  
-- Constraints and primary keys are set to ensure data integrity.  
-- Usage example: CALL BL_DM_DDL_LOAD(TRUE) to drop and recreate schema.  
-- Usage example: CALL BL_DM_DDL_LOAD(FALSE) to create objects without dropping.  

-- Create or replace the procedure for BL_DM DDL
CREATE OR REPLACE PROCEDURE BL_CL.BL_DM_DDL_LOAD(IN with_drop BOOLEAN)
LANGUAGE plpgsql
AS
$$
BEGIN
    -- drop the schema and recreate it
    IF with_drop IS TRUE THEN
        DROP SCHEMA IF EXISTS BL_DM CASCADE;
        CREATE SCHEMA BL_DM;
    END IF;

    -- Set search path
    SET SEARCH_PATH TO BL_DM;
    

   
-- Ensure the table doesn't exist
CREATE TABLE IF NOT EXISTS BL_DM.dim_dates (
    order_dt DATE PRIMARY KEY NOT NULL, -- surrogate key not needed here, because each date is unique
    day_in_week VARCHAR(20) NOT NULL,
    day_number_in_month INT NOT NULL,
    month_number INT NOT NULL,
    month_name VARCHAR(15) NOT NULL,
    quarter_number INT NOT NULL,
    year INT NOT NULL
);


-- Create sequence for customer surrogate key
CREATE SEQUENCE IF NOT EXISTS BL_DM.SEQ_DIM_CUSTOMERS;

CREATE TABLE IF NOT EXISTS BL_DM.DIM_CUSTOMERS (
    cust_surr_id INT NOT NULL PRIMARY KEY,
    cust_first_name VARCHAR(50) NOT NULL,
    cust_last_name VARCHAR(50) NOT NULL,
    cust_company_name VARCHAR(50) NOT NULL,
    cust_type VARCHAR(20) NOT NULL,
    address_id INT NOT NULL,
    address VARCHAR(100) NOT NULL,
    postal_code VARCHAR(50) NOT NULL,
    city_id INT NOT NULL,
    city VARCHAR(50) NOT NULL,
    country_id INT NOT NULL,
    country VARCHAR(50) NOT NULL,
    insert_dt TIMESTAMP NOT NULL,
    update_dt TIMESTAMP NOT NULL,
    customers_src_id VARCHAR(50) NOT NULL,
    source_system VARCHAR(50) NOT NULL,
    source_entity VARCHAR(50) NOT NULL
);

-- Alter sequence for customer surrogate key
ALTER SEQUENCE BL_DM.SEQ_DIM_CUSTOMERS OWNED BY BL_DM.DIM_CUSTOMERS.cust_surr_id;



-- Creates a sequence to generate surrogate keys for DIM_PRODUCTS_SCD
CREATE SEQUENCE IF NOT EXISTS BL_DM.SEQ_DIM_PRODUCTS_SCD;  

-- Creates the DIM_PRODUCTS_SCD table to store product details
CREATE TABLE IF NOT EXISTS BL_DM.DIM_PRODUCTS_SCD (
    prod_surr_id INT NOT NULL PRIMARY KEY,
    prod_category_id INT NOT NULL,
    prod_category VARCHAR(50) NOT NULL,
    price_each DECIMAL(10, 2) NOT NULL,
    cost_each DECIMAL(10, 2) NOT NULL,
    insert_dt TIMESTAMP NOT NULL,
    update_dt TIMESTAMP NOT NULL,
    is_active char(1) NOT NULL,
    end_dt TIMESTAMP NOT NULL,
    products_src_id VARCHAR(50) NOT NULL,
    source_system VARCHAR(50) NOT NULL,
    source_entity VARCHAR(50) NOT NULL
);

-- Associates the sequence with the surrogate key of the table
ALTER SEQUENCE BL_DM.SEQ_DIM_PRODUCTS_SCD OWNED BY BL_DM.DIM_PRODUCTS_SCD.prod_surr_id;



-- Creates a sequence to generate surrogate keys for DIM_ORDER_DETAILS
CREATE SEQUENCE IF NOT EXISTS BL_DM.SEQ_DIM_ORDER_DETAILS;

-- Creates the DIM_ORDER_DETAILS table to store order details
CREATE TABLE IF NOT EXISTS BL_DM.DIM_ORDER_DETAILS (
    order_surr_id INT NOT NULL PRIMARY KEY,
    order_status VARCHAR(20) NOT NULL,
    insert_dt TIMESTAMP NOT NULL,
    update_dt TIMESTAMP NOT NULL,
    orders_src_id VARCHAR(50) NOT NULL,
    source_system VARCHAR(50) NOT NULL,
    source_entity VARCHAR(50) NOT NULL
);

-- Associates the sequence with the surrogate key of the table
ALTER SEQUENCE BL_DM.SEQ_DIM_ORDER_DETAILS OWNED BY BL_DM.DIM_ORDER_DETAILS.order_surr_id;



-- Creates a sequence to generate surrogate keys for DIM_DISCOUNTS
CREATE SEQUENCE IF NOT EXISTS BL_DM.SEQ_DIM_DISCOUNTS;

-- Creates the DIM_DISCOUNTS table to store discount details
CREATE TABLE IF NOT EXISTS BL_DM.DIM_DISCOUNTS (
    discount_surr_id INT NOT NULL PRIMARY KEY,
    discount_rate INT NOT NULL,
    discount_code  varchar(50) NOT NULL,
    insert_dt TIMESTAMP NOT NULL,
    update_dt TIMESTAMP NOT NULL,
    discounts_src_id VARCHAR(50) NOT NULL,
    source_system VARCHAR(50) NOT NULL,
    source_entity VARCHAR(50) NOT NULL
);

-- Associates the sequence with the surrogate key of the table
ALTER SEQUENCE BL_DM.SEQ_DIM_DISCOUNTS OWNED BY BL_DM.DIM_DISCOUNTS.discount_surr_id;




-- Creates a sequence to generate surrogate keys for DIM_PAYMENTS
CREATE SEQUENCE IF NOT EXISTS BL_DM.SEQ_DIM_PAYMENTS;

-- Creates the DIM_PAYMENTS table
CREATE TABLE IF NOT EXISTS BL_DM.DIM_PAYMENTS (
    payment_surr_id INT NOT NULL PRIMARY KEY,
    payment_type VARCHAR(20) NOT NULL,
    amount DECIMAL(10, 2) NOT NULL,
    insert_dt TIMESTAMP NOT NULL,
    update_dt TIMESTAMP NOT NULL,
    payments_src_id VARCHAR(50) NOT NULL,
    source_system VARCHAR(50) NOT NULL,
    source_entity VARCHAR(50) NOT NULL
);

-- Associates the sequence with the surrogate key of the table
ALTER SEQUENCE BL_DM.SEQ_DIM_PAYMENTS OWNED BY BL_DM.DIM_PAYMENTS.payment_surr_id;




-- fct_orders
CREATE TABLE IF NOT EXISTS BL_DM.FCT_ORDERS (
    ORDER_SURR_ID INT NOT NULL,
    PROD_SURR_ID INT NOT NULL,
    CUST_SURR_ID INT NOT NULL,
    PAYMENT_SURR_ID INT NOT NULL,
    DISCOUNT_SURR_ID INT NOT NULL,
    ORDER_DT DATE NOT NULL,  -- Partition Key
    QUANTITY INT NOT NULL,
    PRICE_EACH DECIMAL(10, 2) NOT NULL,
    GROSS_AMOUNT DECIMAL(10, 2) GENERATED ALWAYS AS (PRICE_EACH * QUANTITY) STORED,
    DISCOUNT_RATE INT NOT NULL,
    NET_AMOUNT DECIMAL(10, 2) GENERATED ALWAYS AS ((PRICE_EACH * QUANTITY) - ((PRICE_EACH * QUANTITY) * DISCOUNT_RATE / 100)) STORED,
    COST_EACH DECIMAL(10, 2) NOT NULL,
    ORDERS_SRC_ID VARCHAR(50) NOT NULL,
    source_system VARCHAR(50) NOT NULL,
    source_entity VARCHAR(50) NOT NULL,
    insert_dt TIMESTAMP NOT NULL,
    update_dt TIMESTAMP NOT NULL,
    PRIMARY KEY (ORDER_SURR_ID, ORDER_DT),  -- ORDER_DT added for partitioning
    FOREIGN KEY (ORDER_SURR_ID) REFERENCES BL_DM.DIM_ORDER_DETAILS(ORDER_SURR_ID),
    FOREIGN KEY (CUST_SURR_ID) REFERENCES BL_DM.DIM_CUSTOMERS(CUST_SURR_ID),
    FOREIGN KEY (PROD_SURR_ID) REFERENCES BL_DM.DIM_PRODUCTS_SCD(PROD_SURR_ID),
    FOREIGN KEY (PAYMENT_SURR_ID) REFERENCES BL_DM.DIM_PAYMENTS(PAYMENT_SURR_ID),
    FOREIGN KEY (DISCOUNT_SURR_ID) REFERENCES BL_DM.DIM_DISCOUNTS(DISCOUNT_SURR_ID),
    FOREIGN KEY (ORDER_DT) REFERENCES BL_DM.DIM_DATES(ORDER_DT)
) PARTITION BY RANGE (ORDER_DT);



--  Add default row for DIM_DATES
INSERT INTO BL_DM.dim_dates (order_dt, day_in_week, day_number_in_month, month_number, month_name, quarter_number, year)
SELECT '1900-01-01'::DATE, 'n.a.', -1, -1, 'n.a.', -1, -1
WHERE NOT EXISTS (SELECT 1 FROM BL_DM.dim_dates WHERE order_dt = '1900-01-01');


-- Add default row for DIM_CUSTOMERS
INSERT INTO BL_DM.DIM_CUSTOMERS (cust_surr_id, cust_first_name, cust_last_name, cust_company_name, cust_type, address_id, 
address, postal_code, city_id, city, country_id, country, insert_dt, update_dt, customers_src_id, source_system, source_entity)
SELECT -1, 'n.a.', 'n.a.', 'n.a.', 'n.a.', -1, 'n.a.', 'n.a', -1, 'n.a.', -1, 'n.a.', '1900-01-01'::DATE, '1900-01-01'::DATE, 'n.a.', 'MANUAL', 'MANUAL'
WHERE NOT EXISTS (SELECT 1 FROM BL_DM.DIM_CUSTOMERS WHERE cust_surr_id = -1);

-- Add default row for DIM_PRODUCTS_SCD
INSERT INTO BL_DM.DIM_PRODUCTS_SCD (prod_surr_id, prod_category_id, prod_category, price_each, cost_each, insert_dt, update_dt, is_active, end_dt,  products_src_id, source_system, source_entity)
SELECT -1, -1, 'n.a.', 0.00, 0.00, '1900-01-01'::DATE, '1900-01-01'::DATE, 'Y', '9999-12-31'::DATE, 'n.a.', 'MANUAL', 'MANUAL'
WHERE NOT EXISTS (SELECT 1 FROM BL_DM.DIM_PRODUCTS_SCD WHERE prod_surr_id = -1);

-- Add default row for DIM_ORDER_DETAILS
INSERT INTO BL_DM.DIM_ORDER_DETAILS (order_surr_id, order_status, insert_dt, update_dt, orders_src_id, source_system, source_entity) 
SELECT -1, 'n.a.', '1900-01-01'::DATE, '1900-01-01'::DATE, 'n.a.', 'MANUAL', 'MANUAL'
WHERE NOT EXISTS (SELECT 1 FROM BL_DM.DIM_ORDER_DETAILS WHERE order_surr_id = -1);

-- Add default row for DIM_DISCOUNTS
INSERT INTO BL_DM.DIM_DISCOUNTS (discount_surr_id, discount_rate, discount_code, insert_dt, update_dt, discounts_src_id, source_system, source_entity) 
SELECT -1, 0, 'n.a', '1900-01-01'::DATE, '1900-01-01'::DATE, 'n.a.', 'MANUAL', 'MANUAL'
WHERE NOT EXISTS (SELECT 1 FROM BL_DM.DIM_DISCOUNTS WHERE discount_surr_id = -1);

-- Add default row for DIM_PAYMENTS
INSERT INTO BL_DM.DIM_PAYMENTS (payment_surr_id, payment_type, amount, insert_dt, update_dt, payments_src_id, source_system, source_entity) 
SELECT -1, 'n.a.', 0.00, '1900-01-01'::DATE, '1900-01-01'::DATE, 'n.a.', 'MANUAL', 'MANUAL'
WHERE NOT EXISTS (SELECT 1 FROM BL_DM.DIM_PAYMENTS WHERE payment_surr_id = -1);




END;
$$;





-- If needed to drop and recreate the schema:
CALL bl_cl.BL_DM_DDL_LOAD(TRUE);







