-- 7. PROCEDURE bl_cl.fct_orders_load   
-- The procedure begins by creating indexes on key columns for better performance 
-- during filtering and joining operations on `CE_ORDERS`, `DIM_ORDER_DETAILS`, and related tables.
-- It then partitions the `FCT_ORDERS` table into 3-month intervals based on `ORDER_DT` to manage data efficiently 
-- and ensure that historical data is handled in chunks for easier querying.
-- New active records are inserted into `FCT_ORDERS`, ensuring only relevant data from source tables is used.
-- Duplicate records are avoided by checking existing data in `FCT_ORDERS` before insertion.
-- Additionally, existing orders are updated if the product information has changed so that only active records are tracked.
-- The number of inserted and updated rows is captured for logging and auditing purposes.
-- If any error occurs during the process, it's caught, logged with details, and a warning is raised to notify the user.
-- Finally, the inserted and updated data is verified, and the results of the procedure are logged for review.


-- Index for BL_3NF.CE_ORDERS (essential for filtering and joining)
CREATE INDEX IF NOT EXISTS idx_ce_orders_order_dt ON BL_3NF.CE_ORDERS (ORDER_DT);

-- Index for BL_DM.DIM_ORDER_DETAILS (used for joining with CE_ORDERS)
CREATE INDEX IF NOT EXISTS idx_dim_order_details_orders_src_id ON BL_DM.DIM_ORDER_DETAILS (ORDERS_SRC_ID);

-- Index for BL_3NF.CE_PAYMENTS (used for joining)
CREATE INDEX IF NOT EXISTS idx_ce_payments_order_id ON BL_3NF.CE_PAYMENTS (ORDER_ID);

-- Index for BL_DM.DIM_PRODUCTS_SCD (used for joining and filtering)
CREATE INDEX IF NOT EXISTS idx_dim_products_scd_products_src_id ON BL_DM.DIM_PRODUCTS_SCD (PRODUCTS_SRC_ID);

-- Index for BL_DM.DIM_PRODUCTS_SCD (filter active products)
CREATE INDEX IF NOT EXISTS idx_dim_products_scd_is_active ON BL_DM.DIM_PRODUCTS_SCD (IS_ACTIVE);

-- Index for BL_DM.DIM_DATES (used for joining)
CREATE INDEX IF NOT EXISTS idx_dim_dates_order_dt ON BL_DM.DIM_DATES (ORDER_DT);




CREATE OR REPLACE PROCEDURE bl_cl.fct_orders_load()

AS $$ 
DECLARE
    start_date DATE;
    end_date DATE;
    partition_name TEXT;
    context TEXT;
    context_short TEXT;
    err_code TEXT;
    err_msg TEXT;
    err_context TEXT;
    err_context_short TEXT;
    err_detail TEXT;
    rows_inserted INT := 0;
    rows_updated INT := 0;
BEGIN
    -- Get the min and max ORDER_DT from the source data
    SELECT MIN(ORDER_DT), MAX(ORDER_DT)
    INTO start_date, end_date
    FROM BL_3NF.CE_ORDERS
    WHERE ORDER_DT != '1900-01-01';

    -- Loop through the date range and create partitions
    WHILE start_date < end_date LOOP
        partition_name := format('fct_orders_%s_b%s', 
                         to_char(start_date, 'YYYY'), 
                         LPAD(to_char(start_date, 'MM'), 3, '0'));

        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS BL_DM.%I PARTITION OF BL_DM.FCT_ORDERS 
            FOR VALUES FROM (%L) TO (%L);', 
            partition_name, start_date, start_date + INTERVAL '3 months');

        start_date := start_date + INTERVAL '3 months';
    END LOOP;


    -- Insert new active records
    INSERT INTO BL_DM.FCT_ORDERS (
        ORDER_SURR_ID,
        PROD_SURR_ID,
        CUST_SURR_ID,
        PAYMENT_SURR_ID,
        DISCOUNT_SURR_ID,
        ORDER_DT,
        QUANTITY,
        PRICE_EACH,
        COST_EACH,
        DISCOUNT_RATE,
        ORDERS_SRC_ID,
        source_system,
        source_entity,
        insert_dt,
        update_dt
    )
    SELECT 
        od.ORDER_SURR_ID,  
        dp.PROD_SURR_ID,   
        cds.CUST_SURR_ID,  
        pd.PAYMENT_SURR_ID, 
        dd.DISCOUNT_SURR_ID, 
        dt.ORDER_DT,       
        o.QUANTITY,      
        dp.PRICE_EACH,    
        dp.COST_EACH,     
        dd.discount_rate,  
        od.ORDERS_SRC_ID,  
        od.SOURCE_SYSTEM, 
        od.SOURCE_ENTITY,  
        NOW(),             
        NOW()               
    FROM BL_3NF.CE_ORDERS o
    JOIN BL_DM.DIM_ORDER_DETAILS od ON o.ORDERS_SRC_ID = od.ORDERS_SRC_ID
    JOIN BL_3NF.CE_PAYMENTS p ON p.ORDER_ID = o.ORDER_ID
    JOIN BL_DM.DIM_PAYMENTS pd ON p.PAYMENTS_SRC_ID = pd.PAYMENTS_SRC_ID
    JOIN BL_3NF.CE_CUSTOMERS c ON c.CUST_ID = p.CUST_ID
    JOIN BL_DM.DIM_CUSTOMERS cds ON c.CUSTOMERS_SRC_ID = cds.CUSTOMERS_SRC_ID
    JOIN BL_3NF.CE_DISCOUNTS d ON d.DISCOUNT_ID = p.DISCOUNT_ID
    JOIN BL_DM.DIM_DISCOUNTS dd ON d.DISCOUNTS_SRC_ID = dd.DISCOUNTS_SRC_ID
    JOIN BL_3NF.CE_PRODUCTS_SCD cps ON o.PROD_ID = cps.PROD_ID 
    JOIN BL_DM.DIM_PRODUCTS_SCD dp ON dp.PRODUCTS_SRC_ID = CPS.PRODUCTS_SRC_ID AND dp.IS_ACTIVE = 'Y'
    JOIN BL_DM.DIM_DATES dt ON o.ORDER_DT = dt.ORDER_DT 
    WHERE o.ORDER_DT != '1900-01-01' 
    AND dp.IS_ACTIVE = 'Y'
    AND NOT EXISTS (
        SELECT 1
        FROM BL_DM.FCT_ORDERS f
        WHERE f.ORDER_SURR_ID = od.ORDER_SURR_ID
        AND f.ORDERS_SRC_ID = od.ORDERS_SRC_ID
        AND f.PAYMENT_SURR_ID = pd.PAYMENT_SURR_ID
    );

    GET DIAGNOSTICS rows_inserted = ROW_COUNT;

 -- Update existing orders where the product is active and the PROD_SURR_ID needs to be updated
   UPDATE BL_DM.FCT_ORDERS f
   SET 
      PROD_SURR_ID = dp.PROD_SURR_ID,
      PRICE_EACH = dp.PRICE_EACH,
      COST_EACH = dp.COST_EACH,
      UPDATE_DT = NOW()
   FROM BL_3NF.CE_ORDERS o
   JOIN BL_3NF.CE_PRODUCTS_SCD SP ON SP.PROD_ID = O.PROD_ID
   JOIN BL_DM.DIM_PRODUCTS_SCD dp ON sp.PRODUCTS_SRC_ID = dp.PRODUCTS_SRC_ID
   WHERE f.ORDERS_SRC_ID = o.ORDERS_SRC_ID
   AND dp.IS_ACTIVE = 'Y'
   AND f.PROD_SURR_ID <> dp.PROD_SURR_ID;  -- Update only if PROD_SURR_ID differs

-- Get the number of rows updated
GET DIAGNOSTICS rows_updated = ROW_COUNT;

    GET DIAGNOSTICS 
        context := PG_CONTEXT;
    context_short := SUBSTRING(context FROM 'function (.*?) line');

    CALL BL_CL.LOAD_LOGGER(context_short, rows_inserted, rows_updated);

EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        err_context := PG_EXCEPTION_CONTEXT,
        err_detail := PG_EXCEPTION_DETAIL;
    err_context_short := SUBSTRING(err_context FROM 'function (.*?) line');

    -- Logging the error
    CALL BL_CL.LOAD_LOGGER(err_context_short, 0, 0, FORMAT('ERROR %s: %s. Details: %s', SQLSTATE, SQLERRM, err_detail));
    RAISE WARNING 'STATE: %, ERRM: %', SQLSTATE, SQLERRM;
END;
$$ LANGUAGE plpgsql;



-- Execute the procedure
CALL bl_cl.fct_orders_load();

-- Verify the inserted data
SELECT * FROM BL_DM.FCT_ORDERS
ORDER BY order_surr_id ;

-- Log the procedure execution
SELECT * FROM bl_cl.mta_load_logs mll;

