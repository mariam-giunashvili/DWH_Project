--creates the `bl_cl` schema if it does not exist
CREATE SCHEMA IF NOT EXISTS bl_cl;


-- Step 1: Grant ALL PRIVILEGES on everything in the `bl_cl` schema to the postgres user
GRANT USAGE ON SCHEMA bl_cl TO postgres;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA bl_cl TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA bl_cl TO postgres;
GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA bl_cl TO postgres;
GRANT ALL PRIVILEGES ON ALL PROCEDURES IN SCHEMA bl_cl TO postgres;
GRANT CREATE ON SCHEMA bl_cl TO postgres;


-- Step 5: Set the role for the current session 
SET ROLE postgres;






-- Log Table DDL
-- The `mta_load_logs` table is designed to log metadata for DML operations across the Data Warehouse.
-- The table includes the following columns:
--    - `id`: An auto-incremented identifier for each log entry.
--    - `proc_name`: The name of the procedure calling the log.
--    - `rows_inserted`: The number of rows inserted (default is 0).
--    - `rows_updated`: The number of rows updated (default is 0).
--    - `status`: The operation's status (default is 'SUCCESS').
--    - `time`: Timestamp for when the log entry was created (defaults to the current time).


--DROP TABLE IF EXISTS bl_cl.mta_load_logs;

DROP TABLE IF EXISTS bl_cl.mta_load_logs;


CREATE TABLE IF NOT EXISTS bl_cl.mta_load_logs ( 
	id 					INT 				GENERATED ALWAYS AS IDENTITY,
	proc_name 			VARCHAR(500)		NOT NULL,
	rows_inserted 		INT 				NOT NULL DEFAULT 0,
	rows_updated 		INT 				NOT NULL DEFAULT 0,
	status				TEXT 				NOT NULL DEFAULT 'SUCCESS',
	time 				TIMESTAMPTZ 		DEFAULT NOW()
);





-- Logging procedure
-- This procedure accepts four parameters:
--    - `pg_context`: A string representing the procedure name or context of the DML operation.
--    - `rows_inserted`: An integer representing the number of inserted rows (default is 0).
--    - `rows_updated`: An integer representing the number of updated rows (default is 0).
--    - `op_status`: A status message (defaults to 'SUCCESS').
-- The `load_logger` procedure is intended to be called by other DML scripts to log changes across the DWH.

CREATE OR REPLACE PROCEDURE bl_cl.load_logger (
	pg_context		VARCHAR,
	rows_inserted	INT DEFAULT 0,
	rows_updated	INT DEFAULT 0,
	op_status		TEXT DEFAULT 'SUCCESS'
)
AS $load_logger$
	INSERT INTO bl_cl.mta_load_logs (proc_name, rows_inserted, rows_updated, status)
	SELECT pg_context, rows_inserted, rows_updated, op_status;
$load_logger$ LANGUAGE SQL;

-- Procedure info
SELECT * FROM pg_proc WHERE proname ~~ '%logger';
