USE lakeflow;
GO

/*
 * Check CDC configuration
 */
-- expected: is_cdc_enabled = true
SELECT name, is_cdc_enabled
FROM sys.databases
WHERE name = 'lakeflow';

-- expected: is_tracked_by_cdc = true
SELECT name, is_tracked_by_cdc
FROM sys.tables
WHERE name IN ('orders', 'customers');

-- expected: change table exists
SELECT *
FROM cdc.change_tables
WHERE source_object_id IN (OBJECT_ID('dbo.orders'), OBJECT_ID('dbo.customers'));


-- is SQL Server Agent running?
SELECT servicename, status_desc
FROM sys.dm_server_services;

-- does sql server write CDC events? (only if CDC enabled)
SELECT *
FROM cdc.dbo_orders_CT
ORDER BY __$start_lsn DESC;

SELECT *
FROM cdc.dbo_customers_CT
ORDER BY __$start_lsn DESC;

/*
 Check CT configuration
 */
-- expected: 1 row
SELECT *
FROM sys.change_tracking_databases
WHERE database_id = DB_ID('lakeflow');

-- expected: 1 row each
SELECT *
FROM sys.change_tracking_tables
WHERE object_id = OBJECT_ID('dbo.orders');

SELECT *
FROM sys.change_tracking_tables
WHERE object_id = OBJECT_ID('dbo.customers');
