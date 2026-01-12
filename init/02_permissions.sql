-- Postgres
/*ALTER USER lakeflow WITH REPLICATION;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO lakeflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT SELECT ON TABLES TO lakeflow;*/

-- SQL Server

-- Enable Change Data Capture
USE lakeflow;
GO

EXEC sys.sp_cdc_enable_db;
GO

-- Enable Change Data Capture for the order table
EXEC sys.sp_cdc_enable_table
     @source_schema = N'dbo',
     @source_name = N'orders',
     @role_name = NULL;
GO
