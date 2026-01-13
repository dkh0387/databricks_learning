USE
    lakeflow;
GO

-- Activate Change Tracking for the database
IF NOT EXISTS (SELECT 1
               FROM sys.change_tracking_databases
               WHERE database_id = DB_ID('lakeflow'))
    BEGIN
        ALTER DATABASE lakeflow
            SET CHANGE_TRACKING = ON
            (CHANGE_RETENTION = 7 DAYS, AUTO_CLEANUP = ON);
    END
GO

-- Activate Change Tracking for the orders table
IF NOT EXISTS (SELECT 1
               FROM sys.change_tracking_tables
               WHERE object_id = OBJECT_ID('dbo.orders'))
    BEGIN
        ALTER TABLE dbo.orders
            ENABLE CHANGE_TRACKING
                WITH (TRACK_COLUMNS_UPDATED = ON);
    END
GO

-- Activate Change Data Capture for the database
IF NOT EXISTS (SELECT 1
               FROM sys.databases
               WHERE name = 'lakeflow'
                 AND is_cdc_enabled = 1)
    BEGIN
        EXEC sys.sp_cdc_enable_db;
    END
GO

-- Activate Change Data Capture for the orders table
IF NOT EXISTS (SELECT 1
               FROM cdc.change_tables
               WHERE source_object_id = OBJECT_ID('dbo.orders'))
    BEGIN
        EXEC sys.sp_cdc_enable_table
             @source_schema = N'dbo',
             @source_name = N'orders',
             @role_name = N'cdc_subscriber',
             @capture_instance = N'dbo_orders',
             @supports_net_changes = 1;
    END
GO