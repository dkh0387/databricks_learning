USE
    lakeflow;
GO


CREATE LOGIN lakeflow_repl WITH PASSWORD = 'Str0ng!Passw0rd';
CREATE USER lakeflow_repl FOR LOGIN lakeflow_repl;
EXEC sp_addrolemember 'db_owner', 'lakeflow_repl';
GO

