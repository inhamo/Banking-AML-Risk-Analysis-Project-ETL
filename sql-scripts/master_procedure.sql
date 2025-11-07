/*************************************************************************************
Script		: master_procedure.sql
Purpose		: Master file to create and execute all procedures in the 
Author		: Innocent Nhamo
Created On	: 2025-10-29
Version		: 
			  v.1 - Initial creation 
**************************************************************************************/
USE banking_db;
GO

-- CREATE SCHEMAS 
-- Silver
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE schema_id = SCHEMA_ID('silver')) 
BEGIN 
	PRINT 'Creating a silver schema...';
	EXEC ('CREATE SCHEMA silver');
	PRINT 'Silver schema created in banking_db';
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE schema_id = SCHEMA_ID('gold'))
BEGIN
	PRINT 'Creating a gold schema....';
	EXEC ('CREATE SCHEMA gold');
	PRINT 'Gold schema created in banking_db';
END

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE schema_id = SCHEMA_ID('error_log'))
BEGIN
	PRINT 'Creating a error_log schema....';
	EXEC ('CREATE SCHEMA error_log');
	PRINT 'Error_log schema created in banking_db';
END

-- INDEX THE TABLES 

-- EXECUTE PROCEDURES