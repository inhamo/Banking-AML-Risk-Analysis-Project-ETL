/*************************************************************************************
Script		: pcd_create_transactions_normalized_tables.sql
Purpose		: ETL procedure to create tables for customers tables 
Author		: Innocent Nhamo
Created On	: 2025-11-07
Version		: 
			  v.1 - Initial creation 
**************************************************************************************/
USE banking_db;
GO

-- Procedure 
CREATE OR ALTER PROCEDURE silver.pcd_create_transactions_normalized_tables
AS
BEGIN
	SET NOCOUNT ON;

	/***********************************************
		Drop tables if they exist
	***********************************************/
	PRINT 'Dropping existing tables...';
	
	-- DROP all FK constraints in silver schema first
	DECLARE @sql NVARCHAR(MAX) = '';
	
	SELECT @sql = @sql + 'ALTER TABLE ' + QUOTENAME(OBJECT_SCHEMA_NAME(parent_object_id)) + '.' + 
				  QUOTENAME(OBJECT_NAME(parent_object_id)) + 
				  ' DROP CONSTRAINT ' + QUOTENAME(name) + ';' + CHAR(13)
	FROM sys.foreign_keys
	WHERE OBJECT_SCHEMA_NAME(parent_object_id) = 'silver';
	
	IF @sql <> ''
	BEGIN
		PRINT 'Dropping foreign key constraints...';
		EXEC sp_executesql @sql;
		PRINT 'Foreign key constraints dropped successfully';
	END;

	IF OBJECT_ID('silver.fact_transactions', 'U') IS NOT NULL 
	BEGIN 
		DROP TABLE silver.fact_transactions;
		PRINT 'Dropped the silver.fact_transactions';
	END;

	PRINT 'Table dropping completed';

	/***********************************************
		Create tables
	***********************************************/
	-- FACT TRANSACTIONS 
	PRINT 'Creating fact transaction table...';
	CREATE TABLE silver.fact_transactions (
		transaction_key INT IDENTITY(1, 1) PRIMARY KEY NOT NULL,
		transaction_id NVARCHAR(50) NOT NULL,
		account_id NVARCHAR(50) NOT NULL,	
		transaction_date DATE,	
		transaction_time TIME ,	
		transaction_amount DECIMAL(15, 2),	
		transaction_fee	DECIMAL(5, 2),
		fx_fee DECIMAL(5, 2),	
		swift_fee DECIMAL(5, 2),
		correspondent_fee DECIMAL(5, 2),	
		transaction_type NVARCHAR(50) NOT NULL,	
		transaction_category NVARCHAR(50) ,	
		transaction_status NVARCHAR(50),	
		transaction_description NVARCHAR(200),	
		transaction_channel NVARCHAR(50),	
		merchant_name NVARCHAR(50) ,	
		merchant_category NVARCHAR(50), 
		receiving_account_number NVARCHAR(50),	
		receiving_bank NVARCHAR(50),	
		is_international_transaction BIT,	
		is_instant_payment BIT,	
		batch_reference NVARCHAR(50),	
		recipient_phone NVARCHAR(50),	
		atm_location NVARCHAR(MAX),	
		branch_location NVARCHAR(MAX),	
		is_reversal BIT,	
		cashback_amount DECIMAL(8, 2),	
		loyalty_points INT, 

		created_date DATETIME2 DEFAULT GETDATE(), 
		update_date DATETIME2 DEFAULT GETDATE(), 

		-- Constraints prevent duplicate 
		CONSTRAINT UQ_fact_transactions_transaction_id UNIQUE (transaction_id)
	);
	PRINT 'Fact transactions table created successfully';	

	/***********************************************
		Create table indexes
	***********************************************/
	PRINT 'Creating indexes.....';

	-- fact transactions table 
	CREATE INDEX IDX_fact_transactions_transaction_id ON silver.fact_transactions(transaction_id)
	CREATE INDEX IDX_fact_transactions_account_id ON silver.fact_transactions(account_id)
	CREATE INDEX IDX_fact_transactions_transaction_date ON silver.fact_transactions(transaction_date)

	PRINT 'Indexes created';
END; 
GO

