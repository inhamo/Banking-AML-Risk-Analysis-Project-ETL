/*************************************************************************************
Script		: clean_accounts_silver_to_staging.sql
Purpose		: ETL procedure to load bronze messy data into a unionized stg_customers
			  standardize text, solve for null values and duplicates 
Author		: Innocent Nhamo
Created On	: 2025-10-29
Version		: 
			  v.1 - Initial creation 
**************************************************************************************/

USE banking_db; 
GO 

CREATE OR ALTER PROCEDURE pcd_clean_transactions_silver_to_staging
AS 
BEGIN
	SET NOCOUNT ON;

	/***********************************************
		Declare variables
	***********************************************/
	DECLARE
		@StartTime DATETIME = SYSDATETIME(), 
		@RowsInserted INT = 0, 
		@ErrorMessage NVARCHAR(4000), 
		@BatchID UNIQUEIDENTIFIER = NEWID();

	BEGIN TRY
        /********************************************
			Begin logging for audit trail
        *********************************************/

		-- Create the error log table if it does not exist 

		IF OBJECT_ID('error_log.etl_log', 'U') IS NULL 
		BEGIN 
			CREATE TABLE error_log.etl_log (
				log_id INT IDENTITY(1,1) PRIMARY KEY,
				batch_id UNIQUEIDENTIFIER NOT NULL,
				procedure_name VARCHAR(200) NOT NULL,
				start_time DATETIME NOT NULL,
				end_time DATETIME NULL,
				status VARCHAR(50) NOT NULL,
				rows_processed INT NOT NULL DEFAULT 0,
				error_message NVARCHAR(MAX) NULL,
				created_at DATETIME DEFAULT GETDATE()
			);
		END;

		INSERT INTO error_log.etl_log(batch_id, procedure_name, start_time, status)
		VALUES (@BatchID, 'pcd_clean_transactions_to_staging', @StartTime, 'STARTED')

        /********************************************
			Data cleaning pipeline
        *********************************************/

		-- Drop temp table if it already exists
		IF OBJECT_ID('tempdb..#stg_transctions') IS NOT NULL
			DROP TABLE #stg_transactions

		-- Union tables and insert into a temporary table
		PRINT 'Union all tables...';
		SELECT * 
		INTO #trans_temp 
		FROM (
			SELECT * FROM bronze.erp_transactions_2018
			UNION ALL 
			SELECT * FROM bronze.erp_transactions_2019
			UNION ALL 
			SELECT * FROM bronze.erp_transactions_2020
			UNION ALL 
			SELECT * FROM bronze.erp_transactions_2021
			UNION ALL 
			SELECT * FROM bronze.erp_transactions_2022
			UNION ALL 
			SELECT * FROM bronze.erp_transactions_2023
			UNION ALL 
			SELECT * FROM bronze.erp_transactions_2024
		) AS all_transactons; 
		PRINT 'Union all tables successful';

		WITH CTE_fix_receiving_account AS (
			SELECT 
				tt.*, 
				CASE 
					-- If receiving_account starts with 'AC', try to map to account_number
					WHEN receiving_account LIKE 'AC%' THEN 
						COALESCE(fa.account_number, tt.receiving_account)
					-- For CREDIT transactions, if receiving_account is NULL, it should be external
					WHEN UPPER(TRIM(tt.debit_credit)) = 'CREDIT' AND tt.receiving_account IS NULL 
						THEN 'EXTERNAL_ACCOUNT'  -- or leave as NULL if appropriate
					-- For DEBIT transactions with NULL receiving_account, use the source account
					WHEN UPPER(TRIM(tt.debit_credit)) = 'DEBIT' AND tt.receiving_account IS NULL 
						THEN ffa.account_number
					-- For eWallet transactions, leave empty
					WHEN UPPER(TRIM(tt.channel)) = 'EWALLET' THEN ''
					-- Otherwise, use the receiving_account as is
					ELSE tt.receiving_account
				END AS receiving_account_number
			FROM #trans_temp tt
			LEFT JOIN silver.fact_accounts fa
				ON fa.account_id = tt.receiving_account 
			LEFT JOIN silver.fact_accounts ffa 
				ON ffa.account_id = tt.account_id
		)
		SELECT
			UPPER(TRIM(transaction_id)) AS transaction_id, 
			UPPER(TRIM(account_id)) AS account_id, 
			CAST(transaction_date AS DATE) AS transaction_date , 
			CAST(transaction_time AS TIME) AS transaction_time, 
			ABS(amount) AS transaction_amount, 
			transaction_fee,
			fx_fee, 
			swift_fee, 
			correspondent_fee, 
			UPPER(TRIM(debit_credit)) AS transaction_type, 
			UPPER(TRIM(category)) AS transaction_category, 
			UPPER(TRIM(status)) AS transaction_status, 
			UPPER(TRIM(description)) AS transaction_description, 
			UPPER(TRIM(channel)) AS transaction_channel, 
			UPPER(TRIM(merchant_name)) AS merchant_name, 
			UPPER(TRIM(merchant_category)) AS merchant_category, 
			receiving_account_number AS receiving_account_number, 
			CASE 
				WHEN UPPER(TRIM(receiving_bank)) LIKE 'SAME BANK' THEN '' 
				WHEN UPPER(TRIM(channel)) = 'EWALLET' THEN '' 
				ELSE receiving_bank
			END AS receiving_bank,
			CASE WHEN UPPER(TRIM(is_international)) = 'YES' THEN 1 ELSE 0 END AS is_international_transaction, 
			CASE WHEN UPPER(TRIM(instant_payment)) = 'YES' THEN 1 ELSE 0 END AS is_instant_payment, 
			UPPER(TRIM(batch_reference)) AS batch_reference, 
			recipient_phone, 
			UPPER(TRIM(CAST(atm_location AS NVARCHAR(MAX)))) AS atm_location, 
			UPPER(TRIM(CAST(branch_location AS NVARCHAR(MAX)))) AS branch_location, 
			CASE WHEN UPPER(TRIM(is_reversal)) = 'YES' THEN 1 ELSE 0 END AS is_reversal, 
			ABS(cashback_amount) AS cashback_amount, 
			loyalty_points, 
			_source_file_url, 
			_ingestion_timestamp, 
			_source_hash
		INTO #stg_transactions
		FROM CTE_fix_receiving_account
		WHERE amount != 0;

		-- Get the number of rows inserted
		SET @RowsInserted = @@ROWCOUNT;

        /********************************************
			Create normalized tables
        *********************************************/
		
		-- Check if stored procedure exists and execute it
		IF EXISTS (SELECT 1 FROM sys.procedures WHERE name = 'pcd_create_transactions_normalized_tables' AND schema_id = SCHEMA_ID('silver'))
		BEGIN
			EXEC silver.pcd_create_transactions_normalized_tables;
			PRINT 'Stored procedure executed successfully';
		END
		ELSE
		BEGIN
			PRINT 'Stored procedure silver.pcd_create_transactions_normalized_tables does not exist';
		END


        /********************************************
			Insert into the normalized tables
        *********************************************/
		
		-- Check if stored procedure exists and execute it
		IF EXISTS (SELECT 1 FROM sys.procedures WHERE name = 'pcd_insert_into_accounts_normalized_tables' AND schema_id = SCHEMA_ID('silver'))
		BEGIN
			EXEC silver.pcd_insert_into_accounts_normalized_tables;
			PRINT 'Stored procedure executed successfully';
		END
		ELSE
		BEGIN
			PRINT 'Stored procedure silver.pcd_insert_into_accounts_normalized_tables does not exist';
		END

	END TRY 
	BEGIN CATCH 
        /********************************************
			Handle and log errors
        *********************************************/

		SET @ErrorMessage = ERROR_MESSAGE();
		UPDATE error_log.etl_log
		SET end_time = SYSDATETIME(),
			status = 'FAILED', 
			error_message = @ErrorMessage
		WHERE batch_id = @BatchID;

		RAISERROR('ETL failed in pcd_clean_transactions_to_staging: %s', 16, 1, @ErrorMessage);

	END CATCH
END; 
GO

EXEC pcd_clean_transactions_silver_to_staging;