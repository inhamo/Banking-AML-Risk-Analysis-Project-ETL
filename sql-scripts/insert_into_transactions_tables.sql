/*************************************************************************************
Script		: insert_into_transactions_tables.sql
Purpose		: ETL procedure to load data into normalized accounts tables 
Author		: Innocent Nhamo
Created On	: 2025-13-07
Version		: 
			  v.1 - Initial creation 
**************************************************************************************/
USE banking_db;
GO

CREATE OR ALTER PROCEDURE silver.pcd_insert_into_accounts_normalized_tables
AS 
BEGIN 
	SET NOCOUNT ON;

	DECLARE @RowCount INT;
	DECLARE @StartTime DATETIME2 = SYSDATETIME();

	BEGIN TRY
		BEGIN TRANSACTION; 
		/***********************************************
			Create indexes on temp table for performance
		***********************************************/
		PRINT 'Creating indexes on staging table...';
		
		CREATE INDEX IDX_fact_transactions_account_id ON #stg_transactions(account_id);
		CREATE INDEX IDX_fact_transactions_transaction_id ON #stg_transactions(account_id);
		CREATE INDEX IDX_fact_transactions_transaction_date ON #stg_transactions(transaction_date);

		PRINT 'Indexes created successfully';

		/***********************************************
			Insert into the fact table
		***********************************************/
		RAISERROR('Loading data into the fact transactions table.....', 0, 1) WITH NOWAIT;
		INSERT INTO silver.fact_transactions (
			transaction_id, account_id, transaction_date, transaction_time,
			transaction_amount, transaction_fee, fx_fee, swift_fee, correspondent_fee,
			transaction_type, transaction_category, transaction_status, transaction_description,
			transaction_channel, merchant_name, merchant_category, receiving_account_number,
			receiving_bank, is_international_transaction, is_instant_payment, batch_reference,
			recipient_phone, atm_location, branch_location, is_reversal, cashback_amount,
			loyalty_points
		)
		SELECT DISTINCT 
			transaction_id, 
			account_id, 
			transaction_date, 
			transaction_time,
			transaction_amount, 
			transaction_fee, 
			fx_fee, 
			swift_fee, 
			correspondent_fee,
			transaction_type, 
			transaction_category, 
			transaction_status, 
			transaction_description,
			transaction_channel, 
			merchant_name, 
			merchant_category, 
			receiving_account_number,
			receiving_bank, 
			is_international_transaction, 
			is_instant_payment, 
			batch_reference,
			recipient_phone, 
			atm_location, 
			branch_location, 
			is_reversal, 
			cashback_amount,
			loyalty_points	
		FROM #stg_transactions 

		SET @RowCount = @@ROWCOUNT;
		RAISERROR('Loaded %d accounts into fact table successfully', 0, 1, @RowCount) WITH NOWAIT;
		COMMIT TRANSACTION;
	
		DECLARE @Duration INT = DATEDIFF(SECOND, @StartTime, SYSDATETIME());
		RAISERROR('All data loaded successfully in %d seconds', 0, 1, @Duration) WITH NOWAIT;

	END TRY
	BEGIN CATCH
		IF @@TRANCOUNT > 0
			ROLLBACK TRANSACTION;
			
		DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
		DECLARE @ErrorLine INT = ERROR_LINE();
		
		RAISERROR('Error at line %d: %s', 16, 1, @ErrorLine, @ErrorMessage);
		THROW;
	END CATCH
END;