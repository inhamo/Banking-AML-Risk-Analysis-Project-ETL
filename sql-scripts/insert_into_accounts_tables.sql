/*************************************************************************************
Script		: insert_into_customer_tables.sql
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
		
		CREATE INDEX IDX_stg_accounts_account_type ON #stg_accounts(account_type);
		CREATE INDEX IDX_stg_accounts_account_id ON #stg_accounts(account_id);
		CREATE INDEX IDX_stg_accounts_customer_id ON #stg_accounts(customer_id);
		
		PRINT 'Indexes created successfully';

		/***********************************************
			Insert into the account type dimension table
		***********************************************/
		RAISERROR('Loading data into the account type table....', 0, 1) WITH NOWAIT; 

		INSERT INTO silver.dim_account_type (account_type)
		SELECT DISTINCT account_type 
		FROM #stg_accounts
		WHERE account_type IS NOT NULL
			AND NOT EXISTS (
			SELECT 1 
			FROM silver.dim_account_type dat
			WHERE dat.account_type = #stg_accounts.account_type
			);
		
		SET @RowCount = @@ROWCOUNT;
		RAISERROR('Loaded %d account types successfully', 0, 1, @RowCount) WITH NOWAIT; 
		

		/***********************************************
			Insert into the fact table
		***********************************************/
		RAISERROR('Loading data into the fact account table.....', 0, 1) WITH NOWAIT;
		INSERT INTO silver.fact_accounts (
			account_id, account_number, customer_id, account_purpose, account_type_id, 
			is_primary_account, branch_code, is_kyc_verified, is_fica_verified, expected_amount, 
			currency, swift_code, iban, statement_frequency, is_cross_border_enabled, 
			is_minimum_deposit_met, account_opening_channel
		)
		SELECT DISTINCT 
			sa.account_id, 
			sa.account_number, 
			sa.customer_id, 
			sa.account_purpose, 
			aat.account_type_id, 
			sa.is_primary_account, 
			sa.branch_code, 
			sa.kyc_verified AS is_kyc_verified, 
			sa.fica_verified AS is_fica_verified, 
			sa.expected_amount, 
			sa.currency, 
			sa.swift_code, 
			sa.iban, 
			sa.statement_frequency, 
			sa.cross_border_enabled AS is_cross_border_enabled, 
			sa.minimum_deposit_met AS is_minimum_deposit_met, 
			sa.account_opening_channel
		FROM #stg_accounts sa 
		INNER JOIN silver.dim_account_type aat
		ON sa.account_type = aat.account_type

		SET @RowCount = @@ROWCOUNT;
		RAISERROR('Loaded %d accounts into fact table successfully', 0, 1, @RowCount) WITH NOWAIT;

		/***********************************************
			Insert into the account status table
		***********************************************/
		RAISERROR('Loading data into the account status table.....', 0, 1) WITH NOWAIT;
		INSERT INTO silver.dim_account_status (
			account_id, account_status,	account_opening_date, account_approval_date,
			account_status_change_date,	account_closing_date, account_status_change_reason
		)
		SELECT DISTINCT 
			account_id, 
			account_status,	
			account_opening_date,	
			account_approval_date,	
			account_status_change_date,	
			account_closing_date,	
			account_status_change_reason
		FROM #stg_accounts
		SET @RowCount = @@ROWCOUNT;
		RAISERROR('Loaded %d accounts into account status table successfully', 0, 1, @RowCount) WITH NOWAIT;


		/***********************************************
			Insert into the joint accounts table
		***********************************************/
		RAISERROR('Loading data into the joint accounts table.....', 0, 1) WITH NOWAIT;
		INSERT INTO silver.dim_account_joint_customers(
			account_id, joint_customer_id
		)
		SELECT DISTINCT 
			account_id, 
			joint_customer_id
		FROM #stg_accounts
		WHERE joint_customer_id IS NOT NULL

		SET @RowCount = @@ROWCOUNT;
		RAISERROR('Loaded %d accounts into joint accounts table successfully', 0, 1, @RowCount) WITH NOWAIT;

		/***********************************************
			Insert into the account type details table
		***********************************************/
		RAISERROR('Loading data into the account type details table.....', 0, 1) WITH NOWAIT;
		INSERT INTO silver.dim_account_type_details(
			account_id, account_tier, annual_interest_rate, monthly_maintenance_fee, 
			transaction_fee_rate, overdraft_interest_rate, approved_overdraft_limit
		)
		SELECT DISTINCT 
			account_id, 
			account_tier, 
			interest_rate AS annual_interest_rate, 
			monthly_charges AS monthly_maintenance_fee, 
			transactions_rate AS transaction_fee_rate, 
			negative_balance_rate AS overdraft_interest_rate, 
			overdraft_limit AS approved_overdraft_limit
		FROM #stg_accounts
		SET @RowCount = @@ROWCOUNT;
		RAISERROR('Loaded %d accounts into account type details table successfully', 0, 1, @RowCount) WITH NOWAIT;

		/***********************************************
			Insert into the account products table 
		***********************************************/
		RAISERROR('Loading data into the account products table.....', 0, 1) WITH NOWAIT;
		INSERT INTO silver.dim_account_products(
			account_id, product_type, product_activation_date
		)
		SELECT DISTINCT 
			account_id, 
			bundled_product AS product_type, 
			CASE 
				WHEN online_banking_enabled = 1 THEN online_banking_activation_date
				ELSE account_approval_date 
			END AS product_activation_date
		FROM #stg_accounts
		WHERE bundled_product IS NOT NULL
		SET @RowCount = @@ROWCOUNT;
		RAISERROR('Loaded %d accounts into account products table successfully', 0, 1, @RowCount) WITH NOWAIT;

		/***********************************************
			Insert into the account cards table
		***********************************************/
		RAISERROR('Loading data into the account cards table.....', 0, 1) WITH NOWAIT;
		INSERT INTO silver.dim_account_card_details(
			account_id, card_number, card_type, card_issue_date, 
			card_expiry_date, is_card_replaced, card_replacement_date,
			credit_card_limit
		)
		SELECT DISTINCT 
			account_id,	
			card_number,	
			card_type,	
			card_issue_date,	
			card_expiry_date,	
			card_replaced AS is_card_replaced,	
			card_replacement_date,	
			credit_card_limit
		FROM #stg_accounts
		WHERE card_number IS NOT NULL

		SET @RowCount = @@ROWCOUNT;
		RAISERROR('Loaded %d accounts into accounts cards table successfully', 0, 1, @RowCount) WITH NOWAIT;

		/***********************************************
			Insert into the account beneficiary table
		***********************************************/
		RAISERROR('Loading data into the account beneficiary table.....', 0, 1) WITH NOWAIT;
		INSERT INTO silver.dim_account_beneficiary (
			account_id, beneficiary_name, beneficiary_relationship ,beneficiary_percentage
		)
		SELECT DISTINCT 
			account_id, 
			beneficiary_name, 
			beneficiary_relationship,
			beneficiary_percentage
		FROM #stg_accounts
		WHERE beneficiary_name IS NOT NULL
		SET @RowCount = @@ROWCOUNT;
		RAISERROR('Loaded %d accounts into accounts beneficiary table successfully', 0, 1, @RowCount) WITH NOWAIT;

		/***********************************************
			Insert into the account documents table
		***********************************************/
		RAISERROR('Loading data into the account documents table.....', 0, 1) WITH NOWAIT;
		INSERT INTO silver.dim_account_documents (
			account_id, is_proof_of_income_provided, is_bank_statements_provided, 
			is_employer_letter_provided, is_business_registration_provided, is_tax_certificate_provided, 
			document_status_score
		)
		SELECT DISTINCT 
			account_id, 
			proof_of_income_provided AS is_proof_of_income_provided, 
			bank_statements_provided AS is_bank_statements_provided, 
			employer_letter_provided AS is_employer_letter_provided, 
			business_registration_provided AS is_business_registration_provided, 
			tax_certificate_provided AS is_tax_certificate_provided,
			(
				proof_of_income_provided + bank_statements_provided + employer_letter_provided + 
				business_registration_provided + tax_certificate_provided
			)
			AS document_status_score
		FROM #stg_accounts
		SET @RowCount = @@ROWCOUNT;
		RAISERROR('Loaded %d accounts into accounts documents table successfully', 0, 1, @RowCount) WITH NOWAIT;

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
