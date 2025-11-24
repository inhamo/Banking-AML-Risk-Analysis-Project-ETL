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

--- Procedure 
CREATE OR ALTER PROCEDURE silver.pcd_clean_accounts_to_staging
AS 
BEGIN 
	-- SET NOCOUNT ON;

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
		VALUES (@BatchID, 'pcd_clean_accounts_to_staging', @StartTime, 'STARTED')

        /********************************************
			Data cleaning pipeline
        *********************************************/
		-- Drop temp table if it already exists
		IF OBJECT_ID('tempdb..#stg_accounts') IS NOT NULL
			DROP TABLE #stg_accounts;

		IF OBJECT_ID('tempdb..#accounts_temp') IS NOT NULL
			DROP TABLE #accounts_temp;

		RAISERROR('Union all accounts tables...', 0, 1) WITH NOWAIT;
		SELECT * 
		INTO #accounts_temp 
		FROM (
			SELECT *, 'erp_accounts_2018' AS source_file FROM bronze.erp_accounts_2018
			UNION ALL 
			SELECT *, 'erp_accounts_2019' AS source_file FROM bronze.erp_accounts_2019
			UNION ALL 
			SELECT *, 'erp_accounts_2020' AS source_file FROM bronze.erp_accounts_2020
			UNION ALL 
			SELECT *, 'erp_accounts_2021' AS source_file FROM bronze.erp_accounts_2021
			UNION ALL 
			SELECT *, 'erp_accounts_2022' AS source_file FROM bronze.erp_accounts_2022
			UNION ALL 
			SELECT *, 'erp_accounts_2023' AS source_file FROM bronze.erp_accounts_2023
			UNION ALL 
			SELECT *, 'erp_accounts_2024' AS source_file FROM bronze.erp_accounts_2024
		) AS all_accounts;
		RAISERROR('Union all accounts tables created', 0, 1) WITH NOWAIT;

		RAISERROR('Data cleaning process started...', 0, 1) WITH NOWAIT;
		WITH CTE_check_account_number_duplicates AS (
			SELECT *, 
				COUNT(account_number) OVER(PARTITION BY account_number) AS account_number_count,
				COUNT(account_id) OVER(PARTITION BY account_number) AS account_id_count, 
				COUNT(card_number) OVER(PARTITION BY card_number) AS card_number_count
			FROM #accounts_temp
		), 
		CTE_check_primary_account_sanity AS (
			SELECT *, 
				SUM(is_primary_account) OVER(PARTITION BY customer_id) AS primary_acc_count
			FROM CTE_check_account_number_duplicates
		), 
		CTE_highlight_multiple_primary_acc AS (
			SELECT *, 
				CASE WHEN primary_acc_count > 1 THEN 1 ELSE 0 END over_one_primary_acc
			FROM CTE_check_primary_account_sanity
		), 
		CTE_highlight_invalid_dates AS (
			SELECT cte.*,
				CASE 
					WHEN 
						fc.customer_registration_date > cte.opening_date 
						OR fc.customer_registration_date > cte.approval_date
						OR fc.customer_registration_date > cte.status_change_date
						OR fc.customer_registration_date > cte.closure_date
						OR fc.customer_registration_date > cte.online_banking_activation_date
						OR fc.customer_registration_date > cte.card_issue_date
						OR fc.customer_registration_date > cte.card_replacement_date
						OR cte.opening_date > cte.approval_date
						OR cte.opening_date > cte.status_change_date
						OR cte.opening_date > cte.closure_date
						OR cte.opening_date > cte.online_banking_activation_date
						OR cte.opening_date > cte.card_issue_date
						OR cte.opening_date > cte.card_replacement_date
						OR cte.approval_date > cte.status_change_date
						OR cte.approval_date > cte.closure_date 
						OR cte.approval_date > cte.online_banking_activation_date
						OR cte.approval_date > cte.card_replacement_date
						OR cte.closure_date > cte.online_banking_activation_date
						OR cte.closure_date > cte.card_issue_date
						OR cte.closure_date > cte.card_replacement_date
						OR card_issue_date > cte.card_expiry_date
						OR cte.card_issue_date > cte.card_replacement_date
					THEN 1 
					ELSE 0 
				END invalid_date
			FROM CTE_highlight_multiple_primary_acc cte
			JOIN silver.fact_customers fc
			ON fc.customer_id = cte.customer_id
		), 
		CTE_highlight_missing_status_information AS (
			SELECT * , 
				CASE 
					WHEN account_status != 'ACTIVE' AND status_change_date IS NULL THEN 1 ELSE 0 END AS missing_status_change_date
			FROM CTE_highlight_invalid_dates
		),	
		CTE_outer_apply_joint_accounts AS (
			SELECT cte.*, 
				UPPER(TRIM(j.value)) AS joint_customer_id
			FROM CTE_highlight_missing_status_information cte
			OUTER APPLY STRING_SPLIT(linked_joint_accounts, ';') j
		), 
		CTE_outer_apply_bundled_products AS (
			SELECT cte.*, 
				UPPER(TRIM(bp.value)) AS bundled_product
			FROM CTE_outer_apply_joint_accounts cte 
			OUTER APPLY STRING_SPLIT(bundled_products, ';') bp
		), 
		CTE_outer_apply_account_beneficiaries AS (
			SELECT cte.*, 
				(CAST
					(TRIM(
						REPLACE(
							PARSENAME(REPLACE(ben.value, '|', '.'), 1)
							, '%', '')
						) 
				AS INT) / 100) AS beneficiary_percentage, 
				PARSENAME(REPLACE(ben.value, '|', '.'), 2) AS beneficiary_relationship, 
				PARSENAME(REPLACE(ben.value, '|', '.'), 3) AS beneficiary_name
			FROM CTE_outer_apply_bundled_products cte
			OUTER APPLY STRING_SPLIT(beneficiaries, ';') ben
		)
		SELECT 
					account_id, 
					account_number, 
					UPPER(TRIM(customer_id)) AS customer_id, 
					UPPER(TRIM(account_type)) AS account_type, 
					REPLACE(UPPER(TRIM(account_purpose)), '_', ' ') AS account_purpose, 
					is_primary_account,
					CAST(opening_date AS DATE) AS account_opening_date ,
					CAST(approval_date AS DATE) AS account_approval_date, 
					branch_code, 
					kyc_verified, 
					fica_verified, 
					expected_amount, 
					UPPER(TRIM(account_status)) AS account_status, 
					CAST(status_change_date AS DATE) AS account_status_change_date, 
					CAST(closure_date AS DATE) AS account_closing_date, 
					REPLACE(UPPER(TRIM(status_reason)), '_', ' ') AS account_status_change_reason, 
					joint_customer_id, 
					interest_rate, 
					monthly_charges, 
					transactions_rate, 
					negative_balance_rate, 
					overdraft_limit, 
					credit_card_limit, 
					REPLACE(bundled_product, '_', ' ') AS bundled_product, 
					currency, 
					swift_code, 
					iban, 
					UPPER(TRIM(account_tier)) AS account_tier, 
					UPPER(TRIM(statement_frequency)) AS statement_frequency, 
					online_banking_enabled, 
					CAST(online_banking_activation_date AS DATE) AS online_banking_activation_date, 
					card_number, 
					REPLACE(UPPER(TRIM(card_type)), '_', ' ') AS card_type, 
					CAST(card_issue_date AS DATE) AS card_issue_date, 
					CAST(card_expiry_date AS DATE) AS card_expiry_date, 
					card_replaced, 
					CAST(card_replacement_date AS DATE) AS card_replacement_date,
					beneficiary_name, 
					beneficiary_relationship, 
					beneficiary_percentage,
					cross_border_enabled, 
					proof_of_income_provided, 
					bank_statements_provided, 
					employer_letter_provided,
					business_registration_provided, 
					tax_certificate_provided, 
					minimum_deposit_met, 
					REPLACE(UPPER(TRIM(opening_channel)), '_', ' ') AS account_opening_channel,
					account_id_count, 
					account_number_count, 
					card_number_count, 
					invalid_date, 
					over_one_primary_acc
		INTO #stg_accounts 
		FROM CTE_outer_apply_account_beneficiaries;
		RAISERROR('Data cleaning process completed', 0, 1) WITH NOWAIT;

		-- Get the number of rows inserted
		SET @RowsInserted = @@ROWCOUNT;

        /********************************************
			Create normalized tables
        *********************************************/

		-- Check if stored procedure exists and execute it
		IF EXISTS (SELECT 1 FROM sys.procedures WHERE name = 'pcd_create_accounts_normalized_tables' AND schema_id = SCHEMA_ID('silver'))
		BEGIN 
			EXEC silver.pcd_create_accounts_normalized_tables; 
			PRINT 'Stored procedure executed successfully'; 
		END 
		ELSE 
		BEGIN 
			PRINT 'Stored procedure silver.pcd_create_accounts_normalized_tables does not exist';
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


        /********************************************
			Log successful completion
        *********************************************/

		UPDATE error_log.etl_log
		SET end_time = SYSDATETIME(),
			status = 'SUCCESS',
			rows_processed = @RowsInserted
		WHERE batch_id = @BatchID;
		
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

		RAISERROR('ETL failed in pcd_clean_accounts_to_staging: %s', 16, 1, @ErrorMessage);

	END CATCH
END;
GO

EXEC silver.pcd_clean_accounts_to_staging