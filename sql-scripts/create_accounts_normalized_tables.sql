/*************************************************************************************
Script			: create_accounts_normalized_tables.sql
Purpose			: ETL procedure to create normalized tables for accounts 
					1. fact_accounts 
					2. dim_account_type
					3. dim_account_status
					4. dim_account_joint_customers
					5. dim_account_type_details
					6. dim_account_products
					7. dim_account_card_details
					8. dim_account_beneficiary
					9. dim_account_documents
Author			: Innocent Nhamo
Created On		: 2025-11-11
Version			: 
				  v.1 - Initial creation
				  v.2 - Fixed schema qualification and table creation
*************************************************************************************/
USE banking_db; 
GO 

CREATE OR ALTER PROCEDURE silver.pcd_create_accounts_normalized_tables
AS 
BEGIN
	SET NOCOUNT ON; 

	BEGIN TRY
		/***********************************************
			Drop tables if they exist
		***********************************************/
		PRINT 'Dropping existing tables.....';

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

		-- Drop tables in correct order (child tables first, then parent tables)
		IF OBJECT_ID('silver.dim_account_documents', 'U') IS NOT NULL 
		BEGIN 
			DROP TABLE silver.dim_account_documents;
			PRINT 'Dropped the silver.dim_account_documents';
		END;

		IF OBJECT_ID('silver.dim_account_beneficiary', 'U') IS NOT NULL 
		BEGIN 
			DROP TABLE silver.dim_account_beneficiary;
			PRINT 'Dropped the silver.dim_account_beneficiary';
		END;

		IF OBJECT_ID('silver.dim_account_card_details', 'U') IS NOT NULL 
		BEGIN 
			DROP TABLE silver.dim_account_card_details;
			PRINT 'Dropped the silver.dim_account_card_details';
		END;

		IF OBJECT_ID('silver.dim_account_products', 'U') IS NOT NULL 
		BEGIN 
			DROP TABLE silver.dim_account_products;
			PRINT 'Dropped the silver.dim_account_products';
		END;

		IF OBJECT_ID('silver.dim_account_type_details', 'U') IS NOT NULL 
		BEGIN 
			DROP TABLE silver.dim_account_type_details;
			PRINT 'Dropped the silver.dim_account_type_details';
		END;

		IF OBJECT_ID('silver.dim_account_joint_customers', 'U') IS NOT NULL 
		BEGIN 
			DROP TABLE silver.dim_account_joint_customers;
			PRINT 'Dropped the silver.dim_account_joint_customers';
		END;

		IF OBJECT_ID('silver.dim_account_status', 'U') IS NOT NULL 
		BEGIN 
			DROP TABLE silver.dim_account_status;
			PRINT 'Dropped the silver.dim_account_status';
		END;

		IF OBJECT_ID('silver.dim_account_type', 'U') IS NOT NULL 
		BEGIN 
			DROP TABLE silver.dim_account_type;
			PRINT 'Dropped the silver.dim_account_type';
		END;

		IF OBJECT_ID('silver.fact_accounts', 'U') IS NOT NULL 
		BEGIN 
			DROP TABLE silver.fact_accounts;
			PRINT 'Dropped the silver.fact_accounts';
		END;

		/***********************************************
			Create tables
		***********************************************/
		-- DIM ACCOUNT TYPE  
		PRINT 'Creating silver.dim_account_type table...';
		CREATE TABLE silver.dim_account_type(
			account_type_id INT IDENTITY(1,1) PRIMARY KEY NOT NULL,
			account_type NVARCHAR(50) NOT NULL,
			created_at DATETIME2 DEFAULT GETDATE(), 
			updated_at DATETIME2 DEFAULT GETDATE(),
			
			-- Constraint
			CONSTRAINT UQ_dim_account_type_account_type UNIQUE(account_type)
		); 
		PRINT 'silver.dim_account_type table created successfully';

		-- FACT ACCOUNTS 
		PRINT 'Creating silver.fact_accounts table...';
		CREATE TABLE silver.fact_accounts(
			account_key INT IDENTITY(1,1) PRIMARY KEY NOT NULL,
			account_id NVARCHAR(50) NOT NULL, 
			account_number NVARCHAR(200) NOT NULL, 
			customer_id NVARCHAR(50) NOT NULL,
			account_purpose NVARCHAR(50), 
			account_type_id INT, 
			is_primary_account BIT,
			branch_code NVARCHAR(50), 
			is_kyc_verified BIT, 
			is_fica_verified BIT,
			expected_amount DECIMAL(15, 2), 
			currency CHAR(4), 
			swift_code NVARCHAR(100), 
			iban NVARCHAR(100), 
			statement_frequency NVARCHAR(50), 
			is_cross_border_enabled BIT, 
			is_minimum_deposit_met BIT, 
			account_opening_channel NVARCHAR(50),  
			created_at DATETIME2 DEFAULT GETDATE(), 
			updated_at DATETIME2 DEFAULT GETDATE(),  

			-- Constraints 
			CONSTRAINT UQ_fact_accounts_account_id_number_customer_id UNIQUE(account_id, account_number, customer_id),
			CONSTRAINT FK_account_type_fact_accounts_account_type_id FOREIGN KEY (account_type_id)
				REFERENCES silver.dim_account_type(account_type_id)
				ON DELETE CASCADE 
				ON UPDATE CASCADE
		);
		PRINT 'silver.fact_accounts table created successfully';

		-- DIM ACCOUNT STATUS
		PRINT 'Creating silver.dim_account_status table...';
		CREATE TABLE silver.dim_account_status(
			account_status_key INT IDENTITY(1,1) PRIMARY KEY NOT NULL,  -- Renamed to avoid confusion
			account_id NVARCHAR(50) NOT NULL, 
			account_status NVARCHAR(50) NOT NULL, 
			account_opening_date DATE NOT NULL, 
			account_approval_date DATE, 
			account_status_change_date DATE, 
			account_closing_date DATE, 
			account_status_change_reason NVARCHAR(255),  -- Changed from DATE to NVARCHAR
			created_at DATETIME2 DEFAULT GETDATE(), 
			updated_at DATETIME2 DEFAULT GETDATE(),
			
			-- Constraint
			CONSTRAINT UQ_dim_account_status_account_id UNIQUE(account_id)
		); 
		PRINT 'silver.dim_account_status table created successfully';

		-- DIM ACCOUNT JOINT CUSTOMERS 
		PRINT 'Creating silver.dim_account_joint_customers table...';
		CREATE TABLE silver.dim_account_joint_customers(
			joint_customer_key INT IDENTITY(1,1) PRIMARY KEY NOT NULL,  -- Renamed for clarity
			account_id NVARCHAR(50) NOT NULL,
			joint_customer_id NVARCHAR(50) NOT NULL,
			created_at DATETIME2 DEFAULT GETDATE(), 
			updated_at DATETIME2 DEFAULT GETDATE(),

			-- Constraints 
			CONSTRAINT UQ_dim_account_joint_customers_account_id_customer_id UNIQUE (account_id, joint_customer_id)
		); 
		PRINT 'silver.dim_account_joint_customers table created successfully';

		-- DIM ACCOUNT TYPE DETAILS  
		PRINT 'Creating silver.dim_account_type_details table...';
		CREATE TABLE silver.dim_account_type_details(
			account_details_key INT IDENTITY(1,1) PRIMARY KEY NOT NULL,  -- Renamed for clarity
			account_id NVARCHAR(50) NOT NULL,
			account_tier NVARCHAR(50), 
			annual_interest_rate DECIMAL(5, 3), 
			monthly_maintenance_fee DECIMAL(10, 2),  -- Increased precision for fees
			transaction_fee_rate DECIMAL(5, 3), 
			overdraft_interest_rate DECIMAL(5, 3), 
			approved_overdraft_limit DECIMAL(15, 2) DEFAULT 0,
			created_at DATETIME2 DEFAULT GETDATE(), 
			updated_at DATETIME2 DEFAULT GETDATE(),
			
			-- Constraint
			CONSTRAINT UQ_dim_account_type_details_account_id UNIQUE(account_id)
		); 
		PRINT 'silver.dim_account_type_details table created successfully';

		-- DIM ACCOUNT PRODUCTS 
		PRINT 'Creating silver.dim_account_products table...';
		CREATE TABLE silver.dim_account_products(
			account_product_key INT IDENTITY(1,1) PRIMARY KEY NOT NULL,  -- Renamed for clarity
			account_id NVARCHAR(50) NOT NULL,
			product_type NVARCHAR(50), 
			product_activation_date DATE,
			created_at DATETIME2 DEFAULT GETDATE(), 
			updated_at DATETIME2 DEFAULT GETDATE(),

			-- Constraint 
			CONSTRAINT UQ_dim_account_products_account_product UNIQUE (account_id, product_type)
		);
		PRINT 'silver.dim_account_products table created successfully';

		-- DIM CARD DETAILS 
		PRINT 'Creating silver.dim_account_card_details table...';
		CREATE TABLE silver.dim_account_card_details(
			card_key INT IDENTITY(1,1) PRIMARY KEY NOT NULL,  -- Renamed for clarity
			account_id NVARCHAR(50) NOT NULL,
			card_number NVARCHAR(100) NOT NULL,
			card_type NVARCHAR(50) NOT NULL, 
			card_issue_date DATE NOT NULL, 
			card_expiry_date DATE NOT NULL, 
			is_card_replaced BIT, 
			card_replacement_date DATE, 
			credit_card_limit DECIMAL(15, 2) DEFAULT 0,
			created_at DATETIME2 DEFAULT GETDATE(), 
			updated_at DATETIME2 DEFAULT GETDATE(),

			-- Constraints 
			CONSTRAINT UQ_dim_card_details_card_number UNIQUE (card_number) 
		); 
		PRINT 'silver.dim_account_card_details table created successfully';

		-- DIM ACCOUNTS BENEFICIARY 
		PRINT 'Creating silver.dim_account_beneficiary table...';
		CREATE TABLE silver.dim_account_beneficiary(
			beneficiary_key INT IDENTITY(1,1) PRIMARY KEY NOT NULL,  -- Renamed for clarity
			account_id NVARCHAR(50) NOT NULL,
			beneficiary_name NVARCHAR(100) NOT NULL,  -- Increased size
			beneficiary_relationship NVARCHAR(50), 
			beneficiary_percentage DECIMAL(5, 2),  -- Changed to 5,2 for proper percentage (0-100)
			created_at DATETIME2 DEFAULT GETDATE(), 
			updated_at DATETIME2 DEFAULT GETDATE(),

			-- Constraints 
			CONSTRAINT UQ_dim_account_beneficiary_account_beneficiary UNIQUE (account_id, beneficiary_name)
		); 
		PRINT 'silver.dim_account_beneficiary table created successfully';

		-- DIM ACCOUNT DOCUMENTS 
		PRINT 'Creating silver.dim_account_documents table...';
		CREATE TABLE silver.dim_account_documents(
			document_key INT IDENTITY(1,1) PRIMARY KEY NOT NULL,  -- Renamed for clarity
			account_id NVARCHAR(50) NOT NULL,
			is_proof_of_income_provided BIT,	
			is_bank_statements_provided BIT,	
			is_employer_letter_provided BIT,	
			is_business_registration_provided BIT,	
			is_tax_certificate_provided BIT,
			document_status_score DECIMAL(5, 2),  -- Changed to 5,2 for proper percentage
			created_at DATETIME2 DEFAULT GETDATE(), 
			updated_at DATETIME2 DEFAULT GETDATE(),
			
			-- Constraint
			CONSTRAINT UQ_dim_account_documents_account_id UNIQUE(account_id)
		); 
		PRINT 'silver.dim_account_documents table created successfully';

		/***********************************************
			Create table indexes
		***********************************************/
		PRINT 'Creating indexes.....';

		-- fact accounts table 
		CREATE INDEX IDX_fact_accounts_account_id ON silver.fact_accounts(account_id);
		CREATE INDEX IDX_fact_accounts_customer_id ON silver.fact_accounts(customer_id);
		CREATE INDEX IDX_fact_accounts_account_type_id ON silver.fact_accounts(account_type_id);
		
		-- dim tables indexes
		CREATE INDEX IDX_dim_account_status_account_id ON silver.dim_account_status(account_id);
		CREATE INDEX IDX_dim_account_joint_customers_account_id ON silver.dim_account_joint_customers(account_id);
		CREATE INDEX IDX_dim_account_type_details_account_id ON silver.dim_account_type_details(account_id);
		CREATE INDEX IDX_dim_account_products_account_id ON silver.dim_account_products(account_id);
		CREATE INDEX IDX_dim_account_card_details_account_id ON silver.dim_account_card_details(account_id);
		CREATE INDEX IDX_dim_account_beneficiary_account_id ON silver.dim_account_beneficiary(account_id);
		CREATE INDEX IDX_dim_account_documents_account_id ON silver.dim_account_documents(account_id);
		
		PRINT 'Indexes created successfully';
		PRINT 'All tables and indexes created successfully!';
		
	END TRY
	BEGIN CATCH
		DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
		DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
		DECLARE @ErrorState INT = ERROR_STATE();
		
		PRINT 'Error occurred: ' + @ErrorMessage;
		
		RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);
	END CATCH
END;
GO