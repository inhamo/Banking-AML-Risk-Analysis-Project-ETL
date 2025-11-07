/*************************************************************************************
Script		: pcd_create_customer_normalized_tables.sql
Purpose		: ETL procedure to create tables for customers tables 
Author		: Innocent Nhamo
Created On	: 2025-11-07
Version		: 
			  v.1 - Initial creation 
**************************************************************************************/
USE banking_db;
GO

-- Procedure 
CREATE OR ALTER PROCEDURE silver.pcd_create_customer_normalized_tables
AS
BEGIN
	SET NOCOUNT ON;

	/***********************************************
		Drop tables if they exist
	***********************************************/
	PRINT 'Dropping existing tables...';
	IF OBJECT_ID('silver.dim_director_information', 'U') IS NOT NULL 
	BEGIN 
		DROP TABLE silver.dim_director_information;
		PRINT 'Dropped the silver.dim_director_information';
	END;

	IF OBJECT_ID('silver.dim_business_directors', 'U') IS NOT NULL 
	BEGIN 
		DROP TABLE silver.dim_business_directors;
		PRINT 'Dropped the silver.dim_business_directors';
	END;

	IF OBJECT_ID('silver.dim_business_shareholders', 'U') IS NOT NULL 
	BEGIN 
		DROP TABLE silver.dim_business_shareholders; 
		PRINT 'Dropped the silver.dim_business_shareholders';
	END;

	IF OBJECT_ID('silver.dim_business_licensing', 'U') IS NOT NULL 
	BEGIN 
		DROP TABLE silver.dim_business_licensing;
		PRINT 'Dropped the silver.dim_business_licensing';
	END;

	IF OBJECT_ID('silver.dim_business_details', 'U') IS NOT NULL 
	BEGIN 
		DROP TABLE silver.dim_business_details; 
		PRINT 'Dropped the silver.dim_business_details';
	END;

	IF OBJECT_ID('silver.dim_customer_profile ', 'U') IS NOT NULL 
	BEGIN 
		DROP TABLE silver.dim_customer_profile ; 
		PRINT 'Dropped the silver.dim_customer_profile ';
	END;

	IF OBJECT_ID('silver.dim_customer_risks', 'U') IS NOT NULL 
	BEGIN 
		DROP TABLE silver.dim_customer_risks; 
		PRINT 'Dropped the silver.dim_customer_risks';
	END;

	IF OBJECT_ID('silver.dim_customer_documents', 'U') IS NOT NULL 
	BEGIN 
		DROP TABLE silver.dim_customer_documents;
		PRINT 'Dropped the silver.dim_customer_documents';
	END;

	IF OBJECT_ID('silver.dim_customer_contacts', 'U') IS NOT NULL 
	BEGIN 
		DROP TABLE silver.dim_customer_contacts; 
		PRINT 'Dropped the silver.dim_customer_contacts';
	END;

	IF OBJECT_ID('silver.dim_customer_addresses', 'U') IS NOT NULL 
	BEGIN 
		DROP TABLE silver.dim_customer_addresses; 
		PRINT 'Dropped the silver.dim_customer_addresses';
	END;

	IF OBJECT_ID('silver.fact_customers', 'U') IS NOT NULL 
	BEGIN 
		DROP TABLE silver.fact_customers; 
		PRINT 'Dropped the silver.fact_customers';
	END;

	PRINT 'Table dropping completed';

	/***********************************************
		Create tables
	***********************************************/
	-- FACT CUSTOMERS 
	PRINT 'Creating fact_customers table...';
	CREATE TABLE silver.fact_customers (
		customer_key INT IDENTITY(1,1) PRIMARY KEY NOT NULL, 
		customer_id NVARCHAR(50) NOT NULL,
		customer_type NVARCHAR(20) NOT NULL CHECK(customer_type IN ('INDIVIDUAL', 'COMPANY', 'ORGANIZATION')),
		customer_name NVARCHAR(50) NOT NULL, 
		original_date_of_birth DATE NOT NULL, 
		fIDXed_date_of_birth DATE NOT NULL, 
		original_age_at_opening INT NOT NULL, 
		fIDXed_age_at_opening INT NOT NULL, 
		citizenship CHAR(3) NOT NULL, 
		nationality NVARCHAR(50) NOT NULL, 
		is_dual_or_transfered BIT, 
		gender NVARCHAR(20) CHECK (gender IN ('F', 'M', 'NOT APPLICABLE', 'PREFER NOT TO SAY')),
		ethinicity NVARCHAR(20) NOT NULL, 
		preferred_contact_method NVARCHAR(20) NOT NULL, 
		customer_registration_date DATE, 
		age_group NVARCHAR(50) CHECK(age_group IN ('YOUNG ADULT', 'EARLY CAREER', 'ESTABLISHED PROFESSIONAL', 'MID-CAREER', 'PRE-RETIREMENT', 'RETIREES')), 
		created_date DATETIME2 DEFAULT GETDATE(), 
		update_date DATETIME2 DEFAULT GETDATE(), 

		-- Constraints 
		-- Prevent duplicate customer id
		CONSTRAINT UQ_fact_customers_customer_id UNIQUE (customer_id)
	); 
	PRINT 'fact_customer table created successfully';

	-- DIM ADDRESSES 
	PRINT 'Creating dim_addresses table...';
	CREATE TABLE silver.dim_addresses (
		address_key INT IDENTITY(1, 1) PRIMARY KEY NOT NULL, 
		customer_id NVARCHAR(50) NOT NULL, 
		address_type NVARCHAR(20) NOT NULL CHECK(address_type IN ('RESIDENTIAL', 'COMMERCIAL', 'PRINCIPAL', 'OFFICE')), 
		street_name NVARCHAR(MAX), 
		city NVARCHAR(50), 
		province NVARCHAR(50), 
		country NVARCHAR(50) DEFAULT 'SOUTH AFRICA', 
		is_current_address BIT DEFAULT 1,
		effective_date DATE, 
		end_date DATE DEFAULT NULL,
		created_date DATETIME2 DEFAULT GETDATE(), 
		update_date DATETIME2 DEFAULT GETDATE(), 

		-- Constraints
		CONSTRAINT FK_dim_addresses_fact_customers  FOREIGN KEY (customer_id) 
			REFERENCES silver.fact_customers(customer_id)
			ON DELETE CASCADE
			ON UPDATE CASCADE
	);
	PRINT 'dim_addresses table created successfully';

	-- DIM CONTACTS 
	PRINT 'Creating dim_contacts table...';
	CREATE TABLE silver.dim_contacts (
		contact_key INT IDENTITY(1,1) PRIMARY KEY NOT NULL, 
		customer_id NVARCHAR(50) NOT NULL, 
		contact_type NVARCHAR(20) NOT NULL CHECK (contact_type IN ('PHONE', 'EMAIL', 'NEXT OF KIN')), 
		contact NVARCHAR(100) NOT NULL, 
		contact_status NVARCHAR(20) DEFAULT 'ACTIVE' CHECK (contact_status IN ('ACTIVE', 'INVALID', 'INACTIVE')),
		is_primary_contact BIT DEFAULT 1,
		is_opted_in_marketing BIT DEFAULT 0,
		created_date DATETIME2 DEFAULT GETDATE(), 
		update_date DATETIME2 DEFAULT GETDATE(), 

		-- Constraints 
		CONSTRAINT FK_dim_contacts_fact_customers FOREIGN KEY (customer_id) 
			REFERENCES silver.fact_customers(customer_id) 
			ON DELETE CASCADE 
			ON UPDATE CASCADE,

		-- Prevent duplicate contacts for the same customer
		CONSTRAINT UQ_dim_contacts_customer_contact UNIQUE (customer_id, contact_type, contact)
	);
	PRINT 'dim_contacts table created successfully';

	-- DIM DOCUMENTS 
	PRINT 'Creating dim_documents table...';
	CREATE TABLE silver.dim_documents (
		document_key INT IDENTITY(1,1) PRIMARY KEY NOT NULL, 
		customer_id NVARCHAR(50) NOT NULL, 
		document_type NVARCHAR(50) NOT NULL,
		document_status NVARCHAR(20) DEFAULT 'VERIFIED' CHECK (document_status IN (
			'PENDING', 'VERIFIED', 'REJECTED', 'EXPIRED', 'UNDER_REVIEW', 'ARCHIVED'
		)),
		document_number NVARCHAR(50) NOT NULL, 
		expiry_review_required BIT DEFAULT 0, 
		document_expiry_date DATE,
		created_date DATETIME2 DEFAULT GETDATE(), 
		update_date DATETIME2 DEFAULT GETDATE(), 

		-- Constraints 
		CONSTRAINT FK_dim_documents_fact_customers FOREIGN KEY (customer_id) 
			REFERENCES silver.fact_customers(customer_id) 
			ON DELETE CASCADE 
			ON UPDATE CASCADE,
	);
	PRINT 'dim_documents table created successfully';

	-- DIM CUSTOMER RISKS 
	PRINT 'Creating dim_customer_risks table...';
	CREATE TABLE silver.dim_customer_risks (
		risk_key INT IDENTITY(1,1) PRIMARY KEY NOT NULL, 
		customer_id NVARCHAR(50) NOT NULL, 
		is_pep BIT, 
		pep_type NVARCHAR(50), 
		is_sanctioned_country BIT, 
		is_director BIT, 
		is_supervised_aml_cft BIT,
		risk_score DECIMAL(5, 5), 
		risk_level NVARCHAR(20) CHECK (risk_level IN ('LOW', 'MEDIUM', 'HIGH')), 
		created_date DATETIME2 DEFAULT GETDATE(), 
		update_date DATETIME2 DEFAULT GETDATE(), 

		-- Constraints 
		CONSTRAINT FK_dim_customer_risks_fact_customers FOREIGN KEY (customer_id) 
			REFERENCES silver.fact_customers(customer_id) 
			ON DELETE CASCADE 
			ON UPDATE CASCADE,
	);
	PRINT 'dim_customer_risks table created successfully';

	-- EMPLOYMENT EDUCATION AND MARITAL STATUS 
	PRINT 'Creating dim_customer_profile  table...';
	CREATE TABLE silver.dim_customer_profile  (
		profile_key INT IDENTITY(1,1) PRIMARY KEY NOT NULL, 
		customer_id NVARCHAR(50) NOT NULL, 
		occupation NVARCHAR(50) NOT NULL, 
		employer_name NVARCHAR(50), 
		source_of_funds NVARCHAR(50) NOT NULL, 
		marital_status NVARCHAR(20) NOT NULL,
		education_level NVARCHAR(50) NOT NULL,
		annual_expected_income DECIMAL(15, 2) NOT NULL, 
		created_date DATETIME2 DEFAULT GETDATE(), 
		update_date DATETIME2 DEFAULT GETDATE(), 

		-- Constraints 
		CONSTRAINT FK_dim_customer_profile_fact_customers FOREIGN KEY (customer_id) 
			REFERENCES silver.fact_customers(customer_id) 
			ON DELETE CASCADE 
			ON UPDATE CASCADE,
	);
	PRINT 'dim_customer_profile  table created successfully';

	-- DIM BUSINESS DETAILS 
	PRINT 'Creating dim_business_details table...';
	CREATE TABLE silver.dim_business_details (
		business_key INT IDENTITY(1,1) PRIMARY KEY NOT NULL, 
		customer_id NVARCHAR(50) NOT NULL, 
		trading_name NVARCHAR(50), 
		website NVARCHAR(50), 
		has_sanctioned_relationship BIT, 
		fatca_giin NVARCHAR(50), 
		swift_bic_code NVARCHAR(50), 
		industry NVARCHAR(50), 
		date_of_incorporation DATE, 
		city_of_incorporation NVARCHAR(50), 
		country_of_incorporation NVARCHAR(50) NOT NULL DEFAULT 'SOUTH AFRICA',
		number_of_employees INT, 
		director_count INT, 
		annual_turnover DECIMAL(15, 2), 
		shareholder_count INT, 
		bee_level INT, 
		vat_registered INT, 
		industry_risk_level NVARCHAR(20),
		is_public BIT, 
		external_auditor NVARCHAR(50), 
		legal_representative_name NVARCHAR(50), 
		contact_person NVARCHAR(50), 
		contact_person_title NVARCHAR(50),	
		contact_person_phone NVARCHAR(50),	
		contact_person_email NVARCHAR(50),	
		contact_person_customer_id NVARCHAR(50),	
		num_branches_local NVARCHAR(50),	
		num_branches_foreign INT,	
		num_subsidiaries_local INT,
		num_subsidiaries_foreign INT,
		age_related_risk NVARCHAR(20),
		is_license_required BIT,
		created_date DATETIME2 DEFAULT GETDATE(), 
		update_date DATETIME2 DEFAULT GETDATE(), 

		-- Constraints 
		CONSTRAINT FK_dim_business_details_fact_customers FOREIGN KEY (customer_id) 
			REFERENCES silver.fact_customers(customer_id) 
			ON DELETE CASCADE 
			ON UPDATE CASCADE,
	);
	PRINT 'dim_business_details table created successfully';

	-- DIM BUSINESS LICENSING	
	PRINT 'Creating dim_business_licensing table...';	
	CREATE TABLE silver.dim_business_licensing (	
		licensing_key INT IDENTITY(1,1) PRIMARY KEY NOT NULL, 
		customer_id NVARCHAR(50) NOT NULL, 
		license_number NVARCHAR(50), 
		license_issue_date DATE, 
		license_expiry_date DATE, 
		license_regulatory_body NVARCHAR(50),
		created_date DATETIME2 DEFAULT GETDATE(), 
		update_date DATETIME2 DEFAULT GETDATE(), 

		-- Constraints 
		CONSTRAINT FK_dim_business_licensing_fact_customers FOREIGN KEY (customer_id) 
			REFERENCES silver.fact_customers(customer_id) 
			ON DELETE CASCADE 
			ON UPDATE CASCADE,
	);	
	PRINT 'dim_business_licensing table created successfully';

	-- DIM BUSINESS SHAREHOLDERS	
	PRINT 'Creating dim_business_shareholders table...';	
	CREATE TABLE silver.dim_business_shareholders (
		shareholder_key INT IDENTITY(1,1) PRIMARY KEY NOT NULL, 
		customer_id NVARCHAR(50) NOT NULL, 
		shareholder_id NVARCHAR(50), 
		shareholder_name NVARCHAR(50), 
		ownership_percentage DECIMAL(5, 5),
		created_date DATETIME2 DEFAULT GETDATE(), 
		update_date DATETIME2 DEFAULT GETDATE(), 

		-- Constraints 
		CONSTRAINT FK_dim_business_shareholders_fact_customers FOREIGN KEY (customer_id) 
			REFERENCES silver.fact_customers(customer_id) 
			ON DELETE CASCADE 
			ON UPDATE CASCADE,
	);	
	PRINT 'dim_business_shareholders table created successfully';	

	-- DIM BUSINESS DIRECTORS	
	PRINT 'Creating dim_business_directors table...';	
	CREATE TABLE silver.dim_business_directors (
		director_key INT IDENTITY(1,1) PRIMARY KEY NOT NULL, 
		customer_id NVARCHAR(50) NOT NULL, 
		director_customer_id NVARCHAR(50) NOT NULL,
		created_date DATETIME2 DEFAULT GETDATE(), 
		update_date DATETIME2 DEFAULT GETDATE(), 

		-- Constraints 
		CONSTRAINT FK_dim_business_directors_fact_customers FOREIGN KEY (customer_id) 
			REFERENCES silver.fact_customers(customer_id) 
			ON DELETE CASCADE 
			ON UPDATE CASCADE,
	);	
	PRINT 'dim_business_directors table created successfully';	

	-- DIM DIRECTOR INFORMATION	
	PRINT 'Creating dim_director_information table...';	
	CREATE TABLE silver.dim_director_information (
		director_info_key INT IDENTITY(1,1) PRIMARY KEY NOT NULL, 
		customer_id NVARCHAR(50) NOT NULL, 
		directed_company NVARCHAR(50) NOT NULL,
		created_date DATETIME2 DEFAULT GETDATE(), 
		update_date DATETIME2 DEFAULT GETDATE(), 

		-- Constraints 
		CONSTRAINT FK_dim_director_information_fact_customers FOREIGN KEY (customer_id) 
			REFERENCES silver.fact_customers(customer_id) 
			ON DELETE CASCADE 
			ON UPDATE CASCADE,
	);	
	PRINT 'dim_director_information table created successfully';	

	/***********************************************
		Create table indexes
	***********************************************/
	PRINT 'Creating indexes.....';

	-- fact customer table 
	CREATE INDEX IDX_fact_customers_customer_id ON silver.fact_customers(customer_id)

	-- addresses
	CREATE INDEX IDX_dim_addresses_customer_id ON silver.dim_addresses(customer_id);
	CREATE INDEX IDX_dim_addresses_address_type ON silver.dim_addresses(address_type);
	CREATE INDEX IDX_dim_addresses_city_province ON silver.dim_addresses(city, province);
	CREATE UNIQUE INDEX UQ_IDX_dim_addresses_is_current_address ON silver.dim_addresses(customer_id, address_type)
		WHERE is_current_address = 1;

	-- contacts
	CREATE INDEX IDX_dim_contacts_customer_id ON silver.dim_contacts(customer_id);
	CREATE INDEX IDX_dim_contacts_contact_type ON silver.dim_contacts(contact_type);
	CREATE INDEX IDX_dim_contacts_is_primary ON silver.dim_contacts(is_primary_contact);
	CREATE INDEX IDX_dim_contacts_contact ON silver.dim_contacts(contact);

	PRINT 'Indexes created';

END 
