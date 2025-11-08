/*************************************************************************************
Script		: pcd_insert_into_customer_normalized_tables.sql
Purpose		: ETL procedure to load data into normalized customers tables 
Author		: Innocent Nhamo
Created On	: 2025-11-07
Version		: 
			  v.1 - Initial creation 
**************************************************************************************/
USE banking_db;
GO

-- Procedure 
CREATE OR ALTER PROCEDURE silver.pcd_insert_into_customer_normalized_tables
AS
BEGIN
	SET NOCOUNT ON;

	/***********************************************
		Insert into the fact table
	***********************************************/
	PRINT 'Loading data into the fact_customer table...';
	INSERT INTO silver.fact_customers (
		customer_id, customer_type, customer_name, original_date_of_birth, fixed_date_of_birth,
		original_age_at_opening, fixed_age_at_opening, citizenship, nationality, is_dual_or_transfered, 
		gender ,ethnicity, preferred_contact_method, customer_registration_date, age_group
	)
	SELECT DISTINCT
		customer_id, 
		customer_type, 
		full_name AS customer_name, 
		CASE 
			WHEN customer_type = 'COMPANY' THEN date_of_incorporation
			ELSE date_of_birth
		END AS original_birth_date ,
		CASE 
			WHEN customer_type = 'COMPANY' THEN date_of_incorporation
			ELSE new_date_of_birth
		END AS fixed_birth_date, 
		CASE 
			WHEN customer_type = 'COMPANY' THEN company_age
			ELSE age_at_opening
		END AS original_age_at_opening, 
		CASE 
			WHEN customer_type = 'COMPANY' THEN company_age
			ELSE new_age_at_opening
		END AS  fixed_age_at_opening, 
		citizenship, 
		nationality,
		CASE 
			WHEN citizenship = 'ZA' AND nationality != 'SOUTH AFRICA' THEN 1
			ELSE 0
		END AS is_dual_or_transfered, 
		CASE 
			WHEN customer_type = 'COMPANY' THEN 'NOT APPLICABLE'
			ELSE gender
		END AS gender,
		CASE 
			WHEN customer_type = 'COMPANY' THEN 'NOT APPLICABLE'
			ELSE ethnicity
		END AS ethnicity, 
		CASE 
			WHEN 
				(preferred_contact_method = 'EMAIL' AND is_invalid_email_format = 1)
				OR preferred_contact_method IS NULL
			THEN 'UNKNOWN'
			ELSE preferred_contact_method
		END AS preferred_contact_method, 
		customer_registration_date, 
		CASE 
			WHEN CUSTOMER_TYPE = 'INDIVIDUAL' THEN
				CASE 
					WHEN new_age_at_opening < 18 THEN NULL 
					WHEN new_age_at_opening <= 24 THEN 'YOUNG ADULT'
					WHEN new_age_at_opening <= 34 THEN 'EARLY CAREER'
					WHEN new_age_at_opening <= 44 THEN 'ESTABLISHED PROFESSIONAL'
					WHEN new_age_at_opening <= 54 THEN 'MID-CAREER'
					WHEN new_age_at_opening <= 64 THEN 'PRE-RETIREMENT'
					ELSE 'RETIREES'
				END
			WHEN CUSTOMER_TYPE = 'COMPANY' THEN
				CASE 
					WHEN company_age < 1 THEN 'STARTUP'
					WHEN company_age = 1 THEN '1 YEAR OLD'
					WHEN company_age = 2 THEN '2 YEARS OLD'
					WHEN company_age BETWEEN 3 AND 5 THEN '3-5 YEARS OLD'
					WHEN company_age BETWEEN 6 AND 10 THEN '6-10 YEARS OLD'
					WHEN company_age BETWEEN 11 AND 20 THEN '11-20 YEARS OLD'
					WHEN company_age BETWEEN 21 AND 50 THEN '21-50 YEARS OLD'
					WHEN company_age > 50 THEN 'OVER 50 YEARS OLD'
					ELSE NULL
				END
			ELSE NULL
		END AS age_group
	FROM #stg_customers
	PRINT 'Address table customer data loading complete';

	-- ADDRESS TABLE
	PRINT 'Loading data into the address table...'
	INSERT INTO silver.dim_addresses (
		customer_id, address_type, street_name, city, province, 
		country, is_primary_address, effective_date
	)
	-- RESIDENTIAL ADDRESS (for all customers who provided it)
	SELECT DISTINCT
		customer_id, 
		'RESIDENTIAL' AS address_type, 
		residential_street_name AS street_name, 
		residential_city AS city, 
		residential_province AS province, 
		residential_country AS country, 
		CASE 
			WHEN customer_type = 'INDIVIDUAL' THEN 1 
			ELSE 0 
		END AS is_primary_address,
		customer_registration_date AS effective_date
	FROM #stg_customers
	WHERE residential_street_name IS NOT NULL

	UNION ALL 

	-- COMMERCIAL ADDRESS (for all customers who provided it)
	SELECT DISTINCT
		customer_id, 
		'COMMERCIAL' AS address_type, 
		commercial_street_name AS street_name, 
		commercial_city AS city, 
		commercial_province AS province, 
		commercial_country AS country, 
		CASE 
			WHEN customer_type = 'COMPANY' THEN 1 
			WHEN customer_type = 'INDIVIDUAL' AND residential_street_name IS NULL THEN 1
			ELSE 0 
		END AS is_primary_address,
		customer_registration_date AS effective_date
	FROM #stg_customers
	WHERE commercial_street_name IS NOT NULL

	UNION ALL 

	-- PRINCIPAL PLACE OF BUSINESS (only if different from existing addresses)
	SELECT DISTINCT
		customer_id, 
		'PRINCIPAL' AS address_type, 
		principal_street_name AS street_name, 
		principal_city AS city, 
		principal_province AS province, 
		principal_country AS country, 
		0 AS is_primary_address, -- Secondary address
		customer_registration_date AS effective_date
	FROM #stg_customers
	WHERE principal_street_name IS NOT NULL
		AND NOT (
			(commercial_street_name = principal_street_name AND commercial_city = principal_city) OR
			(residential_street_name = principal_street_name AND residential_city = principal_city)
		)

	UNION ALL 

	-- REGISTERED OFFICE ADDRESS (only if different from existing addresses)
	SELECT DISTINCT
		customer_id, 
		'OFFICE' AS address_type, 
		office_street_name AS street_name, 
		office_city AS city, 
		office_province AS province, 
		office_country AS country, 
		0 AS is_primary_address, -- Secondary address
		customer_registration_date AS effective_date
	FROM #stg_customers
	WHERE office_street_name IS NOT NULL
		AND NOT (
			(commercial_street_name = office_street_name AND commercial_city = office_city) OR
			(residential_street_name = office_street_name AND residential_city = office_city) OR
			(principal_street_name = office_street_name AND principal_city = office_city)
		);

	-- CONTACTS TABLE 
	PRINT 'Loading data into the contact table...'
	INSERT INTO silver.dim_contacts (
		customer_id, contact_type, contact, contact_status, 
		is_primary_contact
	)
	-- RESIDENTIAL ADDRESS (for all customers who provided it)
	SELECT DISTINCT
		customer_id, 
		'PHONE' AS contact_type,
		new_phone_number AS contact, 
		'ACTIVE' AS contact_status,
		CASE 
			WHEN preferred_contact_method IN ('PHONE', 'SMS') THEN 1 
			ELSE 0 
		END AS is_primary_contact
	FROM #stg_customers
	WHERE new_phone_number IS NOT NULL

	UNION ALL 

	SELECT DISTINCT
		customer_id, 
		'EMAIL' AS contact_type,
		email AS contact, 
		CASE
			WHEN is_invalid_email_format = 1 THEN 'INVALID'
			ELSE 'ACTIVE' 
		END AS contact_status,
		CASE 
			WHEN preferred_contact_method = 'EMAIL' AND is_invalid_email_format != 1 THEN 1
			ELSE 0 
		END AS is_primary_contact
	FROM #stg_customers
	WHERE email IS NOT NULL

	UNION ALL 

	SELECT DISTINCT
		customer_id, 
		'NEXT OF KIN' AS contact_type,
		next_of_kin AS contact, 
		'ACTIVE' AS contact_status,
		0 AS is_primary_contact
	FROM #stg_customers
	WHERE next_of_kin IS NOT NULL;
	PRINT 'Address table customer data loading complete';

	-- CUSTOMER DOCUMENTS 
	PRINT 'Loading customer data into documents table.....';
	INSERT INTO silver.dim_documents (
		customer_id, document_type, document_number, document_status,
		expiry_review_required, document_expiry_date
	)
	SELECT DISTINCT
		customer_id, 
		'NATIONAL ID' AS document_type, 
		document_number, 
		'VERIFIED' AS document_status,
		0 AS expiry_review_required, 
		NULL document_expiry_date
	FROM #stg_customers
	WHERE document_type = 'NATIONAL ID'

	UNION ALL 

	SELECT DISTINCT
		customer_id, 
		'PASSPORT' AS document_type, 
		document_number,
		CASE WHEN registered_expired_document = 1 THEN 'EXPIRED' ELSE 'VERIFIED' END AS document_status,
		CASE WHEN registered_expired_document = 1 THEN 1 ELSE 0 END AS expiry_review_required, 
		document_expiry_date document_expiry_date
	FROM #stg_customers
	WHERE document_type = 'PASSPORT'

	UNION ALL

	SELECT DISTINCT
		customer_id, 
		'REGISTRATION NUMBER' AS document_type, 
		document_number,
		'VERIFIED' AS document_status,
		0 AS expiry_review_required, 
		NULL document_expiry_date
	FROM #stg_customers
	WHERE document_type = 'REGISTRATION NUMBER'

	UNION ALL

	SELECT DISTINCT
		customer_id, 
		CONCAT('VISA', ' - ', UPPER(visa_type)) AS document_type, 
		document_number,
		CASE WHEN registered_expired_visa = 1 THEN 'EXPIRED' ELSE 'VERIFIED' END AS document_status,
		CASE WHEN registered_expired_visa = 1 THEN 1 ELSE 0 END AS expiry_review_required, 
		visa_expiry_date AS document_expiry_date
	FROM #stg_customers
	WHERE visa_type IS NOT NULL

	PRINT 'Documents table customer data loading complete';

	-- CUSTOMER RISKS 
	PRINT 'Loading customer data into risk table.....';
	INSERT INTO silver.dim_customer_risks (
		customer_id, is_pep, pep_type, is_sanctioned_country, 
		is_director, is_supervised_aml_cft, risk_score, risk_level
	)
	SELECT DISTINCT
		customer_id, 
		is_pep, 
		pep_type, 
		is_sanctioned_country, 
		is_director, 
		is_supervised_aml_cft, 
		risk_score, 
		CASE 
			WHEN risk_score < 0.3 THEN 'LOW'
			WHEN risk_score BETWEEN 0.3 AND 0.5 THEN 'MEDIUM'
			ELSE 'HIGH'
		END AS risk_level
	FROM #stg_customers;
	PRINT 'Risk table customer data loading complete';

	-- CUSTOMER PROFILE 
	PRINT 'Loading customer data into profile table.....';
	INSERT INTO silver.dim_customer_profile(
		customer_id, occupation, employer_name, source_of_funds, 
		marital_status, education_level, annual_expected_income
	)
	SELECT DISTINCT
		customer_id, 
		occupation, 
		employer_name, 
		source_of_funds, 
		marital_status, 
		education_level, 
		annual_income AS annual_expected_income
	FROM #stg_customers
	WHERE customer_type = 'INDIVIDUAL'
	
	PRINT 'Profile table customer data loading complete';

	-- BUSINESS PROFILE
	PRINT 'Loading customer data into business details table.....';
	INSERT INTO silver.dim_business_details(
		customer_id, trading_name, website, has_sanctioned_relationship,
		fatca_giin, swift_bic_code, industry, date_of_incorporation, city_of_incorporation, 
		country_of_incorporation, number_of_employees, director_count, annual_turnover,
		shareholder_count, bee_level, vat_registered, industry_risk_level, is_public,
		external_auditor, legal_representative_name, contact_person, contact_person_title, contact_person_phone,
		contact_person_email, contact_person_customer_id, num_branches_local, num_branches_foreign,
		num_subsidiaries_local, num_subsidiaries_foreign, age_related_risk, is_license_required
	)
	SELECT DISTINCT
		customer_id,
		company_trading_name AS trading_name,
		website,
		has_sanctioned_relationship,
		fatca_giin,
		swift_bic_code,
		industry,
		date_of_incorporation,
		city_of_incorporation,
		country_of_incorporation,
		number_of_employees,
		directors_count AS director_count,
		annual_turnover,
		shareholders_count AS shareholder_count,
		bee_level,
		vat_registered,
		industry_risk_rating AS industry_risk_level,
		is_publicly_listed AS is_public,
		external_auditor,
		legal_representative_name,
		contact_person_name AS contact_person,
		contact_person_title,
		contact_person_phone,
		contact_person_email,
		contact_person_customer_id,
		num_branches_local,
		num_branches_foreign,
		num_subsidiaries_local,
		num_subsidiaries_foreign,
		CASE 
			WHEN company_age < 2 THEN 'HIGH'
			WHEN company_age BETWEEN 2 AND 5 THEN 'MEDIUM'
			ELSE 'LOW'
		END AS age_related_risk,
		requires_license AS is_license_required
	FROM #stg_customers
	WHERE customer_type = 'COMPANY'
	
	PRINT 'Business details table customer data loading complete';

	-- BUSINESS LICENSING
	PRINT 'Loading customer data into business licensing table.....';
	INSERT INTO silver.dim_business_licensing(
		customer_id, license_number, license_issue_date, license_expiry_date, 
		license_regulatory_body
	)
	SELECT DISTINCT
		customer_id, 
		license_number, 
		license_issue_date, 
		license_expiry_date, 
		license_regulatory_body
	FROM #stg_customers
	WHERE license_number IS NOT NULL
	
	PRINT 'Business licensing table customer data loading complete';

	-- BUSINESS SHAREHOLDERS
	PRINT 'Loading customer data into business shareholders table.....';
	INSERT INTO silver.dim_business_shareholders (
		customer_id, shareholder_id, shareholder_name, ownership_percentage
	)
	SELECT DISTINCT
		customer_id, 
		shareholder_id, 
		shareholder_name, 
		TRY_CAST(ownership_percentage AS NUMERIC) AS ownership_percentage
	FROM #stg_customers
	WHERE shareholder_name IS NOT NULL
	
	PRINT 'Business shareholders table customer data loading complete';

	-- BUSINESS DIRECTORS
	PRINT 'Loading customer data into business directors table.....';
	INSERT INTO silver.dim_business_directors (
		customer_id, director_customer_id
	)
	SELECT DISTINCT
		customer_id, 
		director_customer_id
	FROM #stg_customers
	WHERE director_customer_id IS NOT NULL
	
	PRINT 'Business directors table customer data loading complete';

	-- BUSINESS INFORMATION
	PRINT 'Loading customer data into business director information table.....';
	INSERT INTO silver.dim_director_information (
		customer_id, directed_company
	)
	SELECT DISTINCT
		customer_id, 
		directed_company
	FROM #stg_customers
	WHERE directed_company IS NOT NULL
	
	PRINT 'Business director information table customer data loading complete';
END; 
