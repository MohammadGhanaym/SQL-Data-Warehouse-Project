-- check the data quality of silver.crm_cust_info

-- Check for Nulls or duplicates in primary key
-- Expectation: No Result
SELECT 
	cst_id,
	COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL


-- Check for unwanted spaces
-- Expectation: No Results
SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)

-- cst_firstname has unwanted spaces
-- cst_lastname has unwanted spaces
-- cst_gndr don't have unwanted spaces

-- Data Standardization & Consistency (cst_gndr, cst_material_status)
SELECT DISTINCT cst_material_status
FROM silver.crm_cust_info


SELECT COUNT(*)
FROM silver.crm_cust_info

--=======================================================================
-- Check the data quality of silver.crm_prd_info

-- Check for Nulls or duplicates in primary key
-- Expectation: No Result
SELECT 
	prd_id,
	COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL


-- Check for unwanted spaces
-- Expectation: No Results
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm) 


-- Check for NULLs or Negative Numbers
-- Expectation: No Results
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL


-- Data Standardization & Consistency (prd_line, cst_material_status)
SELECT DISTINCT prd_line
FROM silver.crm_prd_info

-- Check for Invalid Date Orders
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt

--=======================================================================
-- Check the data quality of silver.crm_sales_details

-- Check for Invalid Date Orders
SELECT 
	*
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

-- Check Data Consistency: Between Sales, Quantity, and Price
-- >> Sales = Quantity * Price
-- >> Values must not be NULL, zero, or negative.
SELECT DISTINCT
	sls_sales,
	sls_quantity,
	sls_price 
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
	  OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
	  OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price

SELECT * FROM silver.crm_sales_details

/*
--------------------------------------------------------------------------------------
-- Project: Data Warehouse - Silver Layer Data Quality Checks
-- Script Name: dq_check_silver.sql
-- Description:
--   This script performs comprehensive Data Quality (DQ) checks on the Silver Layer tables
--   to validate data accuracy, consistency, completeness, and standardization after the ETL process.
--
-- Objectives:
--   • Verify that all primary key columns have no NULLs or duplicates.
--   • Ensure text fields contain no unwanted leading/trailing spaces.
--   • Confirm categorical fields (e.g., gender, marital status, product line) are standardized.
--   • Validate date fields to detect invalid or illogical date orders.
--   • Check numerical fields for negative, NULL, or inconsistent values.
--   • Confirm referential consistency between CRM and ERP datasets.
--
-- Tables Checked:
--   1. silver.crm_cust_info
--   2. silver.crm_prd_info
--   3. silver.crm_sales_details
--   4. silver.erp_cust_az12
--   5. silver.erp_loc_a101
--   6. silver.erp_px_cat_g1v2
--
-- Author: Mohamed Ghanaym
-- Date: 2025-10-10
-- Layer: Silver (Cleaned & Standardized Data)
-- Dependencies: silver.load_silver (ETL Procedure)
-- Expected Results:
--   Each validation query should return no unexpected results 
--   (i.e., no duplicates, NULLs, invalid values, or inconsistencies).
--------------------------------------------------------------------------------------
*/


--=======================================================================
-- Check the data quality of silver.erp_cust_az12

-- Identify Out-of-range dates
SELECT DISTINCT
	bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()

-- Data Standardization & Consistency
 SELECT DISTINCT gen
 FROM silver.erp_cust_az12

 SELECT * FROM silver.erp_cust_az12

 --=======================================================================
 -- Check the data quality of silver.erp_loc_a101

SELECT
	REPLACE(cid, '-', '') cid,
	cntry
FROM silver.erp_loc_a101
WHERE REPLACE(cid, '-', '') NOT IN
(SELECT cst_key FROM silver.crm_cust_info)

-- Data Standardization & Consistency
SELECT DISTINCT cntry
FROM silver.erp_loc_a101
ORDER BY cntry

 --=======================================================================
 -- Check the data quality of silver.erp_px_cat_g1v2

 SELECT * FROM silver.erp_px_cat_g1v2
