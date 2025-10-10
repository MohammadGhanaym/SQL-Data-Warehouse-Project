-- Check the data quality of bronze.crm_cust_info

-- Check for Nulls or duplicates in primary key
-- Expectation: No Result
SELECT 
	cst_id,
	COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL


-- Check for unwanted spaces
-- Expectation: No Results
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

-- cst_firstname has unwanted spaces
-- cst_lastname has unwanted spaces
-- cst_gndr don't have unwanted spaces

-- Data Standardization & Consistency (cst_gndr, cst_material_status)
SELECT DISTINCT cst_material_status
FROM bronze.crm_cust_info

--=======================================================================
-- Check the data quality of bronze.crm_prd_info

-- Check for Nulls or duplicates in primary key
-- Expectation: No Result
SELECT 
	prd_id,
	COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL


-- Check for unwanted spaces
-- Expectation: No Results
SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm) 


-- Check for NULLs or Negative Numbers
-- Expectation: No Results
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL


-- Data Standardization & Consistency (prd_line, cst_material_status)
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info

-- Check for Invalid Date Orders
SELECT *
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt
--=======================================================================
-- Check the data quality of bronze.crm_sales_details
-- Checking for Unwanted Spaces
SELECT
	sls_ord_num,
	sls_prd_key,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
FROM bronze.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num)

-- Checking Key Integrity (sls_prd_key, sls_cust_id)
SELECT
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info)

-- Transforming Integer Dates to Date Type (sls_order_dt, sls_ship_dt, sls_due_dt)
-- First, check for invalid dates
SELECT
	NULLIF(sls_order_dt, 0) sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 OR LEN(sls_order_dt) != 8

-- Check for Invalid Date Orders
SELECT 
	*
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

-- Check Data Consistency: Between Sales, Quantity, and Price
-- >> Sales = Quantity * Price
-- >> Values must not be NULL, zero, or negative.
SELECT DISTINCT
	sls_sales AS old_sales,
	sls_quantity,
	sls_price AS old_prices,
	CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
		 THEN sls_quantity * ABS(sls_price)
		 ELSE sls_sales 
	END sls_sales,
	CASE WHEN sls_price IS NULL OR sls_price <= 0 
		 THEN sls_sales / NULLIF(sls_quantity, 0)
		 ELSE sls_price
	END sls_price 
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
	  OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
	  OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price

--=======================================================================
-- Check the data quality of bronze.erp_cust_az12

SELECT
	cid,
	bdate, 
	gen
FROM bronze.erp_cust_az12


SELECT * FROM [silver].[crm_cust_info]


SELECT
	CASE WHEN cid LIKE 'NAS%' 
		 THEN SUBSTRING(cid, 4, LEN(cid))
		 ELSE cid 
	END cid,
	bdate, 
	gen
FROM bronze.erp_cust_az12
WHERE CASE WHEN cid LIKE 'NAS%' 
		 THEN SUBSTRING(cid, 4, LEN(cid))
		 ELSE cid 
	END NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info)


-- Identify Out-of-range dates
SELECT DISTINCT
	bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()

-- Data Standardization & Consistency
 SELECT DISTINCT gen,
 CASE WHEN UPPER(TRIM(gen)) IN ('F', 'Female') THEN 'Female'
	  WHEN UPPER(TRIM(gen)) IN ('M', 'Male') THEN 'Male'
	  ELSE 'N/A'
 END AS gen
 FROM bronze.erp_cust_az12

 --=======================================================================
-- Check the data quality of bronze.erp_loc_a101

SELECT
	REPLACE(cid, '-', '') cid,
	cntry
FROM bronze.erp_loc_a101
WHERE REPLACE(cid, '-', '') NOT IN
(SELECT cst_key FROM silver.crm_cust_info)

-- Data Standardization & Consistency
SELECT DISTINCT cntry old_cntry,
	CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
		 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'N/A'
		 ELSE TRIM(cntry)
	END cntry
FROM bronze.erp_loc_a101
ORDER BY cntry

 --=======================================================================
-- Check the data quality of bronze.erp_px_cat_g1v2

SELECT 
	id,
	cat,
	subcat,
	maintenance
FROM bronze.erp_px_cat_g1v2

-- Check unwanted spaces
SELECT * FROM bronze.erp_px_cat_g1v2
WHERE maintenance != TRIM(maintenance)

-- Data Standardization & Consistency
SELECT DISTINCT maintenance 
FROM bronze.erp_px_cat_g1v2





