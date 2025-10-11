/********************************************************************************************
-- Script Name: Data Quality and Model Validation Checks
-- Author: Mohamed Ghanaym
-- Description: 
--   This script performs data quality checks and validates referential integrity 
--   between dimension and fact tables in the gold schema.
--
-- Includes:
--   1. Data quality checks for dimension tables (dim_customers, dim_products)
--   2. Data quality checks for fact table (fact_sales)
--   3. Model validation to ensure all foreign keys in fact tables exist in dimensions
--
-- Date: 2025-10-11
-- Database: Data Warehouse (Gold Layer)
********************************************************************************************/


-- Check data quality of dim_customers
SELECT *
FROM gold.dim_customers


SELECT DISTINCT gender
FROM gold.dim_customers

SELECT customer_key, COUNT(*) duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1

-- Check data quality of dim_products
SELECT product_key, COUNT(*) duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1

-- Check data quality of fact_sales
SELECT * FROM gold.fact_sales

-- validating the data model

SELECT *
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE p.product_key IS NULL OR c.customer_key IS NULL
