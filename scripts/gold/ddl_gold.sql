/*
--------------------------------------------------------------------------------------
-- Project: Data Warehouse - Gold Layer (Dimensional Model)
-- Script Name: create_gold_views.sql
-- Description:
--   This script creates the Gold Layer views representing the dimensional model
--   for analytical and reporting purposes. It builds star-schema components 
--   (dimensions and fact tables) by combining and transforming data from the Silver Layer.
--
-- Objectives:
--   • Create reusable dimension and fact views for BI tools (e.g., Power BI, SSAS).
--   • Apply business-friendly naming conventions and surrogate keys (via ROW_NUMBER()).
--   • Integrate data across CRM and ERP systems for unified analytics.
--
-- Views Created:
--   1. gold.dim_customers
--       - Combines CRM and ERP customer data to form a unified customer dimension.
--       - Includes demographic and geographic attributes.
--
--   2. gold.dim_products
--       - Merges CRM product details with ERP product category data.
--       - Filters active products (prd_end_dt IS NULL).
--
--   3. gold.fact_sales
--       - Links sales transactions to customer and product dimensions.
--       - Provides key performance measures: sales amount, quantity, price.
--
-- Author: Mohamed Ghanaym
-- Date: 2025-10-11
-- Layer: Gold (Presentation & Analysis)
-- Dependencies: Silver Layer tables (silver.*)
-- Output: Gold Layer views (gold.*)
--------------------------------------------------------------------------------------
*/

-- Create the dim_customers dimension
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers
GO
CREATE VIEW gold.dim_customers AS
SELECT
      ROW_NUMBER() OVER(ORDER BY cst_id) customer_key,
      ci.[cst_id] AS customer_id,
      ci.[cst_key] AS customer_number,
      ci.[cst_firstname] AS first_name,
      ci.[cst_lastname] AS last_name,
      la.cntry AS country,
      ci.[cst_material_status] AS marital_status,
      CASE WHEN ci.cst_gndr != 'N/A' THEN ci.cst_gndr
           ELSE COALESCE(ca.gen, 'N/A')
      END AS gender,
      ca.bdate AS birthdate,
      ci.[cst_create_date] AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid
GO

-- Create the dim_products dimension
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products
GO
CREATE VIEW gold.dim_products AS
SELECT
    ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt, pn.prd_key) product_key,
    pn.prd_id AS product_id,
    pn.prd_key AS product_number,
    pn.prd_nm AS product_name,
    pn.cat_id AS category_id,
    pc.cat AS category,
    pc.subcat AS subcategory,
    pc.maintenance,
    pn.prd_cost AS cost,
    pn.prd_line AS product_line,
    pn.prd_start_dt AS start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL -- Filter out all historical data

GO
-- Create the fact_sales dimension
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales
GO
CREATE VIEW gold.fact_sales AS
SELECT
    sd.sls_ord_num AS order_number,
    pr.product_key,
    cu.customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt AS ship_date,
    sd.sls_due_dt AS due_date,
    sd.sls_sales AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
ON sd.sls_cust_id = cu.customer_id
