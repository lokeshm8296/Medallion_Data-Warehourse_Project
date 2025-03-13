/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/
--Dimension Table View for Customers
CREATE VIEW gold.dim_customers AS
SELECT
	ROW_NUMBER() OVER(ORDER BY ci.cst_id) as customer_key,
	ci.cst_id as customer_id,
	ci.cst_key as customer_number,
	ci.cst_firstname as firstname,
	ci.cst_lastname as lastname,
	el.cntry as country,
	ci.cst_marital_status as martial_status,
	ec.bdate as birthdate,
	CASE 
		WHEN ci.cst_gndr != 'N/A' then ci.cst_gndr  -- CRM is the Master for gender information
		ELSE COALESCE(ec.gen , 'N/A')
	END as gender,
	ci.cst_create_date as created_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ec
ON ec.cid = ci.cst_key
LEFT JOIN silver.erp_loc_a101 el
ON el.cid = ci.cst_key

-- Dimension Table View for Products
CREATE VIEW gold.dim_products AS
SELECT
	ROW_NUMBER() OVER(ORDER BY prd_start_dt , cp.prd_key ) as product_key,
	cp.prd_id as product_id,
	cp.prd_key as product_number,	
	cp.prd_nm as product_name,
	cp.cat_id as category_id,				
	ep.cat as category,
	ep.subcat as subcategory,
	ep.maintenance,
	cp.prd_cost as cost,			
	cp.prd_line as product_line,			
	cp.prd_start_dt as start_date	
FROM silver.crm_prd_info cp
LEFT JOIN silver.erp_px_cat_g1v2 ep
ON ep.id = cp.cat_id
WHERE
cp.prd_end_dt IS NULL -- Filter out Historical Data

-- Fact Table View for Sales Details
CREATE VIEW gold.fct_sales AS
SELECT
	sd.sls_ord_num as order_number,
	dp.product_key,
	dc.customer_key,
	sd.sls_order_dt as order_date,
	sd.sls_ship_dt as ship_date,
	sd.sls_due_dt as due_date,
	sd.sls_sales as sales,
	sd.sls_quantity as quantity,
	sd.sls_price as price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_customers dc
ON sd.sls_cust_id = dc.customer_id
LEFT JOIN gold.dim_products dp
ON sd.sls_prd_key = dp.product_number

