/*
-------------------------------------------------------------------------------
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
-------------------------------------------------------------------------------
*/

DROP VIEW IF EXISTS gold.dim_customers
;
GO

CREATE VIEW gold.dim_customers as
SELECT 
	ROW_NUMBER() OVER(ORDER BY t1.cst_id) as customer_key,
	t1.cst_id as customer_id,
	t1.cst_key as customer_number,
	t1.cst_firstname as first_name,
	t1.cst_lastname as last_name,
	t3.cntry as country,
	t1.cst_marital_status as marital_status,
	CASE
		WHEN cst_gndr != 'NA' THEN cst_gndr
		ELSE COALESCE(gen,'NA')
	END as gender,
	t2.bdate as birth_date,
	t1.cst_create_date as create_date
	FROM silver.crm_cust_info as t1
	LEFT JOIN silver.erp_cust_az12 as t2
		ON t1.cst_key = t2.cid
	LEFT JOIN silver.erp_loc_a101 as t3
		ON t1.cst_key = t3.cid
;
GO
--------------------------------------------------------------------
DROP VIEW IF EXISTS gold.dim_products
;
GO
  
CREATE VIEW gold.dim_products as
SELECT 
ROW_NUMBER() OVER(ORDER BY t1.prd_start_dt, t1.prd_key) as product_key,
t1.prd_id as product_id,
t1.prd_key as product_number,
t1.prd_nm as product_name,
t1.cat_id as category_id,
t2.cat as category,
t2.subcat as subcategory,
t2.maintenance,
t1.prd_cost as product_cost,
t1.prd_line as product_line,
t1.prd_start_dt as start_date
FROM silver.crm_prd_info as t1
LEFT JOIN silver.erp_px_cat_g1v2 as t2
	ON t1.cat_id = t2.id
WHERE prd_end_dt IS NULL
;
GO
---------------------------------------------------------------------
DROP VIEW IF EXISTS gold.fact_sales
;
GO
  
CREATE VIEW gold.fact_sales as 
SELECT 
t1.sls_ord_num as order_number,
t2.customer_key,
t3.product_key,
t1.sls_order_dt as order_date,
t1.sls_ship_dt as shipping_date,
t1.sls_due_dt as due_date,
t1.sls_sales as sales_amount,
t1.sls_quantity as quantity,
t1.sls_price as price
FROM silver.crm_sales_details as t1
LEFT JOIN gold.dim_customers as t2
	ON t1.sls_cust_id = t2.customer_id
LEFT JOIN gold.dim_products as t3
	ON t1.sls_prd_key = t3.product_number
;
GO
