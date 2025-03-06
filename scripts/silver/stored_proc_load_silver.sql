/*
==========================================================================
Silver Layer Data Cleaning and Transformation
==========================================================================

This Script creates Stored Procedure for Silver to Load cleaned and Transformed Data
from the Bronze Layer.

- Check Duplicates and Null Primary Keys:
    - Ensure Only One record per entity by identifying and retaining the most relevant row.
    - Removing Null in Primary Key Column is Essential
- Check for Unwanted Spaces:
    - Removing Unnecessary Spaces to ensure Data Consistency and uniformity across all records
- Data Standardization and Consistency:
    - We Aim to Store clear and meaningful values rather than using abbreviated terms
    - Filling the Blanks by adding a default value
- Derived Columns:
    - Creating New Column based on calculations or transformations of existing ones.
- Handling Nulls or Missing Value
    - Using ISNULL()
- Data Type Casting
- Data Enrichment:
    - Add New , relevant Data to Enhance the Dataset for analysis

--To Execute Stored procedure EXEC silver.load_silver;
*/


CREATE OR ALTER PROCEDURE silver.load_silver as
BEGIN
	DECLARE @start_time DATETIME , @end_time DATETIME , @batch_start_time DATETIME , @batch_end_time DATETIME;
	
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '=============================================';
		PRINT 'Loading Silver Layer';
		PRINT '=============================================';

		PRINT '---------------------------------------------';
		PRINT 'Load CRM Tables Data:';
		PRINT '---------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table : silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Inserting Cleaned and Transformed Data Into : silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info
		(
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date
		)
		SELECT
		cst_id,
		cst_key,
		TRIM(cst_firstname) as cst_firstname,
		TRIM(cst_lastname) as cst_lastname,
		CASE 
			WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
			WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
			ELSE 'N/A' 
		END cst_marital_status,
		CASE 
			WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			ELSE 'N/A' 
		END cst_gndr,
		cst_create_date
		FROM 
		(
			SELECT
			*,
			ROW_NUMBER() OVER( PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		)t 
		WHERE flag_last = 1;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST( DATEDIFF(second , @start_time , @end_time) as NVARCHAR) + ' seconds';
		PRINT'------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table : silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Inserting Cleaned and Transformed Data Into : silver.crm_prd_info' ;
		INSERT INTO silver.crm_prd_info (
			prd_id,		
			cat_id,			
			prd_key,			
			prd_nm,			
			prd_cost,			
			prd_line,			
			prd_start_dt,		
			prd_end_dt
		)
		SELECT
		prd_id,
		REPLACE(SUBSTRING(prd_key,1,5) , '-' , '_') as cat_id,
		SUBSTRING(prd_key , 7 , LEN(prd_key)) as prd_key,
		prd_nm,
		ISNULL(prd_cost,0) as prd_cost,
		CASE 
			WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
			WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
			WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
			WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
			ELSE 'N/A' 
		END prd_line,
		CAST( prd_start_dt as DATE ) as prd_start_dt ,
		CAST ( DATEADD(day,-1 , LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) as DATE ) AS prd_end_dt
		FROM bronze.crm_prd_info;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST( DATEDIFF(second , @start_time , @end_time) as NVARCHAR) + ' seconds';
		PRINT'------------------------------------------';
		
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table : silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting Cleaned and Transformed Data Into : silver.crm_sales_details' ;

		INSERT INTO silver.crm_sales_details (
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		)
		SELECT
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
				ELSE CAST (CAST(sls_order_dt as VARCHAR) as DATE) 
			END as sls_order_dt,
			CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				ELSE CAST (CAST(sls_ship_dt as VARCHAR) as DATE) 
			END as sls_ship_dt,
			CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
				ELSE CAST (CAST(sls_due_dt as VARCHAR) as DATE) 
			END as sls_due_dt,
			CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
					THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
			END as sls_sales,
			sls_quantity,
			CASE WHEN sls_price IS NULL OR sls_price <=0
					THEN sls_sales / NULLIF(sls_quantity,0)
				ELSE sls_price
			END as sls_price
		FROM bronze.crm_sales_details;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST( DATEDIFF(second , @start_time , @end_time) as NVARCHAR) + ' seconds';
		PRINT'------------------------------------------';

		PRINT '---------------------------------------------';
		PRINT 'Load ERP Tables Data:';
		PRINT '---------------------------------------------';
		
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table : silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Inserting Cleaned and Transformed Data Into : silver.erp_cust_az12' ;

		INSERT INTO silver.erp_cust_az12 ( cid , bdate , gen )
		SELECT
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
			ELSE cid
		END as cid,
		CASE WHEN bdate>GETDATE() THEN NULL
			ELSE bdate
		END as bdate,
		CASE WHEN UPPER(TRIM(gen)) IN ('F' , 'FEMALE') THEN 'Female'
			WHEN UPPER(TRIM(gen)) IN ('M' , 'MALE') THEN 'Male'
		ELSE 'N/A' END as gen
		FROM bronze.erp_cust_az12;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST( DATEDIFF(second , @start_time , @end_time) as NVARCHAR) + ' seconds';
		PRINT'------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table : silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Inserting Cleaned and Transformed Data Into : silver.erp_loc_a101' ;
		INSERT INTO silver.erp_loc_a101 ( cid , cntry )
		SELECT
		REPLACE (cid , '-' , '') cid,
		CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany' 
			WHEN TRIM(cntry) IN ('US','USA') THEN 'United States' 
			WHEN TRIM(cntry) = '' OR cntry is NULL THEN 'n/a'
			ELSE TRIM(cntry)
		END as cntry
		FROM bronze.erp_loc_a101;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST( DATEDIFF(second , @start_time , @end_time) as NVARCHAR) + ' seconds';
		PRINT'------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table : silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Inserting Cleaned and Transformed Data Into : silver.erp_px_cat_g1v2' ;

		INSERT INTO silver.erp_px_cat_g1v2 ( id ,cat , subcat, maintenance )
		SELECT
		id,
		cat,
		subcat,
		maintenance
		FROM bronze.erp_px_cat_g1v2;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST( DATEDIFF(second , @start_time , @end_time) as NVARCHAR) + ' seconds';
		PRINT'------------------------------------------';
		SET @batch_end_time = GETDATE();
		PRINT '================================================';
		PRINT '>> Loading Silver Layer is Completed'
		PRINT '>> Total Batch Duration: ' + CAST( DATEDIFF(second , @batch_start_time , @batch_end_time) as NVARCHAR) + ' seconds';
		PRINT '================================================';
	END TRY
	BEGIN CATCH
		PRINT '================================================';
		PRINT 'ERROR LOADING DATA INTO Silver LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Errro Number' + CAST(ERROR_NUMBER() as NVARCHAR);
		PRINT 'Error State' + CAST(ERROR_STATE() as NVARCHAR);
		PRINT '================================================';
	END CATCH
END

