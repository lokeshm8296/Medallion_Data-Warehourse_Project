/*
========================================================
Stored Procedure to Load Bronze Layer Data
========================================================

Creating Stored Procedure for Load Data on Regular Basis:
This scripts loads crm and erp sources data into tables
# Method of Loading Data:

- **We are using Bulk Insert.**
    - Single Load of Data into a Table
- Full Load : Truncate Whole Table and then Load

# Creating a Stored Procedure for Bronze Layer:

- Adding TRY and CATCH for Error Handling
- Track ETL Duration
    - Helps identify Bottlenecks , Optimize Performance , Monitor Trends and Detect issues
# To Use the Store Procedure:Excecute 
EXEC bronze.load_bronze;
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze as

BEGIN
	DECLARE @start_time DATETIME , @end_time DATETIME;
	BEGIN TRY
		SET @start_time = GETDATE();
		PRINT '=============================================';
		PRINT 'Loading Bronze Layer';
		PRINT '=============================================';

		PRINT '---------------------------------------------';
		PRINT 'Load CRM Tables Data:';
		PRINT '---------------------------------------------';
		
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>> Inserting Data Into : bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\lokes\OneDrive\Desktop\Advance SQL\Data Warehouse Project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST( DATEDIFF(second , @start_time , @end_time) as NVARCHAR) + ' seconds';
		PRINT'------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>> Inserting Data Into : bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\lokes\OneDrive\Desktop\Advance SQL\Data Warehouse Project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST( DATEDIFF(second , @start_time , @end_time) as NVARCHAR) + ' seconds';
		PRINT'------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>> Inserting Data Into : bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\lokes\OneDrive\Desktop\Advance SQL\Data Warehouse Project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST( DATEDIFF(second , @start_time , @end_time) as NVARCHAR) + ' seconds';


		PRINT '---------------------------------------------';
		PRINT 'Load ERP Tables Data:';
		PRINT '---------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '>> Inserting Data Into : bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\lokes\OneDrive\Desktop\Advance SQL\Data Warehouse Project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST( DATEDIFF(second , @start_time , @end_time) as NVARCHAR) + ' seconds';
		PRINT'------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '>> Inserting Data Into : bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\lokes\OneDrive\Desktop\Advance SQL\Data Warehouse Project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST( DATEDIFF(second , @start_time , @end_time) as NVARCHAR) + ' seconds';
		PRINT'------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>> Inserting Data Into : bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\lokes\OneDrive\Desktop\Advance SQL\Data Warehouse Project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST( DATEDIFF(second , @start_time , @end_time) as NVARCHAR) + ' seconds';
		PRINT'------------------------------------------';

		PRINT '================================================';
		PRINT '>> Total Batch Duration: ' + CAST( DATEDIFF(second , @start_time , @end_time) as NVARCHAR) + ' seconds';
		PRINT '================================================';
	END TRY
	BEGIN CATCH
		PRINT '================================================';
		PRINT 'ERROR LOADING DATA INTO BRONZE LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Errro Number' + CAST(ERROR_NUMBER() as NVARCHAR);
		PRINT 'Error State' + CAST(ERROR_STATE() as NVARCHAR);
		PRINT '================================================';
	END CATCH
	SET @end_time = GETDATE();
END 
