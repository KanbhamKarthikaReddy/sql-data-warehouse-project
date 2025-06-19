/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.
Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

use Datawarehouse;
--- INSERT DATA--
--- first step to insert data--
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @START_TIME DATETIME, @END_TIME DATETIME, @BATCH_START_TIME DATETIME, @BATCH_END_TIME DATETIME;
	BEGIN TRY
		PRINT '==================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '==================================================';

		PRINT '--------------------------------------------------';
		PRINT 'Loading CRM Tables'
		PRINT '--------------------------------------------------';

		SET @START_TIME = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_info'; 
		TRUNCATE TABLE bronze.crm_cust_info;
		-- SECOND STEP 

		PRINT '>> Inserting Data Into: Bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\kanbh\Desktop\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @END_TIME = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @START_TIME, @END_TIME) AS NVARCHAR) + 'SECONDS';
		PRINT '>> -----------------------';

		PRINT '--------------------------------------------------';
		PRINT 'Loading CRM Tables'
		PRINT '--------------------------------------------------';

		SET @START_TIME = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE BRONZE.CRM_PRD_INFO;
		PRINT '>> Inserting Data Into: Bronze.crm_prd_info';
		BULK INSERT BRONZE.CRM_PRD_INFO
		from 'C:\Users\kanbh\Desktop\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with (
			firstrow=2,
			fieldterminator=',',
			tablock
		);
		SET @END_TIME = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @START_TIME, @END_TIME) AS NVARCHAR) + 'SECONDS';
		PRINT '>> -----------------------';

		PRINT '--------------------------------------------------';
		PRINT 'Loading CRM Tables'
		PRINT '--------------------------------------------------';

		SET @START_TIME = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_sales_details';
		truncate table BRONZE.CRM_SALES_DETAILS;

		PRINT '>> Inserting Data Into: Bronze.crm_sales_details';
		BULK INSERT BRONZE.CRM_SALES_DETAILS
		FROM 'C:\Users\kanbh\Desktop\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW=2,
			fieldterminator=',',
			TABLOCK
		);
		SET @END_TIME = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @START_TIME, @END_TIME) AS NVARCHAR) + 'SECONDS';
		PRINT '>> -----------------------';
		
		
		PRINT '--------------------------------------------------';
		PRINT 'Loading ERP Tables'
		PRINT '--------------------------------------------------';

		SET @START_TIME = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE BRONZE.ERP_CUST_AZ12;

		PRINT '>> Inserting Data Into: Bronze.erp_cust_az12';
		BULK INSERT BRONZE.ERP_CUST_AZ12
		FROM 'C:\Users\kanbh\Desktop\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @END_TIME = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @START_TIME, @END_TIME) AS NVARCHAR) + 'SECONDS';
		PRINT '>> -----------------------';

		PRINT '--------------------------------------------------';
		PRINT 'Loading ERP Tables'
		PRINT '--------------------------------------------------';

		SET @START_TIME = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE BRONZE.ERP_LOC_A101;

		PRINT '>> Inserting Data Into: Bronze.erp_loc_a101';
		BULK INSERT BRONZE.ERP_LOC_A101
		FROM 'C:\Users\kanbh\Desktop\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @END_TIME = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @START_TIME, @END_TIME) AS NVARCHAR) + 'SECONDS';
		PRINT '>> -----------------------';
	

		PRINT '--------------------------------------------------';
		PRINT 'Loading ERP Tables'
		PRINT '--------------------------------------------------';

		SET @START_TIME = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE BRONZE.ERP_PX_CAT_G1V2;

		PRINT '>> Inserting Data Into: Bronze.erp_px_cat_g1v2';
		BULK INSERT BRONZE.ERP_PX_CAT_G1V2
		FROM 'C:\Users\kanbh\Desktop\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @END_TIME = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @START_TIME, @END_TIME) AS NVARCHAR) + 'SECONDS';
		PRINT '>> -----------------------';

		SET @END_TIME = GETDATE();
		PRINT '==========================================================================='
		PRINT 'Loading Bronze Layer is completed';
		PRINT ' -Total Load Duration: '+ Cast(Datediff(second, @batch_start_time, @batch_end_time) as NVARCHAR) + 'Seconds';
		PRINT '==========================================================================='
	END TRY
	BEGIN CATCH
		PRINT '==========================================================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'ERROR Message' + Error_message();
		PRINT 'ERROR MASSAGE' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR MASSAGE' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '==========================================================================='
	END CATCH
END

exec bronze.load_bronze;
