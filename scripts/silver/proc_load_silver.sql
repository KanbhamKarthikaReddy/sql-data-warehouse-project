/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
Use Datawarehouse;

Create or Alter procedure silver.load_silver as
Begin
	Declare @start_time Datetime, @end_time Datetime, @batch_start_time Datetime, @batch_end_time Datetime;
	Begin Try
		Set @batch_start_time = Getdate();
		Print '===================================================';
		Print 'Loading Silver Layer';
		Print '===================================================';

		Print '===================================================';
		Print 'Loading CRM Tables';
		Print '===================================================';

		-- Loding silver.crm_cust_info--
		Set @start_time = Getdate();
		print '>> Truncating table: silver.crm_curt_info';
		truncate table silver.crm_cust_info;
		print '>> Inserting Data into: silver.crm_curt_info';

		Insert into silver.crm_cust_info (
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_Marital_status,
			cst_gndr,
			cst_create_date)

		SELECT 
		cst_id,
		cst_key,
		TRIM(cst_firstname) as cst_firstname,
		TRIM(cst_lastname) as cst_lastname,
		case when upper(trim(cst_material_status)) = 'M' then 'Married'
			 when upper(trim(cst_material_status)) = 'S' then 'Single'
			 else 'n/a'
		end cst_material_status,
		case when upper(trim(cst_gndr)) = 'F' Then 'Female'
			 when upper(trim(cst_gndr)) = 'M' Then 'Male'
			 else 'n/a'
		end cst_gndr,
		cst_create_data
		from (
			select
			*,
			ROW_NUMBER() over (partition by cst_id order by cst_create_data desc) as flag_last 
			from bronze.crm_cust_info
			where cst_id is not null
		)t 
		where flag_last = 1;
		set @end_time = Getdate();
		Print '>> Load Duration: ' + cast(Datediff(second, @start_time, @end_time) as Nvarchar) + 'second';
		Print '>>--------------------';
		---------------------------
		-- Loading silver.crm_prd_info
		set @start_time = Getdate();
		print '>> Truncating table: silver.crm_prd_info';
		truncate table silver.crm_prd_info;
		print '>> Inserting Data into: silver.crm_prd_info';
		insert into silver.crm_prd_info(
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
		select 
		prd_id,
		replace(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
		substring(prd_key,7,len(prd_key)) as prd_key,
		prd_nm,
		isnull(prd_cost,0) as prd_cost,
		case upper(trim(prd_line))
			 when 'M' then 'Mountain'
			 when 'R' then 'Road'
			 when 'S' then 'Other Sales'
			 when 'T' then 'Touring'
			 else 'n/a'
		end as prd_line,
		cast (prd_start_dt as date) as prd_start_dt,
		cast(lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)-1 as date) as prd_end_dt
		from bronze.crm_prd_info;
		set @end_time= Getdate();
		print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as Nvarchar) + 'second';
		print '>> -------------------';
		-----------------------------
		-- Loading silver.crm_sales_details
		set @start_time = Getdate();
		print '>> Truncating table: silver.crm_sales_details';
		truncate table silver.crm_sales_details;
		print '>> Inserting Data into: silver.crm_sales_details';
		insert into silver.crm_sales_details(
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
		select 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		case when sls_order_dt = 0 or len(sls_order_dt) !=8 then null
			 else cast(cast(sls_order_dt as varchar) as date)
		end as sls_order_dt,
		case when sls_ship_dt = 0 or len(sls_ship_dt) !=8 then null
			 else cast(cast(sls_ship_dt as varchar) as date)
		end as sls_ship_dt,
		case when sls_due_dt = 0 or len(sls_due_dt) !=8 then null
			 else cast(cast(sls_due_dt as varchar) as date)
		end as sls_due_dt,
		case when sls_price is null or sls_sales <=0 or sls_sales != sls_quantity * abs(sls_price)
				then sls_quantity * abs(sls_price)
			else sls_sales
		end as sls_sales, -- Recalculate sales if original value is missing or incorrect--
		sls_quantity,
		case when sls_price is null or sls_price <=0
				then sls_sales / nullif(sls_quantity,0)
			else sls_price --- Derive price if original value is invalid
		end as sls_price
		from bronze.crm_sales_details;
		set @end_time = Getdate();
		Print '>> Load Duration: ' + Cast(datediff(second, @start_time, @end_time) as Nvarchar) +'second';
		print '-------------------';
		----------------------------------------------------
						-- ERP Tables --
		----------------------------------------------------
		Print '===================================================';
		Print 'Loading ERP Tables';
		Print '===================================================';
		--- Loading silver.erp_cust_az12--
		set @start_time = Getdate();
		print '>> Truncating table: silver.erp_cust_az12';
		truncate table silver.erp_cust_az12;
		print '>> Inserting Data into: silver.erp_cust_az12';
		insert into silver.erp_cust_az12 (
				cid,
				bdate,
				gen
		)
		select 
		case when cid like 'NAS%' then SUBSTRING(cid, 4, len(cid))
			 else cid  -- remove "NAS" prefix if present
		end as cid,
		case when bdate > getdate() then null
			 else bdate
		end as bdate,  --- set future dirthdates to Null
		case when upper(trim(gen)) in ('F', 'FEMALE') THEN 'Female'
			 when upper(trim(gen)) in ('M', 'MALE') THEN 'Male'
			 else 'n/a'
		end as gen  --- Normalize gender values and handle unknow cases
		from bronze.erp_cust_az12;
		set @end_time = Getdate();
		Print '>> Load Duration: ' + Cast(Datediff(second, @start_time, @end_time) as Nvarchar) + 'second';
		Print '>>-------------------';
		--------------------------
		--- Loading silver.erp_loc_a101--
		set @start_time= Getdate();
		print '>> Truncating table: silver.erp_loc_a101';
		truncate table silver.erp_loc_a101;
		print '>> Inserting Data into: silver.erp_loc_a101';

		insert into silver.erp_loc_a101(
				cid,
				cntry
		)
		select 
		replace(cid,'-','') as cid,
		case when trim(cntry) = 'DE' then 'Germany'
			 when trim(cntry) in ('US','USA') then 'United States'
			 when trim(cntry) = '' or cntry is null then 'n/a'
			 else trim (cntry)
		end as cntry  --- Normalize and Handle missing or blank country codes
		from bronze.erp_loc_a101;
		set @end_time= Getdate();
		print '>> Load Duartion: ' + cast(datediff(second, @start_time, @end_time) as Nvarchar) + 'second';
		Print '>>-----------------------';
		--------------------------------
		-- Loading silver.erp_px_cat_g1v2--
		set @start_time = Getdate();
		print '>> Truncating table: silver.erp_px_cat_g1v2';
		truncate table silver.erp_px_cat_g1v2;
		print '>> Inserting Data into: silver.erp_px_cat_g1v2';
		insert into silver.erp_px_cat_g1v2(
				id,
				cat,
				subcat,
				maintenance
		)
		select
		id,
		cat,
		subcat,
		maintenance
		from bronze.erp_px_cat_g1v2;
		set @end_time = Getdate();
		print '>> Load Duartion: ' + cast(datediff(second, @start_time, @end_time) as Nvarchar) + 'second';
		print '>>--------------------------';

		Set @batch_end_time = GetDate();
		Print '=================================================='
		Print 'Loading Silver Layer is Completed'
		Print ' -Total Load Duration: ' + Cast(datediff(second, @batch_start_time, @batch_end_time) as Nvarchar) + 'second';
		Print '=================================================='
	End try
	Begin Catch
		Print '=================================================='
		Print 'Error Occured During Loading Silver Layer'
		Print 'Error Message' + Error_message();
		Print 'Error Message' + Cast (Error_Number() as Nvarchar);
		Print 'Error Message' + Cast (Error_State() as Nvarchar);
	End Catch
End
-----------------------------------------------------------

Exec Silver.load_silver;
