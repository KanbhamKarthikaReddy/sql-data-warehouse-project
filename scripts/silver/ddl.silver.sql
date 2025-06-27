/*
=============================================================================================
DDL Script: Create Silver Tables
=============================================================================================
Script Puspose:
  This script creates tables in the 'silver' schema, dropping existing tables
  if they already exits.
      Run this script to re-define the DDL struvture of 'Bronze' Tables
=============================================================================================
*/

		print '>> Drop Table for silver.crm_cust_info';
		if object_id ('silver.crm_cust_info','U') is not null
			drop table silver.crm_cust_info;
		print '>> Create Table for silver.crm_cust_info';
		Go
		Create table silver.crm_cust_info(
			cst_id int,
			cst_key Nvarchar(50),
			cst_firstname Nvarchar(50),
			cst_lastname Nvarchar(50),
			cst_Marital_status Nvarchar(50),
			cst_gndr Nvarchar(50),
			cst_create_date date,
			dwh_create_date Datetime2 default getdate()
			);
			
		Go
		print '>> Drop Table for silver.crm_prd_info';
		IF OBJECT_ID('silver.crm_prd_info', 'U') is not null
			drop table silver.crm_prd_info;
		print '>> Create Table for silver.crm_prd_info';
		Go
		Create table silver.crm_prd_info (
			prd_id int,
			cat_id nvarchar(50),
			prd_key Nvarchar(50),
			prd_nm nvarchar(50),
			prd_cost int,
			prd_line nvarchar(50),
			prd_start_dt date,
			prd_end_dt date,
			dwh_create_date Datetime2 default getdate()
		);
		
		Go
		print '>> Drop Table for silver.crm_sales_details';
		if OBJECT_ID ('silver.crm_sales_details', 'U') is not null
			drop table silver.crm_sales_details;
		print '>> Create Table for silver.crm_sales_details';

		create table silver.crm_sales_details (
			sls_ord_num Nvarchar(50),
			sls_prd_key Nvarchar(50),
			sls_cust_id int,
			sls_order_dt date,
			sls_ship_dt date,
			sls_due_dt date,
			sls_sales int,
			sls_quantity int,
			sls_price int,
			dwh_create_date datetime2 default getdate()
		);
		
		Go
		print '>> Drop Table for silver.erp_cust_az12';
		if OBJECT_ID ('silver.erp_cust_az12', 'U') is not null
			drop table silver.erp_cust_az12;
		Go
		print '>> Create Table for silver.erp_cust_az12';
		create table silver.erp_cust_az12 (
				cid Nvarchar (50),
				bdate date,
				gen Nvarchar(50),
				dwh_create_date datetime2 default getdate()
		);
		
		Go
		print '>> Drop Table for silver.erp_loc_a101';
		if object_id('silver.erp_loc_a101','U') is not null
			drop table silver.erp_loc_a101;
		Go
		print '>> Create Table for silver.erp_loc_a101';

		create table silver.erp_loc_a101(
				cid Nvarchar(50),
				cntry Nvarchar(50),
				dwh_create_date datetime2 default getdate()
		);
		
		Go
		print '>> Drop Table for silver.erp_cat_g1v2';
		if object_id('silver.erp_px_cat_g1v2','U') is not null
			drop table silver.erp_px_cat_g1v2;
		Go
		print '>> Create Table for silver.erp_cat_g1v2';
		create table silver.erp_px_cat_g1v2(
				id Nvarchar(50),
				cat Nvarchar(50),
				subcat Nvarchar(50),
				maintenance Nvarchar(50),
				dwh_create_date datetime2 default getdate()
		);
