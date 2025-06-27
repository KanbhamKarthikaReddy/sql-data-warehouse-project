/*

============================================================================
Quality Checks
============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy,
    and standardization across the 'silver' schema. It includes checks for:
    - Null or Duplicates in Primary key.
    - Unwanted spaces in string fields.
    - Data consistency between related fields.
    - Invalid data ranges and orders.

Usage Notes:
      -  Run these checks after data loading Silver Layer.
      -  Investigate and resolve any discrepancies found the checks.
============================================================================
*/

-- ============================================================================
--- Quality checks
--- Check for Nulls or Duplicates in Primary key
--- Expections: No Result

select 
prd_id,
Count(*)
from bronze.crm_prd_info
group by prd_id
having count(*) > 1 or prd_id is null;

--- check for unwanted Spaces
-- Expectation: no Results

select prd_nm
from bronze.crm_prd_info
where prd_nm ! = trim(prd_nm);

--- Check for Nulls or Negative numbers
--- Expectation: no Results
Select prd_cost
from bronze.crm_prd_info
where prd_cost < 0 or prd_cost is null;

-- Data Standardization & Consistency--
select distinct prd_line 
from bronze.crm_prd_info;

-- Check for Invalid Data Orders

select * from bronze.crm_prd_info
where prd_end_dt < prd_start_dt;

Select 
*,
lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)-1 as prd_end_dt_test
from bronze.crm_prd_info
where prd_key in ('AC-HE-HL-U509-R','AC-HE-HL_U509');

IF OBJECT_ID('silver.crm_prd_info', 'U') is not null
	drop table silver.crm_prd_info;
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




-----------------------------------

--- Quality checks
--- Check for Nulls or Duplicates in Primary key
--- Expections: No Result

select 
prd_id,
Count(*)
from silver.crm_prd_info
group by prd_id
having count(*) > 1 or prd_id is null;

--- check for unwanted Spaces
-- Expectation: no Results

select prd_nm
from silver.crm_prd_info
where prd_nm ! = trim(prd_nm);

--- Check for Nulls or Negative numbers
--- Expectation: no Results
Select prd_cost
from silver.crm_prd_info
where prd_cost < 0 or prd_cost is null;

-- Data Standardization & Consistency--
select distinct prd_line 
from silver.crm_prd_info;

-- Check for Invalid Data Orders

select * from silver.crm_prd_info
where prd_end_dt < prd_start_dt;

select * from silver.crm_prd_info;

-------------------------------
select * from bronze.crm_cust_info;

select * from bronze.crm_cust_info
where cst_id = 29466;

select *, row_number() over (partition by cst_id order by cst_create_data desc) as flag_last
from bronze.crm_cust_info
where cst_id = 29466;

select *, row_number() over (partition by cst_id order by cst_create_data desc) as flag_last
from bronze.crm_cust_info;

select * from (
select *, row_number() over (partition by cst_id order by cst_create_data desc) as flag_last
from bronze.crm_cust_info)t where flag_last !=1;
-------------------------------------

use Datawarehouse;

select sls_ord_num from bronze.crm_sales_details
where sls_ord_num !=trim(sls_ord_num);

select 
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt
sls_quantity,
sls_price
from bronze.crm_sales_details
where sls_prd_key not in (select prd_key from silver.crm_prd_info);

select 
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt
sls_quantity,
sls_price
from bronze.crm_sales_details
where sls_cust_id not in (select cst_id from silver.crm_cust_info);

--- check for valid (sls_order_dt) dates--
select 
Nullif (sls_order_dt,0) sls_order_dt
from bronze.crm_sales_details
where sls_order_dt <=0 
or len(sls_order_dt) !=8
or sls_order_dt > 20500101
or sls_order_dt < 19000101;

select 
sls_ord_num,
sls_prd_key,
sls_cust_id,
case when sls_order_dt = 0 or len(sls_order_dt) !=8 then null
	 else cast(cast(sls_order_dt as varchar) as date)
end as sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
from bronze.crm_sales_details;

--- check for valid (sls_ship_dt) dates--
select 
Nullif (sls_ship_dt,0) sls_ship_dt
from bronze.crm_sales_details
where sls_ship_dt <=0 
or len(sls_ship_dt) !=8
or sls_ship_dt > 20500101
or sls_ship_dt < 19000101;

select 
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
case when sls_ship_dt = 0 or len(sls_ship_dt) !=8 then null
	 else cast(cast(sls_ship_dt as varchar) as date)
end as sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
from bronze.crm_sales_details;


--- check for valid (sls_due_dt) dates--
select 
Nullif (sls_due_dt,0) sls_due_dt
from bronze.crm_sales_details
where sls_due_dt <=0 
or len(sls_due_dt) !=8
or sls_due_dt > 20500101
or sls_due_dt < 19000101;

select 
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
case when sls_due_dt = 0 or len(sls_due_dt) !=8 then null
	 else cast(cast(sls_due_dt as varchar) as date)
end as sls_due_dt,
sls_sales,
sls_quantity,
sls_price
from bronze.crm_sales_details;

--- check for invalid Date orders range

select * 
from bronze.crm_sales_details
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt;

select distinct
sls_sales,
sls_quantity,
sls_price
from bronze.crm_sales_details
where sls_sales != sls_quantity * sls_price
or sls_sales is null or sls_quantity is null or sls_price is null 
or sls_sales <=0 or sls_quantity<=0 or sls_price <=0
order by sls_sales,sls_quantity,sls_price;

select distinct
sls_sales as old_sales,
sls_quantity as old_quantity,
sls_price as old_price,
case when sls_price is null or sls_sales <=0 or sls_sales != sls_quantity * abs(sls_price)
		then sls_quantity * abs(sls_price)
	else sls_sales
end as sls_sales,
case when sls_price is null or sls_price <=0
		then sls_sales / nullif(sls_quantity,0)
	else sls_price
end as sls_price
from bronze.crm_sales_details
where sls_sales != sls_quantity * sls_price
or sls_sales is null or sls_quantity is null or sls_price is null 
or sls_sales <=0 or sls_quantity<=0 or sls_price <=0
order by sls_sales,sls_quantity,sls_price;

select * from silver.crm_prd_info;

---------------------------------------------
                  ERP Running notes
--------------------------------------------
use Datawarehouse;

select * from bronze.erp_cust_az12;

select 
cid,
bdate,
gen
from bronze.erp_cust_az12;

select * from silver.crm_cust_info;

select 
cid,
case when cid like 'NAS%' then SUBSTRING(cid, 4, len(cid))
	 else cid
end as cid,
bdate,
gen
from bronze.erp_cust_az12;

select 
cid,
case when cid like 'NAS%' then SUBSTRING(cid, 4, len(cid))
	 else cid
end as cid,
bdate,
gen
from bronze.erp_cust_az12
where case when cid like 'NAS%' then SUBSTRING(cid, 4, len(cid))
	 else cid
end not in (select distinct cst_key from silver.crm_cust_info);

---------------

--- Identifyu Out-of-Range dates

select distinct 
bdate
from bronze.erp_cust_az12
where bdate < '1924-01-01' or bdate > getdate();

select 
case when bdate > getdate() then null
	 ELSE BDATE
END AS CBDATE,
bdate
from bronze.erp_cust_az12
where bdate < '1924-01-01' or bdate > getdate();
--------------

select 
case when cid like 'NAS%' then SUBSTRING(cid, 4, len(cid))
	 else cid
end as cid,
case when bdate > getdate() then null
	 else bdate
end as bdate,
gen
from bronze.erp_cust_az12;

---- Data Standardization & Consistency---
select distinct
gen,
case when upper(trim(gen)) in ('F', 'FEMALE') THEN 'Female'
	 when upper(trim(gen)) in ('M', 'MALE') THEN 'Male'
	 else 'n/a'
end as gen
from bronze.erp_cust_az12;

-----------------------------
select 
case when cid like 'NAS%' then SUBSTRING(cid, 4, len(cid))
	 else cid
end as cid,
case when bdate > getdate() then null
	 else bdate
end as bdate,
case when upper(trim(gen)) in ('F', 'FEMALE') THEN 'Female'
	 when upper(trim(gen)) in ('M', 'MALE') THEN 'Male'
	 else 'n/a'
end as gen
from bronze.erp_cust_az12;



--- checks 

select distinct 
bdate
from silver.erp_cust_az12
where bdate < '1924-01-01' or bdate > getdate();

select distinct
gen
from silver.erp_cust_az12;

select *
from silver.erp_cust_az12;
---------------------------

use Datawarehouse;

select 
replace(cid,'-','') as cid,
cntry
from bronze.erp_loc_a101;

select cst_key from silver.crm_cust_info;

select 
replace(cid,'-','') as cid,
case when trim(cntry) = 'DE' then 'Germany'
	 when trim(cntry) in ('US','USA') then 'United States'
	 when trim(cntry) = '' or cntry is null then 'n/a'
	 else trim (cntry)
end as cntry
from bronze.erp_loc_a101
where replace(cid,'-','') not in (select cst_key from silver.crm_cust_info);

select 
replace(cid,'-','') as cid,
cntry
from bronze.erp_loc_a101;

-- Data Standardization & Consistency

select distinct 
cntry
from bronze.erp_loc_a101
order by cntry;


select distinct 
cntry as old_cntry,
case when trim(cntry) = 'DE' then 'Germany'
	 when trim(cntry) in ('US','USA') then 'United States'
	 when trim(cntry) = '' or cntry is null then 'n/a'
	 else trim (cntry)
end as cntry
from bronze.erp_loc_a101
order by old_cntry,cntry;



---checks---

select distinct 
cntry
from silver.erp_loc_a101
order by cntry;

select * from silver.erp_loc_a101;
--------------

use Datawarehouse;

select
id,
cat,
subcat,
maintenance
from bronze.erp_px_cat_g1v2;

select
*
from bronze.erp_px_cat_g1v2;

select * from silver.crm_prd_info;

-- check for unwanted spaces--

select * from bronze.erp_px_cat_g1v2
where cat != trim(cat) or subcat != trim(subcat) or maintenance != trim(maintenance);

-- data Standardization & consistency

select distinct
maintenance
from bronze.erp_px_cat_g1v2;

----------------------------------
