/*
===============================================================================
Running Notes
===============================================================================
Script Purpose:
    This script performs running Notes to validate the integrity, consistency, 
    and accuracy of the Gold Layer. These checks ensure:
    - Uniqueness of surrogate keys in dimension tables.
    - Referential integrity between fact and dimension tables.
    - Validation of relationships in the data model for analytical purposes.

Usage Notes:
    - Investigate and resolve any discrepancies found during the notes.
===============================================================================
*/

use datawarehouse;

select * from silver.crm_cust_info;

select 
	ci.cst_id,
	ci.cst_key,
	ci.cst_firstname,
	ci.cst_lastname,
	ci.cst_marital_status,
	ci.cst_gndr,
	cst_create_date,
	ca.bdate,
	ca.gen,
	la.cntry
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on		  ci.cst_key=ca.cid
left join silver.erp_loc_a101 la
on		  ci.cst_key=la.cid;


--- check Duplicates
select cst_id, count(*) from
	(select 
		ci.cst_id,
		ci.cst_key,
		ci.cst_firstname,
		ci.cst_lastname,
		ci.cst_marital_status,
		ci.cst_gndr,
		cst_create_date,
		ca.bdate,
		ca.gen,
		la.cntry
	from silver.crm_cust_info ci
	left join silver.erp_cust_az12 ca
	on		  ci.cst_key=ca.cid
	left join silver.erp_loc_a101 la
	on		  ci.cst_key=la.cid
	)t group by cst_id
	having count(*) >1;
	
	
	--- check Gender (we have two)

select distinct
	ci.cst_gndr,
	ca.gen
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on		  ci.cst_key=ca.cid
left join silver.erp_loc_a101 la
on		  ci.cst_key=la.cid
order by 1,2;

select distinct
	ci.cst_gndr,
	ca.gen,
	case when ci.cst_gndr != 'n/a' then ci.cst_gndr  -- CRM is the master for gender info
		  else coalesce(ca.gen, 'n/a')
	 end as new_gen
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on		  ci.cst_key=ca.cid
left join silver.erp_loc_a101 la
on		  ci.cst_key=la.cid
order by 1,2,3;


------------ step by step Data transfermation-----------------
use Datawarehouse;

select
pn.prd_id,
pn.cat_id,
pn.prd_key,
pn.prd_nm,
pn.prd_cost,
pn.prd_line,
pn.prd_start_dt, 
pn.prd_end_dt
from silver.crm_prd_info pn
where prd_end_dt is null;  --- Filter out all historical data so no need 

select
pn.prd_id,
pn.cat_id,
pn.prd_key,
pn.prd_nm,
pn.prd_cost,
pn.prd_line,
pn.prd_start_dt,
pc.id,
pc.cat,
pc.subcat,
pc.maintenance
from silver.crm_prd_info pn
left join silver.erp_px_cat_g1v2 pc
on pn.cat_id=pc.id
where prd_end_dt is null;  --- Filter out all historical data so no need to show

select * from silver.crm_prd_info;
select * from silver.erp_px_cat_g1v2;

select prd_key,count(*) from(
	select
	pn.prd_id,
	pn.cat_id,
	pn.prd_key,
	pn.prd_nm,
	pn.prd_cost,
	pn.prd_line,
	pn.prd_start_dt,
	pc.cat,
	pc.subcat,
	pc.maintenance
	from silver.crm_prd_info pn
	left join silver.erp_px_cat_g1v2 pc
	on pn.cat_id=pc.id
	where prd_end_dt is null
	)t group by prd_key
	having count(*)>1;  --- Filter out all historical data so no need to show

select * from silver.crm_prd_info;
select * from silver.erp_px_cat_g1v2;

select
	ROW_NUMBER() over (order by pn.prd_start_dt, pn.prd_key) as product_key,
	pn.prd_id as product_id,
	pn.prd_key as product_Number,
	pn.prd_nm as product_name,
	pn.cat_id as category_id,
	pc.cat as category,
	pc.subcat as subcategory,
	pc.maintenance,
	pn.prd_cost as cost,
	pn.prd_line as product_line,
	pn.prd_start_dt as start_date
from silver.crm_prd_info pn
left join silver.erp_px_cat_g1v2 pc
on pn.cat_id=pc.id
where prd_end_dt is null;  --- Filter out all historical data so no need to show

Create view gold.dim_procucts as 
select
	ROW_NUMBER() over (order by pn.prd_start_dt, pn.prd_key) as product_key,
	pn.prd_id as product_id,
	pn.prd_key as product_Number,
	pn.prd_nm as product_name,
	pn.cat_id as category_id,
	pc.cat as category,
	pc.subcat as subcategory,
	pc.maintenance,
	pn.prd_cost as cost,
	pn.prd_line as product_line,
	pn.prd_start_dt as start_date
from silver.crm_prd_info pn
left join silver.erp_px_cat_g1v2 pc
on pn.cat_id=pc.id
where prd_end_dt is null;



select * from gold.dim_procucts;
------------------------------

               step by step
-----------------------------------------
use Datawarehouse;

select * from silver.crm_sales_details;

select
	sd.sls_ord_num,
	sd.sls_prd_key,
	sd.sls_cust_id,
	sd.sls_order_dt,
	sd.sls_ship_dt,
	sd.sls_due_dt,
	sd.sls_sales,
	sd.sls_quantity,
	sd.sls_price
from silver.crm_sales_details sd;

select * from silver.crm_sales_details;
select * from gold.dim_procucts;
select * from gold.dim_customers;

select
	sd.sls_ord_num,
	sd.sls_prd_key,
	pr.product_number,
	sd.sls_cust_id,
	cu.customer_id,
	sd.sls_order_dt,
	sd.sls_ship_dt,
	sd.sls_due_dt,
	sd.sls_sales,
	sd.sls_quantity,
	sd.sls_price
from silver.crm_sales_details sd
left join gold.dim_procucts pr
on sd.sls_prd_key=pr.product_number
left join gold.dim_customers cu
on sd.sls_cust_id=cu.customer_id;


select
	sd.sls_ord_num as order_number,
	pr.product_key,
	cu.customer_key,
	cu.customer_id,
	sd.sls_order_dt as order_date,
	sd.sls_ship_dt as shiping_date,
	sd.sls_due_dt as due_date,
	sd.sls_sales as sales_amount,
	sd.sls_quantity as quantity,
	sd.sls_price as price
from silver.crm_sales_details sd
left join gold.dim_procucts pr
on sd.sls_prd_key=pr.product_number
left join gold.dim_customers cu
on sd.sls_cust_id=cu.customer_id;


create view gold.fact_sales as 
select
	sd.sls_ord_num as order_number,
	pr.product_key,
	cu.customer_key,
	cu.customer_id,
	sd.sls_order_dt as order_date,
	sd.sls_ship_dt as shiping_date,
	sd.sls_due_dt as due_date,
	sd.sls_sales as sales_amount,
	sd.sls_quantity as quantity,
	sd.sls_price as price
from silver.crm_sales_details sd
left join gold.dim_procucts pr
on sd.sls_prd_key=pr.product_number
left join gold.dim_customers cu
on sd.sls_cust_id=cu.customer_id;

--- foreign key integrity (Dimensions)

select * from gold.fact_sales;
select *
from gold.fact_sales f
left join gold.dim_customers c
on c.customer_key=f.customer_key
left join gold.dim_procucts p
on p.product_key = f.product_key
where p.product_key is null and c.customer_key is null;

select * from gold.fact_sales
where sales_amount is null;

-----------------------------------------
----------------Update the data-----------------------

use datawarehouse;

select * from silver.crm_sales_details
where sls_sales is null;

update silver.crm_sales_details
	set sls_sales = case when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * abs(sls_price)
						then sls_quantity * abs(sls_price)
					 else sls_sales
				end
where sls_sales is null;

select * from gold.fact_sales
where sales_amount is null;
