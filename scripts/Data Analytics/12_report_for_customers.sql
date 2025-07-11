------Reporting Analysis------------
	 Advance analytics Project
	 (Build Customer Report)

-- THE BELOW ONE IS RUNNING NOTES STEP BY STEP
-- BEGIN
 	 
/* 
=====================================================================================
Customer Report
=====================================================================================
Purpose:
	- This report consolidate key customer metrics and behaviors

Highlights:
	1. Gathers essential fields such as names, ages, and transaction details.
	2. Segments customers into categories (VIP, Regular, New) and age groups.
	3. Aggregates customer-level metrics:
		- Total Orders
		- Total Sales
		- Total Quantity Purchased
		- Total Products
		- Lifespan (in Months)
	4. Calculates valuable KPIs:
		- Recency (months since last order)
		- Average order value
		- Average monthly spend
=====================================================================================
*/

/*------------------------------------------------------------------------------------
1) Base Query: Retrives core columns from tables
--------------------------------STEP BY STEP----------------------------------------------------*/
select
f.order_number,
f.product_key,
f.order_date,
f.sales_amount,
f.quantity,
c.customer_key,
c.customer_number,
c.first_name,
c.last_name,
c.birthdate
from gold.fact_sales f
left join gold.dim_customers c
on f.customer_key = f.customer_key;

select
f.order_number,
f.product_key,
f.order_date,
f.sales_amount,
f.quantity,
c.customer_key,
c.customer_number,
c.first_name,
c.last_name,
c.birthdate
from gold.fact_sales f
left join gold.dim_customers c
on f.customer_key = f.customer_key
where order_date is not null;


select
f.order_number,
f.product_key,
f.order_date,
f.sales_amount,
f.quantity,
c.customer_key,
c.customer_number,
concat(c.first_name,' ', c.last_name) as customer_name,
datediff(year,c.birthdate,getdate()) as age
from gold.fact_sales f
left join gold.dim_customers c
on f.customer_key = f.customer_key
where order_date is not null
);

/* 
=====================================================================================
Customer Report
=====================================================================================
Purpose:
	- This report consolidate key customer metrics and behaviors

Highlights:
	1. Gathers essential fields such as names, ages, and transaction details.
	2. Segments customers into categories (VIP, Regular, New) and age groups.
	3. Aggregates customer-level metrics:
		- Total Orders
		- Total Sales
		- Total Quantity Purchased
		- Total Products
		- Lifespan (in Months)
	4. Calculates valuable KPIs:
		- Recency (months since last order)
		- Average order value
		- Average monthly spend
=====================================================================================
*/

/*------------------------------------------------------------------------------------
1) Base Query: Retrives core columns from tables
------------------------------------------------------------------------------------*/

if object_id('gold.report_customers','v') is not null
	drop view gold.report_customers;
	
go

create view gold.report_customers as 
with base_query as (
select
f.order_number,
f.product_key,
f.order_date,
f.sales_amount,
f.quantity,
c.customer_key,
c.customer_number,
concat(c.first_name,' ', c.last_name) as customer_name,
datediff(year,c.birthdate,getdate()) as age
from gold.fact_sales f
left join gold.dim_customers c
on f.customer_key = f.customer_key
where order_date is not null
)
select
* 
from base_query;

with base_query as (
select
f.order_number,
f.product_key,
f.order_date,
f.sales_amount,
f.quantity,
c.customer_key,
c.customer_number,
concat(c.first_name,' ', c.last_name) as customer_name,
datediff(year,c.birthdate,getdate()) as age
from gold.fact_sales f
left join gold.dim_customers c
on f.customer_key = f.customer_key
where order_date is not null
)
select
	customer_key,
	customer_number,
	customer_name,
	age,
	count(distinct order_number) as total_orders,
	sum(sales_amount) as total_sales,
	sum(quantity) as total_quantity,
	count(distinct product_key) AS TOTAL_PRODUCTS,
	max(order_date) as last_order_date,
	datediff(month,min(order_date),max(order_date)) as lifespan
from base_query
group by 
	customer_key,
	customer_number,
	customer_name,
	age;

with base_query as (
select
f.order_number,
f.product_key,
f.order_date,
f.sales_amount,
f.quantity,
c.customer_key,
c.customer_number,
concat(c.first_name,' ', c.last_name) as customer_name,
datediff(year,c.birthdate,getdate()) as age
from gold.fact_sales f
left join gold.dim_customers c
on f.customer_key = f.customer_key
where order_date is not null
), customer_aggregation as (
/* ------------------------------------------------------------------
2) customer Aggregations: Summarizes key metrics at the customer level
--------------------------------------------------------------------*/
select
	customer_key,
	customer_number,
	customer_name,
	age,
	count(distinct order_number) as total_orders,
	sum(sales_amount) as total_sales,
	sum(quantity) as total_quantity,
	count(distinct product_key) AS total_products,
	max(order_date) as last_order_date,
	datediff(month,min(order_date),max(order_date)) as lifespan
from base_query
group by 
	customer_key,
	customer_number,
	customer_name,
	age
)
select
customer_key,
customer_number,
customer_name,
age,
case when age < 20 then 'Under 20'
	 when age between 20 and 29 then '20-29'
	 when age between 30 and 39 then '30-39'
	 when age between 40 and 49 then '40-49'
	 else '50 and Above'
end as age_group,
case 
	when lifespan >= 12 and total_spending > 5000 then 'VIP'
	when lifespan >= 12 and total_spending <= 5000 then 'Regular'
	else 'New'
end as customer_segment,
last_order_date,
datediff(month,last_order_date,getdate()) as recency,
total_orders,
total_sales,
total_quantity,
total_products,
lifespan
from customer_aggregation;

select * from gold.report_customers;

---------final Above last one 
-- END
