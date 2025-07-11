/*
===================================================================
Product Report
===================================================================
Purpose:
	-This report consolidates key product metrics and behaviors.

Highlights:
	1. Gathers essential fileds such as product name, category, subcategory, and cost.
	2. Segments products by revenu to identify high_performers, Mid-Rabge, or Low-Performers.
	3. Aggregates Products-level metrics:
		- total orders
		- total sales
		- total quanity sold
		- total customers (unique)
		- lifespan (in months)
	4. calculates valuable KPIs:
		- recency (months since last sale)
		- average order revenue (AOR)
		- average monthly revenue
===================================================================
*/

select 
	f.order_number,
	f.order_date,
	f.customer_key,
	f.sales_amount,
	f.quantity,
	p.product_key,
	p.product_name,
	p.category,
	p.subcategory,
	p.cost
from gold.fact_sales f
left join gold.dim_products p
	on p.product_key = f.product_key;

select 
	f.order_number,
	f.order_date,
	f.customer_key,
	f.sales_amount,
	f.quantity,
	p.product_key,
	p.product_name,
	p.category,
	p.subcategory,
	p.cost
from gold.fact_sales f
left join gold.dim_products p
	on p.product_key = f.product_key
Where order_date is not null;

/*	3. Aggregates Products-level metrics:
		- total orders
		- total sales
		- total quanity sold
		- total customers (unique)
	4. calculates valuable KPIs:
		- recency (months since last sale)
		- average order revenue (AOR)
		- average monthly revenue
*/

select 
	p.product_key,
	p.product_name,
	p.category,
	p.subcategory,
	p.cost,
	count(distinct f.order_number) as total_orders,
	datediff(month, min(f.order_date), max(f.order_date)) as lifespan,
	max(f.order_date) as last_sale_date,
	count(distinct f.customer_key) as total_customers,
	sum(f.sales_amount) as total_sales,
	sum(f.quantity) as total_quanity
from gold.fact_sales f
left join gold.dim_products p
	on p.product_key = f.product_key
Where order_date is not null
group by 
	p.product_key,
	p.product_name,
	p.category,
	p.subcategory,
	p.cost;

/*
===================================================================
Product Report
===================================================================
Purpose:
	-This report consolidates key product metrics and behaviors.

Highlights:
	1. Gathers essential fileds such as product name, category, subcategory, and cost.
	2. Segments products by revenu to identify high_performers, Mid-Rabge, or Low-Performers.
	3. Aggregates Products-level metrics:
		- total orders
		- total sales
		- total quanity sold
		- total customers (unique)
		- lifespan (in months)
	4. calculates valuable KPIs:
		- recency (months since last sale)
		- average order revenue (AOR)
		- average monthly revenue
===================================================================
*/
if object_id('gold.report_products','v') is not null
	drop view gold.report_products;
	
go
create view gold.report_products as
with base_query as (
/*--------------------------------------------------------------------------
1) Base Query: Retrieves core columns from fact_sales and dim_products
--------------------------------------------------------------------------*/
	select 
		f.order_number,
		f.order_date,
		f.customer_key,
		f.sales_amount,
		f.quantity,
		p.product_key,
		p.product_name,
		p.category,
		p.subcategory,
		p.cost
	from gold.fact_sales f
	left join gold.dim_products p
		on p.product_key = f.product_key
	Where order_date is not null -- only consider valid sales dates --
),

product_aggregations as (
/*--------------------------------------------------------------------------
2) Product Aggregations: Summarizes key metrics at the product level
--------------------------------------------------------------------------*/
	select 
		product_key,
		product_name,
		category,
		subcategory,
		cost,
		count(distinct order_number) as total_orders,
		datediff(month, min(order_date), max(order_date)) as lifespan,
		max(order_date) as last_sale_date,
		count(distinct customer_key) as total_customers,
		sum(sales_amount) as total_sales,
		sum(quantity) as total_quantity,
		round(avg(cast(sales_amount as float)/ nullif(quantity,0)),1) as avg_selling_price -- average order revenue (AOR)
	from base_query
	group by 
		product_key,
		product_name,
		category,
		subcategory,
		cost
)

/*--------------------------------------------------------------------------
3) Final Query: Combines all product results into one output
--------------------------------------------------------------------------*/
select 
		product_key,
		product_name,
		category,
		subcategory,
		cost,
		last_sale_date,
		datediff(month, last_sale_date, getdate()) as recency_in_months,
		case
			when total_sales > 50000 then 'High-Performer'
			when total_sales >= 100000 then 'Mid-Range'
			else 'Low-Performer'
		end as product_segment,
		lifespan,
		total_orders,
		total_sales,
		total_quantity,
		avg_selling_price,
		-- Average order Revenue (AOR)
		case
			when total_orders = 0 then 0
			else total_sales / total_orders
		end as avg_order_revenue,
		-- Average Monthly Revenue
		case
			when lifespan = 0 then total_sales
			else total_sales / lifespan
		end as avg_monthly_revenue
from product_aggregations;

select * from gold.report_products;

---------final Above last one 
-- END
