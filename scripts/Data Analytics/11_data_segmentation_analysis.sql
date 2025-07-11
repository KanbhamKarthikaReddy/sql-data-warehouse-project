------Data Segmentation Analysis------------

formulas 
		[Measure] by [Measure]
		Total Products by Sales Range
		Total Customers by Age
		
	-- Use to CASE WHEN STATEMENT--
	
/* Segment products into cost ranges and 
count how many products fall into each segment*/

select * from gold.dim_products;

with product_segment as (
select 
product_key,
product_name,
cost,
case when cost < 100 then 'Below 100'
	 when cost between 100 and 500 then '100-500'
	 when cost between 500 and 1000 then '500-1000'
	 else 'Above 1000'
end cost_range
from gold.dim_products)

select
cost_range,
count(product_key) as total_products
from product_segment
group by cost_range
order by total_products desc;

/* Group customers into three segments based on their spending behavior:
	- VIP: Customers with at least 12 months of history and spending more than $5,000
	- Regular: Cutomers with at least 12 months of history but spending $5,000 or Less.
	- New: Customers with a lifespan less than 12 months.
And find the total number of customers by each group
*/
-- step By step--

select 
c.customer_key,
f.sales_amount,
f.order_date 
from gold.fact_sales f
left join gold.dim_customers c
on f.customer_key = c.customer_key;

select 
c.customer_key,
sum(f.sales_amount) as total_sales,
min(order_date) as first_order,
max(order_date) as last_order
from gold.fact_sales f
left join gold.dim_customers c
on f.customer_key = c.customer_key
group by c.customer_key;

select 
c.customer_key,
sum(f.sales_amount) as total_sales,
min(order_date) as first_order,
max(order_date) as last_order,
datediff(month,min(order_date), max(order_date)) as lifespan
from gold.fact_sales f
left join gold.dim_customers c
on f.customer_key = c.customer_key
group by c.customer_key;

with customer_spending as (
select 
c.customer_key,
sum(f.sales_amount) as total_sales,
min(order_date) as first_order,
max(order_date) as last_order,
datediff(month,min(order_date), max(order_date)) as lifespan
from gold.fact_sales f
left join gold.dim_customers c
on f.customer_key = c.customer_key
group by c.customer_key
)
select
customer_key,
total_sales,
lifespan,
case when lifespan >= 12 and total_sales > 5000 then 'VIP'
	 when lifespan >= 12 and total_sales <= 5000 then 'Regular'
	 else 'New'
end as customer_segment
from customer_spending;

with customer_spending as (
select 
c.customer_key,
sum(f.sales_amount) as total_sales,
min(order_date) as first_order,
max(order_date) as last_order,
datediff(month,min(order_date), max(order_date)) as lifespan
from gold.fact_sales f
left join gold.dim_customers c
on f.customer_key = c.customer_key
group by c.customer_key
)
select
customer_segment,
count(customer_key) as total_customers
from (
	select 
	customer_key,
	case when lifespan >= 12 and total_sales > 5000 then 'VIP'
	 when lifespan >= 12 and total_sales <= 5000 then 'Regular'
	 else 'New'
	end as customer_segment
	from customer_spending) t
group by customer_segment
order by total_customers desc;
