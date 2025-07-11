------Date Exploration------------

-- find the date of the first and last order
-- How many years and months of sales are avaiable

select order_date from gold.fact_sales;

select min(order_date) as first_order_date,
	   max(order_date) as last_order_date,
	   datediff(year,min(order_date), max(order_date)) as order_range_years
from gold.fact_sales;

select min(order_date) as first_order_date,
	   max(order_date) as last_order_date,
	   datediff(year,min(order_date), max(order_date)) as order_range_years,
	   datediff(month,min(order_date), max(order_date)) as order_range_months
from gold.fact_sales;

-- find the youngest and the oldest customer
	select min(birthdate) as oldest_birthdate,
		   max(birthdate) as youngest_birthdate
	from gold.dim_customers;


select min(birthdate) as oldest_birthdate,
	   datediff(year,min(birthdate), getdate()) as oldest_age,
	   max(birthdate) as youngest_birthdate,
	   datediff(year,max(birthdate),getdate()) as youngest_age
from gold.dim_customers;
