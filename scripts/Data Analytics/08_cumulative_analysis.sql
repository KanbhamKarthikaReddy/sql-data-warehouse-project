------Cumulative Analysis------------

-- Calculate the total sales per month
-- and the running total of sales over time 

select 
order_date,
sales_amount
from gold.fact_sales;

select 
datetrunc(month,order_date) as order_date,
sum(sales_amount) as total_sales
from gold.fact_sales
where order_date is not null
group by datetrunc(month,order_date)
order by datetrunc(month,order_date);

-- running Total
select
order_date,
total_sales,
sum(total_sales) over (order by order_date) as running_total_sales
from
(
select 
datetrunc(month,order_date) as order_date,
sum(sales_amount) as total_sales
from gold.fact_sales
where order_date is not null
group by datetrunc(month,order_date)
)t;

-- running Total(monthly)
select
order_date,
total_sales,
sum(total_sales) over (partition by order_date order by order_date) as running_total_sales
from
(
select 
datetrunc(month,order_date) as order_date,
sum(sales_amount) as total_sales
from gold.fact_sales
where order_date is not null
group by datetrunc(month,order_date)
)t;

-- running Total(Yearly)
select
order_date,
total_sales,
sum(total_sales) over (partition by order_date order by order_date) as running_total_sales
from
(
select 
datetrunc(year,order_date) as order_date,
sum(sales_amount) as total_sales
from gold.fact_sales
where order_date is not null
group by datetrunc(year,order_date)
)t;

Go
--- -- running Total(Yearly Cumulative)
select
order_date,
total_sales,
sum(total_sales) over (order by order_date) as running_total_sales,
avg(avg_price) over (order by order_date) as moving_average_price
from
(
select 
datetrunc(year,order_date) as order_date,
sum(sales_amount) as total_sales,
avg(price) as avg_price
from gold.fact_sales
where order_date is not null
group by datetrunc(year,order_date)
)t;
