
------Change-Over-Time(Trends)------------

-- Day to Day Total Sales
select 
order_date,
sum(sales_amount) as total_Sales
from gold.fact_sales
where order_date is not null
group by order_date
order by order_date;

-- Year to Year Total Sales
select 
year(order_date) as order_year,
sum(sales_amount) as total_Sales
from gold.fact_sales
where order_date is not null
group by year(order_date)
order by year(order_date);

-- Filter by Month
select 
Month(order_date) as order_month,
count(distinct customer_key) as total_customers,
sum(quantity) as Total_quantity,
sum(sales_amount) as total_Sales
from gold.fact_sales
where order_date is not null
group by Month(order_date)
order by Month(order_date);

-- Filter by Order YEAR(only)
select 
year(order_date) as order_year,
Month(order_date) as order_month,
count(distinct customer_key) as total_customers,
sum(quantity) as Total_quantity,
sum(sales_amount) as total_Sales
from gold.fact_sales
where order_date is not null
group by year(order_date),month(order_date)
order by year(order_date),month(order_date);

-- Filter by Order Date using 'DATETRUNC'(2010-12-01) to getting monthly wise
select 
Datetrunc(month,order_date) as order_date,
count(distinct customer_key) as total_customers,
sum(quantity) as Total_quantity,
sum(sales_amount) as total_Sales
from gold.fact_sales
where order_date is not null
group by Datetrunc(month,order_date)
order by Datetrunc(month,order_date);

-- Filter by Order Date using 'DATETRUNC'(2010-12-01) to getting yealy wise
select 
Datetrunc(year,order_date) as order_date,
count(distinct customer_key) as total_customers,
sum(quantity) as Total_quantity,
sum(sales_amount) as total_Sales
from gold.fact_sales
where order_date is not null
group by Datetrunc(year,order_date)
order by Datetrunc(year,order_date);

-- Filter by format like(Month be 1,2,3)

select 
format(order_date, 'yyyy-MM') as order_date,
count(distinct customer_key) as total_customers,
sum(quantity) as Total_quantity,
sum(sales_amount) as total_Sales
from gold.fact_sales
where order_date is not null
group by format(order_date, 'yyyy-MM')
order by format(order_date, 'yyyy-MM');

or

-- Filter by Month name like (JAN,Feb)
select 
format(order_date, 'yyyy-MMM') as order_date,
count(distinct customer_key) as total_customers,
sum(quantity) as Total_quantity,
sum(sales_amount) as total_Sales
from gold.fact_sales
where order_date is not null
group by format(order_date, 'yyyy-MMM')
order by format(order_date, 'yyyy-MMM');

-- How many new customers were added each year

select 
Datetrunc(year, create_date) as create_year,
count(distinct customer_key) as total_customers
from gold.dim_customers
group by Datetrunc(year, create_date)
order by Datetrunc(year, create_date);
