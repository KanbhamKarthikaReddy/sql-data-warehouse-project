------Part-to-Whole Analysis------------
 
Formulas
		([measure]/Total[measure]) * 100 by [Dimension]
		(sales/Total sales) * 100 by Category
		(Quantity/Total Quantity) * 100 by Country
		
		
-- Which categories countribute the most to overall sales?

-- Step by step (recommend to use last one)*** Lead***

-- Which categories countribute the most to overall sales?

select
category,
sales_amount
from gold.fact_sales f
left join gold.dim_products p
on p.product_key = f.product_key;

select
p.category,
sum(f.sales_amount) as total_sales
from gold.fact_sales f
left join gold.dim_products p
on p.product_key = f.product_key
group by p.category;

with category_sales as (
select
p.category,
sum(f.sales_amount) as total_sales
from gold.fact_sales f
left join gold.dim_products p
on p.product_key = f.product_key
group by p.category)

select
category,
total_sales,
sum(total_sales) over() overall_sales
from category_sales;

with category_sales as (
select
p.category,
sum(f.sales_amount) as total_sales
from gold.fact_sales f
left join gold.dim_products p
on p.product_key = f.product_key
group by p.category)

select
category,
total_sales,
sum(total_sales) over() overall_sales,
(total_sales / sum(total_sales) over ()) * 100 as percentage_of_total
from category_sales;

with category_sales as (
select
p.category,
sum(f.sales_amount) as total_sales
from gold.fact_sales f
left join gold.dim_products p
on p.product_key = f.product_key
group by p.category)

select
category,
total_sales,
sum(total_sales) over() overall_sales,
(cast (total_sales as float) / sum(total_sales) over ()) * 100 as percentage_of_total
from category_sales;

with category_sales as (
select
p.category,
sum(f.sales_amount) as total_sales
from gold.fact_sales f
left join gold.dim_products p
on p.product_key = f.product_key
group by p.category)

select
category,
total_sales,
sum(total_sales) over() overall_sales,
round((cast (total_sales as float) / sum(total_sales) over ()) * 100,2) as percentage_of_total
from category_sales;

with category_sales as (
select
p.category,
sum(f.sales_amount) as total_sales
from gold.fact_sales f
left join gold.dim_products p
on p.product_key = f.product_key
group by p.category)

select
category,
total_sales,
sum(total_sales) over() overall_sales,
concat(round((cast (total_sales as float) / sum(total_sales) over ()) * 100,2), '%') as percentage_of_total
from category_sales
order by total_sales desc;


select * from gold.fact_sales;
select * from gold.dim_products;
