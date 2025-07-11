------Performance Analysis------------

Formulas

		Current(Mesure) - Target(measure)
		Current Sales   - Average Sales
		Current Year Sales - Previous Year Sales
		Current Sales - Lowest Sales
		
using Windows Functions


/* Analyze the yearly performance of products by comparing their sales 
to both the average sales performance of the product and previous year's sales */
with yearly_product_sales as (
Select 
year(f.order_date) as order_year,
p.product_name,
sum(f.sales_amount) as current_sales
from gold.fact_sales f
left join gold.dim_products p
on f.product_key=p.product_key
where f.order_date is not null
group by year(f.order_date),
p.product_name
)

select
order_year,
product_name,
current_sales,
avg(current_sales) over (partition by product_name) avg_sales,
current_sales - avg(current_sales) over (partition by product_name) diff_avg_sales,
case when current_sales - avg(current_sales) over (partition by product_name) > 0 then 'Above Avg'
	 when current_sales - avg(current_sales) over (partition by product_name) < 0 then 'Below Avg'
	 else 'Avg'
end as change_avg_sales,
-- Year-over-year Analysis
LAG(current_sales) over (partition by product_name order by order_year) as py_sales,
current_sales - LAG(current_sales) over (partition by product_name order by order_year) as diff_py_sales,
case when current_sales - LAG(current_sales) over (partition by product_name order by order_year) > 0 then 'Increase'
	 when current_sales - LAG(current_sales) over (partition by product_name order by order_year) < 0 then 'Decrease'
	 else 'No Change'
end as Change_py_sales
from yearly_product_sales
order by product_name,order_year;
