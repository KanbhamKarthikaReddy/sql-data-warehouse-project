-- Generate a report that shows all key metrics of the business

select 'Total sales' as measure_name, sum(sales_amount) as measure_value from gold.fact_sales
Union All
select 'Total Quantity', sum(quantity) from gold.fact_sales
union all
select 'Average Price', avg(price) from gold.fact_sales
union all
select 'Total Nr.Orders', count(distinct order_number) from gold.fact_sales
union all
select 'Total Nr.Products', count(distinct(product_name)) from gold.dim_products
union all 
select 'Total Nr.Customers', count(customer_key) from gold.dim_customers
;

--------------------OR-----------------------

-- Find the Total Sales
select sum(sales_amount) as total_sales from gold.fact_sales;

-- Find the how many items are sold
select sum(quantity) as total_quantity from gold.fact_sales;

-- Find the average selling price
select avg(price) as avg_price from gold.fact_sales;

-- Find the total number of orders
select count(order_number) as total_orders from gold.fact_sales;

-- Find the total number of orders(removing duplicates)
select count(distinct(order_number)) as total_orders from gold.fact_sales;

-- Find the total number of products
select count(product_key) as total_products from gold.dim_products;
select count(product_name) as total_products from gold.dim_products;

-- Find the total number of products(removing duplicates)
select count(distinct(product_key)) as total_products from gold.dim_products;
select count(distinct(product_name)) as total_products from gold.dim_products;

-- Find the total number of customers
select count(customer_key) as total_customers from gold.dim_customers;

-- Find the total number of customers that has placed an order
select count(customer_key) as total_customers from gold.fact_sales;
select count(distinct(customer_key)) as total_customers from gold.fact_sales;
