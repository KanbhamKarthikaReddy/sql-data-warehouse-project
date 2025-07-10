------Dimensions Exploration------------

-- Explore All countries our cutomers come from.
select distinct country from gold.dim_customers;

-- Explore all categories 'the major divisions'
select distinct category, subcategory, product_name from gold.dim_procucts
order by 1,2,3;
