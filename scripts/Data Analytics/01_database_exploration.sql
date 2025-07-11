------Database Exploration------------

-- Explore all objects in the database
select * from INFORMATION_SCHEMA.tables;

-- Explore all the colums in the database

select * from information_schema.columns
where table_name = 'dim_customers';
