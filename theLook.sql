CREATE TABLE order_item
(
id numeric primary key,
order_id numeric,
user_id numeric,
product_id numeric ,
inventory_item_id numeric,
status varchar,
created_at  timestamp,
shipped_at timestamp,
delivered_at timestamp,
returned_at timestamp,
sale_price numeric
);
CREATE TABLE products
(
id numeric primary key,
cost numeric,
category varchar,
name varchar,
brand varchar,
retail_price numeric,
department varchar,
sku varchar,
distribution_center_id numeric
);
CREATE TABLE users
(
id numeric primary key,
first_name varchar,
last_name varchar,
email varchar,
age numeric, 
gender varchar,
state varchar,
street_address varchar,
postal_code varchar,
city varchar,
country varchar,
latitude numeric,
longitude numeric,
traffic_source varchar,
created_at timestamp
);
---Import data
select * from order_item;
select * from products;
select * from users;

--Cleaning & structuring
--remove null
select * from order_item
where id is null;

select * from products
where id is null;

select * from users
where id is null;
---ko co null

--remove duplicates
with dupl as (
select *,
ROW_NUMBER() OVER(PARTITION BY order_id, user_id, product_id, inventory_item_id) as stt
From order_item)
Select * from dupl
where stt >1;

with dupl as (
select *,
ROW_NUMBER() OVER(PARTITION BY id,cost,category,name) as stt
From products)
Select * from dupl
where stt >1;

with dupl as (
select *,
ROW_NUMBER() OVER(PARTITION BY id) as stt
From users)
Select * from dupl
where stt >1;
---ko co dupl

--tao bang data ttin trong nam 2023 ko cancel/return
CREATE TABLE customers AS (
SELECT * FROM users
WHERE id IN (
	SELECT 	user_id
	FROM order_item
	WHERE created_at BETWEEN '2023-01-01' AND '2023-12-31' AND status NOT IN ('Cancelled', 'Returned')
	GROUP BY user_id
	ORDER BY user_id )
);
---ANALYZE
-----churned customer = not make purchase in 90 days
--identify the last purchase date
with step1 as (
select user_id,
max(created_at) as latest_date,
('2023-12-31'-max(created_at))as date_diff
from order_item
WHERE created_at BETWEEN '2023-01-01' AND '2023-12-31' AND status NOT IN ('Cancelled', 'Returned')
GROUP BY user_id
),
step2 as(
Select *,
	case
	when extract(day from date_diff)> 90 then 'churn'
	else 'normal'
	end as cus_category
from  step1),
step3 as (
SELECT a.*, b.cus_category
FROM customers as a
INNER JOIN step2 as b
	ON a.id = b.user_id)
;
Select country,
count(id) as number
from step3
where cus_category = 'churn'
group by country
order by number DESC
limit 10

, number_churn AS (
SELECT gender, COUNT(id) as number_churn
FROM step3
WHERE cus_category = 'churn'
GROUP BY gender
)
, number_all AS (
SELECT gender, COUNT(id) as number_all
FROM step3
GROUP BY gender
ORDER BY number_all DESC
)
, churn_perc_gender as(
SELECT 	a.*, b.number_churn,
		ROUND(1.00 * b.number_churn / a.number_all , 2) as churn_perc
FROM number_all AS a
INNER JOIN number_churn AS b
	ON a.gender = b.gender
ORDER BY churn_perc DESC)

---children (0-15), youth (16-24), adult (25-64), senior (64+)
, age_group AS (
SELECT *, CASE
			WHEN age <= 15 THEN 'children'
			WHEN age <= 24 THEN 'youth'
			WHEN age <= 64 THEN 'adult'
			ELSE 'senior'
		END as age_group
FROM step3
)
, number_churn2 as (
select age_group, count(id) as number_churn
from age_group
where cus_category = 'churn'
group by age_group)
, number_all2 as (
select age_group, count (id) as number_all
from age_group
group by age_group
order by number_all)
, churn_perc_age as (
select a.*, b.number_churn,
	round(1.00*b.number_churn/a.number_all,2) as churn_perc
from number_all2 as a inner join number_churn2 as b
on a.age_group=b.age_group
order by churn_perc DESC )

, number_churn3 as (
select traffic_source, count(id) as number_churn
from age_group
where cus_category = 'churn'
group by traffic_source)
, number_all3 as (
select traffic_source, count (id) as number_all
from age_group
group by traffic_source
order by number_all)
, churn_perc_traffic as (
select a.*, b.number_churn,
	round(1.00*b.number_churn/a.number_all,2) as churn_perc
from number_all3 as a inner join number_churn3 as b
on a.traffic_source=b.traffic_source
order by churn_perc DESC )

, step_3 AS (
SELECT a.*, b.cus_category, c.category, c.name as product_name, c.brand
FROM order_item as a
LEFT JOIN products as c
	ON c.id = a.product_id
INNER JOIN step2 as b
	ON a.id = b.user_id
)
, number_churn4 AS (
SELECT category, COUNT(id) as number_churn
FROM step_3
WHERE cus_category = 'churn'
GROUP BY category
)
, number_all4 AS (
SELECT category, COUNT(id) as number_all
FROM step_3
GROUP BY category
ORDER BY number_all DESC
)
, churn_perc_category as (
SELECT 	a.*, b.number_churn,
		ROUND(1.00 * b.number_churn / a.number_all , 2) as churn_perc
FROM number_all4 AS a
INNER JOIN number_churn4 AS b
	ON a.category = b.category
--ORDER BY churn_perc ASC
ORDER BY churn_perc DESC
LIMIT 10)

, number_churn5 AS (
SELECT brand, COUNT(id) as number_churn
FROM step_3
WHERE cus_category = 'churn'
GROUP BY brand
)
, number_all5 AS (
SELECT brand, COUNT(id) as number_all
FROM step_3
GROUP BY brand
ORDER BY number_all DESC
)
, churn_perc_brand as (
SELECT 	a.*, b.number_churn,
		ROUND(1.00 * b.number_churn / a.number_all , 2) as churn_perc
FROM number_all5 AS a
INNER JOIN number_churn5 AS b
	ON a.brand = b.brand
--ORDER BY churn_perc ASC
ORDER BY churn_perc DESC
)
, churn_perc_nobrand as (
	Select churn_perc,
count(distinct brand) as number_of_brand
from churn_perc_brand
GROUP BY churn_perc
HAVING COUNT(DISTINCT brand) > 50
ORDER BY churn_perc DESC)

, number_churn6 AS (
SELECT product_name, COUNT(id) as number_churn
FROM step_3
WHERE cus_category = 'churn'
GROUP BY product_name
)
, number_all6 AS (
SELECT product_name, COUNT(id) as number_all
FROM step_3
GROUP BY product_name
ORDER BY number_all DESC
)
, churn_perc_product_name as (
SELECT 	a.*, b.number_churn,
		ROUND(1.00 * b.number_churn / a.number_all , 2) as churn_perc
FROM number_all6 AS a
INNER JOIN number_churn6 AS b
	ON a.product_name = b.product_name
ORDER BY churn_perc DESC)
, churn_perc_noproduct as (
select churn_perc,
count(distinct product_name) as no_product_name
from churn_perc_product_name
group by churn_perc
order by churn_perc DESC)

, frequency AS (
SELECT product_name, 
			COUNT(id) as frequency
FROM step_3
GROUP BY product_name
)
, frequency_nproduct as (
	SELECT frequency, 
			COUNT(product_name) as n_product
FROM frequency
GROUP BY frequency
ORDER BY n_product DESC)

, price_group AS (
SELECT 	*,
		CASE
			WHEN sale_price <= 100 THEN '0-100$'
			WHEN sale_price <= 300 THEN '101-300$'
			WHEN sale_price <= 700 THEN '301-700$'
			ELSE '701-1000$'
		END as price_group
FROM step_3
)
, number_churn7 AS (
SELECT price_group, COUNT(id) as number_churn
FROM price_group
WHERE cus_category = 'churn'
GROUP BY price_group
)
, number_all7 AS (
SELECT price_group, COUNT(id) as number_all
FROM price_group
GROUP BY price_group
ORDER BY number_all DESC
)
, churn_perc_price_group as (
SELECT 	a.*, b.number_churn,
		ROUND(1.00 * b.number_churn / a.number_all , 2) as churn_perc
FROM number_all7 AS a
INNER JOIN number_churn7 AS b
	ON a.price_group = b.price_group
ORDER BY churn_perc DESC)

select * from churn_perc_price_group

----COHORT ANALYSIS

with first_date as  (
SELECT *
FROM (
SELECT  created_at,
        MIN(created_at) OVER(PARTITION BY user_id) as first_date,
        user_id,
        sale_price
FROM order_item
WHERE status NOT IN ('Cancelled', 'Returned') ) as B1_1
WHERE first_date BETWEEN '2023-01-01' AND '2023-12-31'
)
, cohort_index AS(
SELECT  TO_CHAR(first_date, 'yyyy-mm') as cohort_date,
        (EXTRACT(YEAR FROM created_at) - EXTRACT(YEAR FROM first_date))*12
        + (EXTRACT(MONTH FROM created_at) - EXTRACT(MONTH FROM first_date)) +1 as index,
        user_id,
        sale_price
FROM first_date
WHERE created_at BETWEEN '2023-01-01' AND '2023-12-31'
)
, cohort_cus AS(
SELECT  cohort_date, index,
        SUM(sale_price) as revenue,
        COUNT(DISTINCT user_id) as customer
FROM cohort_index
where index <=12
GROUP BY cohort_date, index
ORDER BY cohort_date, index
)
, customer_cohort as (
SELECT  cohort_date,
        SUM(CASE WHEN index = 1 then customer ELSE 0 END) as t1,
        SUM(CASE WHEN index = 2 then customer ELSE 0 END) as t2,
        SUM(CASE WHEN index = 3 then customer ELSE 0 END) as t3,
        SUM(CASE WHEN index = 4 then customer ELSE 0 END) as t4,
		SUM(CASE WHEN index = 5 then customer ELSE 0 END) as t5,
        SUM(CASE WHEN index = 6 then customer ELSE 0 END) as t6,
        SUM(CASE WHEN index = 7 then customer ELSE 0 END) as t7,
        SUM(CASE WHEN index = 8 then customer ELSE 0 END) as t8,
		SUM(CASE WHEN index = 9 then customer ELSE 0 END) as t9,
        SUM(CASE WHEN index = 10 then customer ELSE 0 END) as t10,
        SUM(CASE WHEN index = 11 then customer ELSE 0 END) as t11,
        SUM(CASE WHEN index = 12 then customer ELSE 0 END) as t12
FROM cohort_cus
GROUP BY cohort_date
ORDER BY cohort_date)

, retention_cohort as (
SELECT  cohort_date,
        ROUND(100.00* t1 / t1 ,2) as t1,
        ROUND(100.00* t2 / t1 ,2) as t2,
        ROUND(100.00* t3 / t1 ,2) as t3,
        ROUND(100.00* t4 / t1 ,2) as t4,
				ROUND(100.00* t5 / t1 ,2) as t5,
        ROUND(100.00* t6 / t1 ,2) as t6,
        ROUND(100.00* t7 / t1 ,2) as t7,
        ROUND(100.00* t8 / t1 ,2) as t8,
				ROUND(100.00* t9 / t1 ,2) as t9,
        ROUND(100.00* t10 / t1 ,2) as t10,
        ROUND(100.00* t11 / t1 ,2) as t11,
        ROUND(100.00* t12 / t1 ,2) as t12
FROM customer_cohort)

select * from retention_cohort
