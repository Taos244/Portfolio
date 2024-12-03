--tao bang de import du lieu
create table SALES_DATASET_RFM_PRJ
(
  ordernumber VARCHAR,
  quantityordered VARCHAR,
  priceeach        VARCHAR,
  orderlinenumber  VARCHAR,
  sales            VARCHAR,
  orderdate        VARCHAR,
  status           VARCHAR,
  productline      VARCHAR,
  msrp             VARCHAR,
  productcode      VARCHAR,
  customername     VARCHAR,
  phone            VARCHAR,
  addressline1     VARCHAR,
  addressline2     VARCHAR,
  city             VARCHAR,
  state            VARCHAR,
  postalcode       VARCHAR,
  country          VARCHAR,
  territory        VARCHAR,
  contactfullname  VARCHAR,
  dealsize         VARCHAR
) 
--chuyen doi kieu du lieu phu hop

ALTER TABLE SALES_DATASET_RFM_PRJ
ALTER COLUMN priceeach type numeric USING (trim(priceeach)::numeric),
ALTER COLUMN ordernumber type integer USING (trim(ordernumber)::integer),
ALTER COLUMN quantityordered type numeric USING (trim(quantityordered)::numeric),
ALTER COLUMN orderlinenumber type numeric USING (trim(orderlinenumber)::numeric),
ALTER COLUMN sales type numeric USING (trim(sales)::numeric),
ALTER COLUMN orderdate type date USING (trim(orderdate)::date),
ALTER COLUMN status type VARCHAR USING (trim(status)::VARCHAR),
ALTER COLUMN productline type VARCHAR USING (trim(productline)::VARCHAR),
ALTER COLUMN msrp type numeric USING (trim(msrp)::numeric),
ALTER COLUMN productcode type VARCHAR USING (trim(productcode)::VARCHAR),
ALTER COLUMN customername type VARCHAR USING (trim(customername)::VARCHAR),
ALTER COLUMN phone type VARCHAR USING (trim(phone)::VARCHAR),
ALTER COLUMN addressline1 type VARCHAR USING (trim(addressline1)::VARCHAR),
ALTER COLUMN addressline2 type VARCHAR USING (trim(addressline2)::VARCHAR),
ALTER COLUMN city type VARCHAR USING (trim(city)::VARCHAR),
ALTER COLUMN postalcode type VARCHAR USING (trim(postalcode)::VARCHAR),
ALTER COLUMN addressline2 type VARCHAR USING (trim(addressline2)::VARCHAR),
ALTER COLUMN country type VARCHAR USING (trim(country)::VARCHAR),
ALTER COLUMN territory type VARCHAR USING (trim(territory)::VARCHAR),
ALTER COLUMN contactfullname type text USING (trim(contactfullname)::text),
ALTER COLUMN dealsize type VARCHAR USING (trim(dealsize)::VARCHAR)

--hold rfm scores & import csv
CREATE TABLE segment_score
(
segment VARCHAR,
scores VARCHAR	
)

--check null/dup
Select *
from public.sales_dataset_rfm_prj
where ORDERNUMBER is null
or QUANTITYORDERED is null
or PRICEEACH is null
or ORDERLINENUMBER is null
or SALES is null
or ORDERDATE is null

with dupl as (
select *,
ROW_NUMBER() OVER(PARTITION BY ordernumber, quantityordered, priceeach,orderlinenumber, sales, orderdate) as stt
From public.sales_dataset_rfm_prj)
Select * from dupl
where stt >1
Select * from public.segment_score

--tim outlier cho quantityordered

with IQR as (
Select
percentile_cont(0.25) within group (order by quantityordered) as Q1,
percentile_cont(0.75) within group (order by quantityordered) as Q3,
percentile_cont(0.75) within group (order by quantityordered)
-percentile_cont(0.25) within group (order by quantityordered) as IQR
from sales_dataset_rfm_prj)

, bound as(
Select
	Q1-1.5*IQR as min,
	Q3+1.5*IQR as max
from IQR
	)
, outlier as (
Select quantityordered
from sales_dataset_rfm_prj
where quantityordered<(select min from bound)
or quantityordered>(select max from bound)
)
DELETE from sales_dataset_rfm_prj
where quantityordered in(Select * from outlier)
--luu data sach vao bang moi
CREATE TABLE SALES_DATASET_RFM_PRJ_CLEAN AS(
Select * from public.sales_dataset_rfm_prj
where ORDERNUMBER is not null
AND QUANTITYORDERED is not null
AND PRICEEACH is not null
AND ORDERLINENUMBER is not null
AND SALES is not null
AND ORDERDATE is not null)

Select * from public.sales_dataset_rfm_prj_clean

--RFM
--B1: tim cac gia tri RFM

With customer_rfm as(
Select customername,
	current_date - max(orderdate) as R,
	count(ordernumber) as F,
	sum(sales) as M
from public.sales_dataset_rfm_prj_clean
group by customername)

--B2 chia 5 khoang levels 1-5

,rfm_score as(Select 
customername,
ntile(5) OVER(order by R Desc) as R_score, --chia dataset thành 5 khoảng, order by r chia khoảng
ntile(5) over(order by F) as F_score,
ntile(5) over(order by M) as M_score
from customer_rfm)

--B3: rfm combinations

,rfm_final as(
Select customername,
cast(r_score as varchar) || cast(f_score as varchar) || cast(m_score as varchar) as RFM
from rfm_score)
,rfm_results as(
	SELECT	a.customername, a.rfm, b.segment
FROM rfm_final as a
INNER JOIN segment_score as b
	ON a.rfm = b.scores)
	
--B4: count + visualize
Select segment,
count(*)
from rfm_results
group by segment
order by count(*)
