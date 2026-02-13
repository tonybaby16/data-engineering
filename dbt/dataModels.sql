--q1
--q2
--q3
SELECT count(revenue_month) FROM `dbt_tbaby.fct_monthly_zone_revenue`;--9963

--q4
select pickup_zone, sum(revenue_monthly_total_amount) as item
FROM `dbt_tbaby.fct_monthly_zone_revenue` 
where service_type = 'Green'
and extract (YEAR from revenue_month) = 2020
group by pickup_zone
order by item desc
limit 5; --East Harlem North = 1817556.25

--q5
select sum(total_monthly_trips)
from dbt_tbaby.fct_monthly_zone_revenue
where  service_type = 'Green'
and revenue_month = '2019-10-01';

select count(*)
from dbt_tbaby.fct_trips
where  service_type = 'Green'
and extract(year from pickup_datetime) = 2019
and extract(month from pickup_datetime) = 10;

--384,624

--q6
LOAD DATA INTO nytaxi.fhv_tripdata
FROM FILES (
  format = 'CSV',
  uris = ['gs://project/fhv/fhv_tripdata_2019-*.csv.gz']
);

select count(*) from dbt_tbaby.stg_fhv_tripdata;--43244693
