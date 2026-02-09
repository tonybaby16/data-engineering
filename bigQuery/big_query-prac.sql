--q1
--Creating external table
CREATE OR REPLACE EXTERNAL TABLE nytaxi.y2024external
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://demo_bucket_project-88010cf5-939d-4e44-92f/yellow_tripdata_2024-*.parquet']
);

SELECT COUNT(*) FROM `nytaxi.y2024external`; --20,332,093


-- Creating a regular table
--LOAD DATA INTO nytaxi.y2024
--FROM FILES (
--  format = 'PARQUET',
 -- uris = ['gs://demo_bucket_project-88010cf5-939d-4e44-92f/yellow_tripdata_2024-*.parquet']
--);

SELECT COUNT(*) FROM `nytaxi.y2024` ;--20,332,093

--q2
select distinct PULocationID from `nytaxi.y2024external`; --0

select distinct PULocationID from `nytaxi.y2024`;--155.12 MB

--q3
select PULocationID from `nytaxi.y2024`; --155.12 MB
select PULocationID,DOLocationID from `nytaxi.y2024`; --310.24
--Option A

--q4
select count(*) from nytaxi.y2024
where fare_amount = 0;
--8,333
select count(*) from nytaxi.y2024
where fare_amount is null;

--q5
--Option A
CREATE OR REPLACE TABLE nytaxi.y2024_part
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID
AS
SELECT * FROM nytaxi.y2024;

--q6
--Option B
select distinct VendorID
from nytaxi.y2024
where tpep_dropoff_datetime between '2024-03-01' and '2024-03-15'; --1,6,2 --310.24 MB

select distinct VendorID
from `nytaxi.y2024_part`
where tpep_dropoff_datetime between '2024-03-01' and '2024-03-15'; -- 26.84 MB

--q7
--GCP Bucket

--q8
--False

--q9
SELECT COUNT(*) FROM `nytaxi.y2024` ;--0B