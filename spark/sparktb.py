import pyspark
from pyspark.sql import SparkSession
import os
from pyspark.sql import functions as F
from pyspark.sql.types import DateType
from pathlib import Path
from pyspark.sql.functions import date_trunc



spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()
get_ipython().system('wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet')



##q1 - uv run pyspark --version
# version 4.1.1



##q2
df = spark.read \
    .option("header", "true") \
    .parquet('yellow_tripdata_2025-11.parquet')
df = df.repartition(4)
#df.write.parquet('processed', mode = 'overwrite')


# Get Path objects, sort by size attribute, and convert to MB
sorted_files = sorted(
    Path('processed').glob('*.parquet'), 
    key=lambda p: p.stat().st_size, 
    reverse=True
)

for p in sorted_files:
    size_mb = p.stat().st_size / (1024**2)
    print(f"{p.name}: {size_mb:.2f} MB")

##25 MB


##q3
#filter for pickup day = 15th Nov
nov15_df = df.filter(date_trunc("day", "tpep_pickup_datetime") == "2025-11-15")
print(f'No. of trips on nov 15 is {nov15_df.count()}')
#162,604



##q4
# 1. Calculate durations in hours by casting to long (seconds) and dividing by 3600
duration_df = df.withColumn("duration",
    (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 3600)
print(duration_df.select("tpep_pickup_datetime", "tpep_dropoff_datetime", "duration").show(1))
max_duration = duration_df.select(F.max("duration")).collect()[0][0]

print(f"The longest trip duration is {max_duration} hours")
#90.6


##q5
#4040


##q6
get_ipython().system('wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv')
lookup_df = spark.read \
    .option("header", "true") \
    .csv('taxi_zone_lookup.csv')
lookup_df.show(1)
df_enrich = df.join(lookup_df, df.PULocationID == lookup_df.LocationID)
df_enrich.show(1)
##group by zone
##pick up zone with the least count
least_count_row = df_enrich.groupBy('Zone').count().orderBy('count').first()
least_zone_name = least_count_row['Zone']
least_zone_value = least_count_row['count']

print(f"The zone with the least trips is: {least_zone_name} ({least_zone_value} trips)")
#Governor's Island/Ellis Island/Liberty Island



