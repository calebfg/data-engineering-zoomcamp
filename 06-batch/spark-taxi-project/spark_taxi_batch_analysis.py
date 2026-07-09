from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('taxi_batch_analysis') \
    .getOrCreate()

#print(spark.version)

# read the raw parquet file
df = spark.read.parquet('data/pq/yellow/2025/11/yellow_tripdata_2025-11.parquet')
df.show(5)

# trips on Nov 15
trips_nov_15 = df.filter(F.to_date(F.col('tpep_pickup_datetime')) == '2025-11-15').count()
print(f"Trips on Nov 15: {trips_nov_15}")

#Longest trip in hours
df = df.withColumn('duration_hours', 
                   (F.unix_timestamp('tpep_dropoff_datetime') - F.unix_timestamp('tpep_pickup_datetime'))
                     / 3600)
df.select(F.max('duration_hours')).show()


    # LEAST FREQUENT LOCATION 

# Count trips per pickup location
pickup_counts = df.groupBy('PULocationID').count()
# Read the zone lookup table
zones = spark.read.option("header", "true").csv('data/taxi_zone_lookup.csv')
# Join the counts with the zone names
result = pickup_counts.join(zones, pickup_counts.PULocationID == zones.LocationID)  
# Sort by count ascending (smallest First) and show the least frequent pickup locations
result.orderBy('count').select('Zone', 'count').show(1)

    # LEAST FREQUENT LOCATION 



# repartition to 4 partitions
#df = df.repartition(4)

# write to a NEW output folder (different from the input)
#df.write.mode('overwrite').parquet('data/pq/yellow/2025/11/repartitioned')