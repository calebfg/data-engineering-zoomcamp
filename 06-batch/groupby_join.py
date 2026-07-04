# entry point to everything in Spark
from pyspark.sql import SparkSession
# built-in Spark functions library
from pyspark.sql import functions as F

# create local Spark session using all available cores
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('groupby_join') \
    .getOrCreate()

# read all green and yellow parquet files across all years and months
df_green = spark.read.parquet('data/pq/green/*/*')
df_yellow = spark.read.parquet('data/pq/yellow/*/*')

#print(df_green.columns)
#print(df_yellow.columns)

# rename datetime columns to a common name so both datasets can be unioned
df_green = df_green.withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')

df_yellow = df_yellow.withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')

# find columns that exist in both datasets — excludes ehail_fee, trip_type, airport_fee
common_columns = list(set(df_green.columns).intersection(set(df_yellow.columns)))
#print(common_columns)

# select only common columns and tag each row with its source dataset
df_green = df_green.select(common_columns).withColumn('service_type', F.lit('green'))
df_yellow = df_yellow.select(common_columns).withColumn('service_type', F.lit('yellow'))

# stack green and yellow into one unified DataFrame
df_trips = df_green.unionAll(df_yellow)

# register as temporary view so spark.sql() can query it by name
df_trips.createOrReplaceTempView('trips_data')


#run revenue aggregation query against the combined trips view
df_result = spark.sql (""" 
    SELECT
        PULocationID AS revenue_zone,
        date_trunc('month', pickup_datetime) AS revenue_month,
        service_type,
                       
        -- revenue calculations
        SUM(fare_amount) AS revenue_monthly_fare,
        SUM(extra) AS revenue_monthly_extra,
        SUM(mta_tax) AS revenue_monthly_mta_tax,
        SUM(tip_amount) AS revenue_monthly_tip_amount,
        SUM(tolls_amount) AS revenue_monthly_tolls_amount,
        SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
        SUM(total_amount) AS revenue_monthly_total_amount,
        SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge,
                       
        -- additional metrics
        AVG(passenger_count) AS avg_monthly_passenger_count,
        AVG(trip_distance) AS avg_monthly_trip_distance
        
    FROM trips_data
    GROUP BY 1, 2, 3
""")

# register revenue results as a view so we can join it with zones
df_result.createOrReplaceTempView('revenue')

# read the small zone lookup table - only 265 rows
df_zones = spark.read\
    .option("header", "true") \
    .csv('data/taxi_zone_lookup.csv')

# register zones as a temporary view so we can query it with SQL
df_zones.createOrReplaceTempView('zones')

# join revenue results with zone names  - Spark will automatically
# use braodcast join since zones table is tiny (265 rows)
df_result_joined = spark.sql("""
    SELECT
        r.revenue_zone,
        z.Zone AS revenue_zone_name,
        z.Borough AS borough,
        r.revenue_month,
        r.service_type,
        r.revenue_monthly_total_amount,
        r.avg_monthly_trip_distance
    FROM revenue r
    LEFT JOIN zones z ON r.revenue_zone = z.LocationID
                    
""")

# reduce to 1 partition since result is small, then write to parquet
#df_result.coalesce(1).write.mode('overwrite').parquet('data/report/revenue/')

df_result_joined.show(5)


# write final enriched result to parquet
df_result_joined.coalesce(1).write.mode('overwrite').parquet('data/report/revenue/')

# keep spark session alive to inspect Spark UI at localhost:4040
input("Press Enter to exit...")