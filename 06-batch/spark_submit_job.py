import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# accept input and output paths as command-line arguments instead of hardcoding
parser = argparse.ArgumentParser()
parser.add_argument('--input_green', required=True)
parser.add_argument('--input_yellow', required=True)
parser.add_argument('--output', required=True)
args = parser.parse_args()

# store parsed arguments in variables
input_green = args.input_green
input_yellow = args.input_yellow
output = args.output

# no .master() here -- spark-submit provides it externally
spark = SparkSession.builder \
    .appName('nyc_taxi_revenue') \
    .getOrCreate()


# TRANSFORMATION LOGIC #

# read green and yello data from the provided input paths
df_green = spark.read.parquet(input_green)
df_yellow = spark.read.parquet(input_yellow)

# rename datetime columns to a common name so both datasets can be unioned
df_green = df_green.withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime').withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')
df_yellow = df_yellow.withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime').withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')

# find columns common to both datasets
common_columns = list(set(df_green.columns).intersection(set(df_yellow.columns)))

# select common columns and tag each row with its source
df_green = df_green.select(common_columns).withColumn('service_type', F.lit('green'))
df_yellow = df_yellow.select(common_columns).withColumn('service_type', F.lit('yellow'))

# combine into one unified DataFrame
df_trips = df_green.unionAll(df_yellow)

# register as temporary view for SQL querying
df_trips.createOrReplaceTempView('trips_data')

# run revenue aggregation query against the combined trips view
df_result = spark.sql("""
SELECT 
    PULocationID AS revenue_zone,
    date_trunc('month', pickup_datetime) AS revenue_month,
    service_type,

    SUM(fare_amount) AS revenue_monthly_fare,
    SUM(total_amount) AS revenue_monthly_total_amount,
    AVG(passenger_count) AS avg_monthly_passenger_count,
    AVG(trip_distance) AS avg_monthly_trip_distance

FROM trips_data
GROUP BY 1, 2, 3
""")

# write result to the provided output path
df_result.coalesce(1).write.mode('overwrite').parquet(output)

print(f"Job complete. Output written to {output}")