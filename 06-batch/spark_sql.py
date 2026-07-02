from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('spark_sql') \
    .getOrCreate()

df = spark.read.parquet('fhvhv/2021/01/')

df.createOrReplaceTempView('fhvhv_2021_01')

result = spark.sql("""
    SELECT
        PULocationID,
        COUNT(*) AS trip_count,
        COUNT(SR_Flag) AS shared_trips
    FROM fhvhv_2021_01
    GROUP BY PULocationID
    ORDER BY trip_count DESC
    LIMIT 10
""")

result.write.mode('overwrite').parquet('reports/fhvhv_zone_summary/')