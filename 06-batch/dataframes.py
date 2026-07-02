from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('dataframes') \
    .getOrCreate()

df=spark.read.parquet('fhvhv/2021/01/')

#df.filter(F.to_date(F.col('pickup_datetime')) == '2021-01-15')\
#    .select('pickup_datetime', 'dropoff_datetime', 'PULocationID', 'DOLocationID')\
#    .show()

#df = df.withColumn('pickup_date', F.to_date(F.col('pickup_datetime')))
#df.show()

def classify_dispatch(base_num):
    num = int(base_num[1:])
    if num % 7 == 0:
        return 'priority'
    else:
        return 'standard'
    
classify_dispatch_udf = F.udf(classify_dispatch, types.StringType())
df = df.withColumn('dispatch_type', classify_dispatch_udf(F.col('dispatching_base_num')))
df.select('dispatching_base_num', 'dispatch_type').show()