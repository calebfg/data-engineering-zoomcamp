import pyspark
import os
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import types

# path to your GCP service account key
credentials_location = '/workspaces/docker-workshop/keys/dezc-project-496112-91723f7c4b57.json'

# configure spark to use the GCS connector JAR and authenticate with GCP
conf = SparkConf() \
    .setMaster('local[*]') \
    .setAppName('spark_gcs') \
    .set("spark.jars", "./lib/gcs-connector-hadoop3-latest.jar") \
    .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_location) \
    .set("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .set("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
    .set("spark.hadoop.fs.gs.auth.service.account.json.keyfile", credentials_location) \
    .set("spark.hadoop.fs.gs.auth.service.account.enable", "true")

# create SparkContext from config — lower level than SparkSession
sc = SparkContext(conf=conf)

# tell Spark which implementation handles gs:// URLs
hadoop_conf = sc._jvm.org.apache.hadoop.conf.Configuration()
hadoop_conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", credentials_location)
hadoop_conf.set("fs.gs.auth.service.account.enable", "true")

# create SparkSession from the existing SparkContext
spark = SparkSession.builder \
    .config(conf=sc.getConf()) \
    .getOrCreate()

# read green data and upload to GCS
df_green = spark.read.parquet('data/pq/green/*/*')
df_green.write.mode('overwrite').parquet('gs://dezc-project-496112-terra-bucket/pq/green/')
print("Green upload complete. Starting yellow...")

# read yellow month by month to handle airport_fee type inconsistency across files
yellow_dfs = []

for year in ['2020', '2021']:
    for month in [f'{m:02d}' for m in range(1, 13)]:
        path = f'data/pq/yellow/{year}/{month}'
        if os.path.exists(path):
            df = spark.read.parquet(path)
            # safely cast airport_fee to DOUBLE only if column exists
            if 'airport_fee' in df.columns:
                df = df.withColumn('airport_fee', F.col('airport_fee').cast(types.DoubleType()))
            else:
                df = df.withColumn('airport_fee', F.lit(None).cast(types.DoubleType()))
            yellow_dfs.append(df)
            print(f"Loaded yellow {year}-{month}")

# guard against empty file list
if not yellow_dfs:
    raise Exception("No yellow parquet files found.")

# union all months together into one DataFrame
df_yellow = yellow_dfs[0]
for df in yellow_dfs[1:]:
    df_yellow = df_yellow.unionByName(df)

# write unified yellow DataFrame to GCS
df_yellow.write.mode('overwrite').parquet('gs://dezc-project-496112-terra-bucket/pq/yellow/')
print("Yellow upload complete!")

input("Press Enter to exit...")