from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import to_date
from pyspark.sql.functions import col
from pyspark.sql.functions import desc
from pyspark.sql.functions import year

# Removing hard coded password - using os module to import them
import os
import sys

# Required configuration to load S3/Minio access credentials securely - no hardcoding keys into code
conf = SparkConf()
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.3')
conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider')

conf.set('spark.hadoop.fs.s3a.access.key', os.getenv('SECRETKEY'))
conf.set('spark.hadoop.fs.s3a.secret.key', os.getenv('ACCESSKEY'))
conf.set("spark.hadoop.fs.s3a.endpoint", "http://minio1.service.consul:9000")
conf.set("fs.s3a.path.style.access", "true")
conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
conf.set("fs.s3a.connection.ssl.enabled", "false")


spark = SparkSession.builder.appName("MW part three-one").config('spark.driver.host','spark-edge-vm0.service.consul').config(conf=conf).getOrCreate()

# Read the parquet datatype into a DataFrame

parquet_df = spark.read.format("parquet").option("header", "true").option("inferSchema", "true").load('s3a://mwaghela/50-parquet')


#csv printschema
parquet_df.printSchema()
parquet_df.select("WeatherStation", "VisibilityDistance", "ObservationDate").where(col("VisibilityDistance") < 200).groupBy("WeatherStation", "VisibilityDistance", year("ObservationDate")).count().orderBy(desc("VisibilityDistance")).show(20)