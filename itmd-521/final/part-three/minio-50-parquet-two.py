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

# Create SparkSession Object - tell the cluster the FQDN of the host system)
spark = SparkSession.builder.appName("MW part three-two").config('spark.driver.host','spark-edge-vm0.service.consul').config(conf=conf).getOrCreate()

# Read the csv datatype into a DataFrame
parquet_df = spark.read.parquet('s3a://mwaghela/50-parquet')

splitDF = parquet_df.withColumn('WeatherStation', parquet_df['_c0'].substr(5, 6)) \
.withColumn('WBAN', parquet_df['_c0'].substr(11, 5)) \
.withColumn('ObservationDate',to_date(parquet_df['_c0'].substr(16,8), 'yyyyMMdd')) \
.withColumn('ObservationHour', parquet_df['_c0'].substr(24, 4).cast(IntegerType())) \
.withColumn('Latitude', parquet_df['_c0'].substr(29, 6).cast('float') / 1000) \
.withColumn('Longitude', parquet_df['_c0'].substr(35, 7).cast('float') / 1000) \
.withColumn('Elevation', parquet_df['_c0'].substr(47, 5).cast(IntegerType())) \
.withColumn('WindDirection', parquet_df['_c0'].substr(61, 3).cast(IntegerType())) \
.withColumn('WDQualityCode', parquet_df['_c0'].substr(64, 1).cast(IntegerType())) \
.withColumn('SkyCeilingHeight', parquet_df['_c0'].substr(71, 5).cast(IntegerType())) \
.withColumn('SCQualityCode', parquet_df['_c0'].substr(76, 1).cast(IntegerType())) \
.withColumn('VisibilityDistance', parquet_df['_c0'].substr(79, 6).cast(IntegerType())) \
.withColumn('VDQualityCode', parquet_df['_c0'].substr(86, 1).cast(IntegerType())) \
.withColumn('AirTemperature', parquet_df['_c0'].substr(88, 5).cast('float') /10) \
.withColumn('ATQualityCode', parquet_df['_c0'].substr(93, 1).cast(IntegerType())) \
.withColumn('DewPoint', parquet_df['_c0'].substr(94, 5).cast('float')) \
.withColumn('DPQualityCode', parquet_df['_c0'].substr(99, 1).cast(IntegerType())) \
.withColumn('AtmosphericPressure', parquet_df['_c0'].substr(100, 5).cast('float')/ 10) \
.withColumn('APQualityCode', parquet_df['_c0'].substr(105, 1).cast(IntegerType())).drop('_c0')


#csv printschema
splitDF.printSchema()
splitDF.select("WeatherStation", "VisibilityDistance", "ObservationDate").where(col("VisibilityDistance") < 200).groupBy("WeatherStation", "VisibilityDistance", year("ObservationDate")).count().orderBy(desc("VisibilityDistance")).show(10)