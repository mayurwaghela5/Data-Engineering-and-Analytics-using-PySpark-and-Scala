from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import to_date

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
spark = SparkSession.builder.appName("MW part-two/minio-read-40").config('spark.driver.host','spark-edge-vm0.service.consul').config(conf=conf).getOrCreate()

# Read the csv datatype into a DataFrame
csvdf = spark.read.csv('s3a://mwaghela/40-csv').cache()

splitDF = csvdf.withColumn('WeatherStation', csvdf['_c0'].substr(5, 6)) \
.withColumn('WBAN', csvdf['_c0'].substr(11, 5)) \
.withColumn('ObservationDate',to_date(csvdf['_c0'].substr(16,8), 'yyyyMMdd')) \
.withColumn('ObservationHour', csvdf['_c0'].substr(24, 4).cast(IntegerType())) \
.withColumn('Latitude', csvdf['_c0'].substr(29, 6).cast('float') / 1000) \
.withColumn('Longitude', csvdf['_c0'].substr(35, 7).cast('float') / 1000) \
.withColumn('Elevation', csvdf['_c0'].substr(47, 5).cast(IntegerType())) \
.withColumn('WindDirection', csvdf['_c0'].substr(61, 3).cast(IntegerType())) \
.withColumn('WDQualityCode', csvdf['_c0'].substr(64, 1).cast(IntegerType())) \
.withColumn('SkyCeilingHeight', csvdf['_c0'].substr(71, 5).cast(IntegerType())) \
.withColumn('SCQualityCode', csvdf['_c0'].substr(76, 1).cast(IntegerType())) \
.withColumn('VisibilityDistance', csvdf['_c0'].substr(79, 6).cast(IntegerType())) \
.withColumn('VDQualityCode', csvdf['_c0'].substr(86, 1).cast(IntegerType())) \
.withColumn('AirTemperature', csvdf['_c0'].substr(88, 5).cast('float') /10) \
.withColumn('ATQualityCode', csvdf['_c0'].substr(93, 1).cast(IntegerType())) \
.withColumn('DewPoint', csvdf['_c0'].substr(94, 5).cast('float')) \
.withColumn('DPQualityCode', csvdf['_c0'].substr(99, 1).cast(IntegerType())) \
.withColumn('AtmosphericPressure', csvdf['_c0'].substr(100, 5).cast('float')/ 10) \
.withColumn('APQualityCode', csvdf['_c0'].substr(105, 1).cast(IntegerType())).drop('_c0')


#csv printschema
splitDF.printSchema()
splitDF.show(10)


JsonSchema = splitDF.coalesce(1).write.format("json").mode("overwrite").save("s3a://mwaghela/40-csvTOjson")
jsondf = spark.read.json("s3a://mwaghela/40-csvTOjson")
#json printschema
jsondf.printSchema()
jsondf.show(10)

ParquetSchema = splitDF.coalesce(1).write.format("parquet").mode("overwrite").save("s3a://mwaghela/40-csvTOparquet")
parquetdf = spark.read.parquet("s3a://mwaghela/40-csvTOparquet")
#parquet printschema
parquetdf.printSchema()
parquetdf.show(10)

