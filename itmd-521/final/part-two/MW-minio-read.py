from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import to_date
from pyspark.sql.types import *
from pyspark.sql.functions import *
 
# Removing hard coded password - using os module to import them
import os
import sys
 
# Required configuration to load S3/Minio access credentials securely - no hardcoding keys into code
conf = SparkConf()
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.0')
conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider')
conf.set('spark.hadoop.fs.s3a.access.key', os.getenv('SECRETKEY'))
conf.set('spark.hadoop.fs.s3a.secret.key', os.getenv('ACCESSKEY'))
conf.set("spark.hadoop.fs.s3a.endpoint", "http://minio1.service.consul:9000")
conf.set("fs.s3a.path.style.access", "true")
conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
conf.set("fs.s3a.connection.ssl.enabled", "false")
 
#Create a schema
schema = StructType([StructField('WeatherStation', StringType(), True),
StructField('WBAN', StringType(), True),
StructField('ObservationDate',  DateType(), True),
StructField('ObservationHour', IntegerType(), True),
StructField('Latitude', FloatType(), True),
StructField('Longitude', FloatType(), True),
StructField('Elevation', IntegerType(), True),
StructField('WindDirection', IntegerType(), True),
StructField('WDQualityCode', IntegerType(), True),
StructField('SkyCeilingHeight', IntegerType(), True),
StructField('SCQualityCode', IntegerType(), True),
StructField('VisibilityDistance', IntegerType(), True),
StructField('VDQualityCode', IntegerType(), True),
StructField('AirTemperature', FloatType(), True),
StructField('ATQualityCode', IntegerType(), True),
StructField('DewPoint', FloatType(), True),
StructField('DPQualityCode', IntegerType(), True),
StructField('AtmosphericPressure', FloatType(), True),
StructField('APQualityCode', IntegerType(), True)])
 
spark_session = SparkSession.builder.appName("MW-minio-read").config('spark.driver.host','spark-edge-vm0.service.consul').config(conf=conf).getOrCreate()
 
#Read partitioned csv
cachedf = spark_session.read.csv("s3a://mwaghela/30-csv", header=True, schema=schema)
 

csvdf = cachedf
#Printschema
csvdf.printSchema()
#Displayschema
csvdf.show(10)
 
print("-------------------- Writing CSV to JSON ----------------------------")
cachedf.write.format("json").option("header", "true").mode("overwrite").save("s3a://mwaghela/30-part-two-json")
jsondf = spark_session.read.schema(schema).json("s3a://mwaghela/30-part-two-json")
#Printschema
print("Print JSON Schema")
jsondf.printSchema()
#Displayschema
print("Display JSONDF")
jsondf.show(10)
 
print("---------------Writing CSV to PARQUET ----------------------")
cachedf.write.format("parquet").option("header", "true").mode("overwrite").save("s3a://mwaghela/30-part-two-parquet")
parquetdf = spark_session.read.schema(schema).parquet("s3a://mwaghela/30-part-two-parquet")
#Printschema
print("Print PARQUET Schema")
parquetdf.printSchema()
#Displayschema
print("Display PARQUETDF")
parquetdf.show(10)