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
conf.set('spark.jars.packages','com.mysql:mysql-connector-j:8.0.32')
conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider')
conf.set('spark.hadoop.fs.s3a.access.key', os.getenv('SECRETKEY'))
conf.set('spark.hadoop.fs.s3a.secret.key', os.getenv('ACCESSKEY'))
conf.set("spark.hadoop.fs.s3a.endpoint", "http://minio1.service.consul:9000")
conf.set("fs.s3a.path.style.access", "true")
conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
conf.set("fs.s3a.connection.ssl.enabled", "false")
conf.set("spark.hadoop.fs.s3a.bucket.all.committer.magic.enabled","true")
 
spark = SparkSession.builder.appName("MW-minio-read").config('spark.driver.host','spark-edge-vm0.service.consul').config(conf=conf).getOrCreate()
 
#Create a schema

spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")    
structSchema =  StructType([StructField('WeatherStation', StringType(), True),
                    StructField('WBAN', StringType(), True),
                    StructField('ObservationDate', DateType(), True),
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
                    StructField('ATQualityCode', FloatType(), True),
                    StructField('DewPoint', FloatType(), True),
                    StructField('DPQualityCode', DoubleType(), True),
                    StructField('AtmosphericPressure', FloatType(), True)])

dataFrame = spark.read.csv("s3a://mwaghela/30-csv", header=True, schema=structSchema).cache()
 
csvdf = dataFrame
csvdf.na.drop()  
print("CSV Schema")
csvdf.printSchema()
print("Display data---CSV")
csvdf.show(10)
 
#Converting to json 
dataFrame.write.format("json").option("header", "true").mode("overwrite").save("s3a://mwaghela/30-part-two-json")
jsondf = spark.read.schema(structSchema).json("s3a://mwaghela/30-part-two-json")
print("JSON Schema")
jsondf.printSchema()
print("Display result---JSON")
jsondf.show(10)
 
#Converting to parquet
dataFrame.write.format("parquet").option("header", "true").mode("overwrite").save("s3a://mwaghela/30-part-two-parquet")
parquetdf = spark.read.schema(structSchema).parquet("s3a://mwaghela/30-part-two-parquet")
print("Parquet Schema")
parquetdf.printSchema()
print("Display result----parquet")
parquetdf.show(10)


#loading parrquet dataframe to Maria DB
(dataFrame.write.format("jdbc").option("url","jdbc:mysql://database-240-vm0.service.consul:3306/ncdc").option("driver","com.mysql.cj.jdbc.Driver").option("dbtable","MW_thirty").option("user",os.getenv('MYSQL_USER')).option("truncate",True).mode("overwrite").option("password", os.getenv('MYSQL_PASS')).save())


df1=(spark.read.format("jdbc").option("url","jdbc:mysql://database-240-vm0.service.consul:3306/ncdc").option("driver","com.mysql.cj.jdbc.Driver").option("dbtable","MW_thirty").option("user",os.getenv('MYSQL_USER')).option("truncate",True).option("password", os.getenv('MYSQL_PASS')).load())
df1.show(10)
df1.printSchema()


