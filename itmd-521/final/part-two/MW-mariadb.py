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

spark_session = SparkSession.builder.appName("MW-mariadb").config('spark.driver.host','spark-edge-vm0.service.consul').config(conf=conf).getOrCreate()

connection_properties = {
    "user": os.getenv('MYSQLUSER'),
    "password": os.getenv('MYSQLPASS'),
    "driver": "com.mysql.cj.jdbc.Driver"
}

df = spark_session.read.jdbc(url="jdbc:mysql://database-240-vm0.service.consul:3306/ncdc",table="thirties",properties=connection_properties) 
      
df.show(10)
df.printSchema()
