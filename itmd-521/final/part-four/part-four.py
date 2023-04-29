from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType,StringType,DateType, FloatType, StructType, StructField
from pyspark.sql.functions import to_date
from pyspark.sql.functions import col
from pyspark.sql.functions import desc
from pyspark.sql.functions import year,month,avg
from pyspark.sql.types import *
from pyspark.sql.functions import *

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

spark = SparkSession.builder.appName("MW part Four").config('spark.driver.host','spark-edge-vm0.service.consul').config(conf=conf).getOrCreate()

# Read the parquet datatype into a DataFrame

parquetdf = spark.read.parquet("s3a://mwaghela/60-parquet", header=True, schema=schema)   
parquetdf.createOrReplaceTempView("parquetdf_view")

# Count the number of records
Number_of_records = spark.sql(""" SELECT year(ObservationDate) As Year, count(*)
                                    FROM parquetdf_view
                                    WHERE Month(ObservationDate) = '2'
                                    group by Year
                                    order by Year desc;
                                 """
                                )
Number_of_records.show(20)
        
# Average air temperature
average_air_temp = spark.sql("""  SELECT year(ObservationDate) As Year, AVG(AirTemperature) AS AvgAirTemp
                                FROM parquetdf_view
                                WHERE Month(ObservationDate) = '2'
                                AND AirTemperature < 999 AND AirTemperature > -999
                                group by Year
                                order by Year desc;
                                """
                                )
average_air_temp.show(20)

#schema2 = StructType([
#StructField('Year', IntegerType(), True),
#StructField('AvgAirTemp', FloatType(), True),])

#average_air_temp.write.format('parquet').mode('overwrite').schema(schema2).save('s3a://mwaghela/MW-part-four-answers-parquet')
#average_air_temp.show(20)

                                   
# Median air temperature
median_air_temp = parquetdf.approxQuantile('AirTemperature', [0.5], 0.25)
print(f"Median Air_Temperature:{median_air_temp}")

#Standard_Deviation of Air temperature
STD_DEV_AIR_TEMP = spark.sql(""" SELECT year(ObservationDate) As Year, std(AirTemperature) as Standard_deviation
                                    FROM parquetdf_view
                                    WHERE Month(ObservationDate) = '2'
                                    AND AirTemperature < 999 AND AirTemperature > -999
                                    group by Year
                                    order by Year desc;
                                 """
                                )
STD_DEV_AIR_TEMP.show(20)

#Find AVG air temperature per StationID in the month of February

february_data = parquetdf.filter(month("ObservationDate") == 2)
avg_temps = february_data.groupBy("StationID").agg(avg("AirTemperature"))

avg_temps.show(20)