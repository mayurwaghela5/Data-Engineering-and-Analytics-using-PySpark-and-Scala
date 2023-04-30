from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType,StructType, StructField,DateType, FloatType, DoubleType, StringType
from pyspark.sql.functions import to_date
from pyspark.sql.functions import col
from pyspark.sql.functions import desc
from pyspark.sql.functions import year
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import year, month, col,avg

# Removing hard coded password - using os module to import them
import os
import sys

#defining file to save answer
queryAnswerFile = "s3a://mgowda2/MDG-part-four-answers-parquet"

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

spark = SparkSession.builder.appName("MW part Four 1").config('spark.driver.host','spark-edge-vm0.service.consul').config(conf=conf).getOrCreate()

structSchema = StructType([StructField('WeatherStation', StringType(), True),
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

parquetdf = spark.read.parquet("s3a://mwaghela/60-parquet", header=True, schema=structSchema)   
parquetdf.createOrReplaceTempView("sqlView")


#Count the number of records
countOfRecords = spark.sql(""" SELECT year(ObservationDate) As Year, count(*)
                                    FROM sqlView
                                    WHERE Month(ObservationDate) = '2'
                                    group by Year
                                    order by Year desc;
                                 """
                                )
countOfRecords.show(20)

#Average air temperature for month of February
averageAirTemp = spark.sql("""  SELECT year(ObservationDate) As Year, AVG(AirTemperature) AS AvgAirTemperature
                                FROM sqlView
                                WHERE Month(ObservationDate) = '2'
                                AND AirTemperature < 999 AND AirTemperature > -999
                                group by Year
                                order by Year desc;
                                """
                                )
averageAirTemp.show(20)

                                   
#Median air temperature for month of February
medianAirTemp = parquetdf.approxQuantile('AirTemperature', [0.5], 0.25)
print(f"Median air temmp:{medianAirTemp}")

#Standard Deviation of air temperature for month of February
standardAirTemp = spark.sql(""" SELECT year(ObservationDate) As Year, std(AirTemperature) as Standard_deviation
                                    FROM sqlView
                                    WHERE Month(ObservationDate) = '2'
                                    AND AirTemperature < 999 AND AirTemperature > -999
                                    group by Year
                                    order by Year desc;
                                 """
                                )
standardAirTemp.show(20)

#Find AVG air temperature per StationID in the month of February
#removing legal but not real values from Air temperature

february_data = parquetdf.filter(month("ObservationDate") == 2)
filtered_df = february_data.filter(february_data.AirTemperature < 150)
avg_temps = filtered_df.groupBy("WeatherStation").agg(avg("AirTemperature"))

avg_temps.show(20)


#creating schema and writing df in parquet file
schema1=StructType([
StructField('Year', DateType(), True),
StructField('Count(1)', IntegerType(), True),
])


countOfRecords.write.format('parquet').mode('append').save("s3a://mwaghela/MW-part-four-answers-parquet",schema=schema1)


schema2=StructType([
StructField('Year', DateType(), True),
StructField('AvgAirTemperature', DoubleType(), True),
])

averageAirTemp.write.format('parquet').mode('append').save("s3a://mwaghela/MW-part-four-answers-parquet",schema=schema2)

schema3=StructType([
StructField('Year', DateType(), True),
StructField('Standard_deviation', DoubleType(), True),
])

standardAirTemp.write.format('parquet').mode('append').save("s3a://mwaghela/MW-part-four-answers-parquet",schema=schema3)

schema4=StructType([
StructField('WeatherStation', StringType(), True),
StructField('AirTemperature', DoubleType(), True),
])

avg_temps.write.format('parquet').mode('append').save("s3a://mwaghela/MW-part-four-answers-parquet",schema=schema4)