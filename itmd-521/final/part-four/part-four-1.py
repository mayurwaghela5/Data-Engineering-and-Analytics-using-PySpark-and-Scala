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
conf.set("spark.hadoop.fs.s3a.bucket.all.committer.magic.enabled","true")

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

parquetdf = spark.read.parquet("s3a://mwaghela/60-parquet",header=True, schema=structSchema)   
parquetdf.createOrReplaceTempView("sqlView")


#Count the number of records
countOfRecords = spark.sql(""" SELECT year(ObservationDate) As Year, count(*) As NoOfRecords
                                    FROM sqlView
                                    WHERE Month(ObservationDate) = '2'
                                    group by Year
                                    order by Year desc;
                                 """
                                ).show(20)

#checking result parquet
#res_parquetdf1 = spark.read.parquet("s3a://mwaghela/MW-part-four-answers-count-parquet")   
#print("------------Reading from Parquet result file-------------------------")
#res_parquetdf1.show(20)




#Average air temperature for month of February
averageAirTemp = spark.sql("""  SELECT year(ObservationDate) As Year, AVG(AirTemperature) AS AvgAirTemperature
                                FROM sqlView
                                WHERE Month(ObservationDate) = '2'
                                AND AirTemperature < 999 AND AirTemperature > -999
                                group by Year
                                order by Year desc;
                                """
                                ).show(20)


#checking result parquet
#res_parquetdf2 = spark.read.parquet("s3a://mwaghela/MW-part-four-answers-avg-parquet-")   
#print("------------Reading from Parquet result file-------------------------")
#res_parquetdf2.show(20)

    
    
                                   
#Median air temperature for month of February
medianAirTemp = parquetdf.approxQuantile('AirTemperature', [0.5], 0.25)
print(f"Median air temmp:{medianAirTemp}")
#averageAirTemp.write.format('parquet').mode('overwrite').parquet("s3a://mwaghela/MW-part-four-answers-median-parquet-")
#res_parquetdf0 = spark.read.parquet("s3a://mwaghela/MW-part-four-answers-median-parquet-")   
#print("------------Reading from Parquet result file-------------------------")
#res_parquetdf0.show(20)



#Standard Deviation of air temperature for month of February
standardAirTemp = spark.sql(""" SELECT year(ObservationDate) As Year, std(AirTemperature) as Standard_deviation
                                    FROM sqlView
                                    WHERE Month(ObservationDate) = '2'
                                    AND AirTemperature < 999 AND AirTemperature > -999
                                    group by Year
                                    order by Year desc;
                                 """
                                ).show(20)

#standardAirTemp.write.format('parquet').mode('overwrite').parquet("s3a://mwaghela/MW-part-four-answers-sdtdev-parquet")
#res_parquetdf3 = spark.read.parquet("s3a://mwaghela/MW-part-four-answers-sdtdev-parquet")   
#print("------------Reading from Parquet result file-------------------------")
#res_parquetdf3.show(20)



#remove illegal values

filtered_df = parquetdf.filter(parquetdf.AirTemperature < 160.0 & parquetdf.AirTemperature > -130.0)
filtered_df1=filtered_df.filter(parquetdf.WeatherStation !=999999)

#Find AVG air temperature per StationID in the month of February
february_data = filtered_df1.filter(month("ObservationDate") == 2)
avg_temps = february_data.groupBy("WeatherStation").agg(avg("AirTemperature"))
avg_temps.show(20)


#avg_temps.write.format('parquet').mode('overwrite').parquet("s3a://mwaghela/MW-part-four-answers-stationid-avg-parquet")

#results read
#res_parquetdf3 = spark.read.parquet("s3a://mwaghela/MW-part-four-answers-stationid-avg-parquet")  
#print("------------Reading from Parquet result file-------------------------")
#res_parquetdf3.show(20)


countOfRecords.write.format("parquet").mode("overwrite").parquet("s3a://mwaghela/MW-part-four-answers-count-parquet")

averageAirTemp.write.format("parquet").mode("overwrite").parquet("s3a://mwaghela/MW-part-four-answers-avg-parquet-")

standardAirTemp.write.format("parquet").mode("overwrite").parquet("s3a://mwaghela/MW-part-four-answers-sdtdev-parquet")

avg_temps.write.format("parquet").mode("overwrite").parquet("s3a://mwaghela/MDG-part-four-answers-station-parquet")

medianAirTemp.write.format("parquet").mode("overwrite").parquet("s3a://mwaghela/MW-part-four-answers-median-parquet-")

