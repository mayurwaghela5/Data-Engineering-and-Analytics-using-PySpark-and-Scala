import sys
from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.types import StructType, StructField, IntegerType,StringType,BooleanType,FloatType
from pyspark.sql.functions import expr,col,from_unixtime,unix_timestamp


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(-1)
        
    spark=(SparkSession.builder.appName("assignment-03").config("spark.sql.catalogImplementation","hive").getOrCreate())
    data_source_file=sys.argv[1]
    #date,delay,distance,origin,destination
    schema_ddl="date STRING,delay INT,distance INT,origin STRING,destination STRING"
    
    
    
    
    df = (spark.read.schema(schema_ddl).format("csv")).option("header", "true").load(data_source_file)
    df.printSchema()
    #df.show()
    df = df.withColumn("dateMonth", from_unixtime(unix_timestamp(df.date, "MMddHHmm"), "MM")).withColumn("dateDay", from_unixtime(unix_timestamp(df.date, "MMddHHmm"), "dd"))
    df.createOrReplaceTempView("us_delay_flights_tbl")
    
    #Part 1
    #Spark Sql examples on Page 87 
    
    #1
    spark.sql("""SELECT distance, origin, destination
    FROM us_delay_flights_tbl WHERE distance > 1000
    ORDER BY distance DESC""").show(10)
    
    
    #2
    spark.sql("""SELECT date, delay, origin, destination
    FROM us_delay_flights_tbl
    WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD'
    ORDER by delay DESC""").show(10)
    
    #3
    spark.sql("""SELECT delay, origin, destination,
    CASE
    WHEN delay > 360 THEN 'Very Long Delays'
    WHEN delay > 120 AND delay < 360 THEN 'Long Delays'
    WHEN delay > 60 AND delay < 120 THEN 'Short Delays'
    WHEN delay > 0 and delay < 60 THEN 'Tolerable Delays'
    WHEN delay = 0 THEN 'No Delays'
    ELSE 'Early'
    END AS Flight_Delays
    FROM us_delay_flights_tbl
    ORDER BY origin, delay DESC""").show(10)
    
    #PySpark DataFrame API
    #1
    df.where(df.distance > 1000).select('distance', 'origin', 'destination').orderBy('distance', ascending=False).show(10)
    
    #2
    df.filter((df.delay>120)&(df.origin=='SFO')&(df.destination=='ORD')).select('date','delay','origin','destination').orderBy('delay',ascending=False).show(10)
    
    #3
    df0 = df.select('delay','origin','destination',expr("CASE WHEN delay > 360 THEN 'Long Delays'  \
                                     WHEN delay > 120 AND delay < 360 THEN 'Long Delays' \
                                     WHEN delay > 60 AND delay < 120 THEN 'Short Delays' \
                                     WHEN delay > 0 and delay < 60 THEN 'Tolerable Delays' \
                                     WHEN delay = 0 THEN 'No Delays' \
                                     ELSE 'Early' END AS Flight_Delays"))
    
    df0.sort(col('origin'),col('delay').desc()).show(10)
    
    #----------------------------------------------------------------------------------------------
    
    #Part2
    
    #create a Table named us_delay_flights_tbl from the departuredelay.csv
    schema_ddl="date STRING,delay INT,distance INT,origin STRING,destination STRING"
    flight_df = spark.read.csv(data_source_file, schema=schema_ddl)
    
    #spark.conf.set("spark.sql.catalogImplementation","hive")
    
    flight_df.write.mode("overwrite").saveAsTable("us_delay_flights_tbl")
    #option("path","./spark-warehouse")
    
    #Create a tempView of all flights with an origin of Chicago (ORD) and a month/day combo of between 03/01 and 03/15
    
    df_tempView=spark.sql("SELECT date,dateMonth,dateDay, delay, origin, destination FROM us_delay_flights_tbl where ORIGIN  like 'ORD' AND dateMonth = 03 AND dateDay >=1 AND dateDay <=15")
    #df_tempView.show(5)   
    
    #tempview created or replaced
    df_tempView.createOrReplaceGlobalTempView("us_delay_flights_tbl_tempview")
    
    #querying on the tempview
    tempviewquery = spark.sql("SELECT date, dateMonth,dateDay,delay, origin, destination  \
                          from global_temp.us_delay_flights_tbl_tempview")
    #Show the first 5 records of the tempView
    tempviewquery.show(5)
    
    #Using the Spark Catalog to list the columns of the tempView
    print(spark.catalog.listColumns(dbName='default', tableName='us_delay_flights_tbl'))
    
    #----------------------------------------------------------------------------------------------
    
    #Part 3
    #read the file into a dataframe
    df3 = (spark.read.schema(schema_ddl).format("csv")).option("header", "true").load(data_source_file)
    
    #Using a DataFrameWriter, write the content out as JSON
    (df3.write.format("json").mode("overwrite").option("compression", "none").json("./df_json_withoutsnappyPython"))
    
    #Using a DataFrameWriter, write the content out as JSON with snappy/lz4
    (df3.write.format("json").mode("overwrite").option("compression", "lz4").save("./df_json_withsnappyPython"))
    
    
    #Using a DataFrameWriter, write the content out as PARQUET
    (df3.write.format("parquet").mode("overwrite").option("compression", "snappy").parquet("./df_json_withParquetPython"))
    
    #----------------------------------------------------------------------------------------------
    
    #Part 4
    #departuredelays parquet file created in part 3 source file location
    parquet_data_file="./spark-warehouse/df_json_withParquet"
    #reading the parquet file
    df4 = spark.read.parquet(parquet_data_file)
    #query of dataframe
    orddeparturedelays=df4.select('date','delay','distance','origin','destination').filter(df4.origin=='ORD')
    #write the results to a DataFrameWriter named orddeparturedelays
    orddeparturedelays.write.format("parquet").mode("overwrite").option("compression", "snappy").save("./part4_ORDdeparturedelaysPython")
    orddeparturedelays.show(10)
    
    
    
    
    


    