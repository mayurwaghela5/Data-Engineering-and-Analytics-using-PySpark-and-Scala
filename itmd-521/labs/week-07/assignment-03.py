import sys
from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.types import StructType, StructField, IntegerType,StringType,BooleanType,FloatType
from pyspark.sql.functions import expr,col


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(-1)
        
    spark=(SparkSession.builder.appName("assignment-03").getOrCreate())
    data_source_file=sys.argv[1]
    #date,delay,distance,origin,destination
    schema_ddl="date INT,delay INT,distance INT,origin STRING,destination STRING"
    
    
    df = (spark.read.schema(schema_ddl).format("csv")).option("header", "true").load(data_source_file)
    df.printSchema()
    df.show()
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
    
    
    