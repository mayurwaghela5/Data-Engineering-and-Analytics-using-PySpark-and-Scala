import sys
from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.types import StructType, StructField, IntegerType,StringType,BooleanType,FloatType


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(-1)
        
    spark=(SparkSession.builder.appName("assignment-03").getOrCreate())
    data_source_file=sys.argv[1]
    #date,delay,distance,origin,destination
    schema_ddl="date INT,delay INT,distance INT,origin STRING,destination STRING"
    
    
    df=(spark.read.format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load(data_source_file))
    df.printSchema()
    #df.createOrReplaceTempView("us_delay_flights_tbl")
    