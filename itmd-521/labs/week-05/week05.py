
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import *

if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(-1)

    spark = (SparkSession.builder.appName("week05").getOrCreate())
    data_source_file = sys.argv[1]
    
    #inferring the Schema
    infer_DF= (spark.read.format("csv").option("header","true").option("inferSchema","true").load(data_source_file))
    infer_DF.show(n=15)
    infer_DF.printSchema()
    infer_DF.count()
    
    # Schema programmatically use StructFields
    structure_schema = StructType([StructField("trip_id", IntegerType()),
        StructField("starttime", StringType()),
        StructField("stoptime", StringType()),
        StructField("bikeid", IntegerType()),
        StructField("tripduration", IntegerType()),
        StructField("from_station_id", IntegerType()),
        StructField("from_station_name", StringType()),
        StructField("to_station_id", IntegerType()),
        StructField("to_station_name", StringType()),
        StructField("usertype", StringType()),
        StructField("gender", StringType()),
        StructField("birthyear", IntegerType())])
    
    struc_Divvy_DF=(spark.read.schema(structure_schema).format("csv")).option("header","true").option("structureSchema","true").load(data_source_file)
    struc_Divvy_DF.show()
    struc_Divvy_DF.printSchema()
    struc_Divvy_DF.count()
    
    #attaching a schema via DDL
    schema_DDL= "trip_id INT,starttime STRING,stoptime STRING,bikeid INT,tripduration INT,from_station_id INT,from_station_name STRING,to_station_id INT,to_station_name STRING,usertype STRING,gender STRING,birthyear INT"
    DDL_Df= (spark.read.schema(schema_DDL).format("csv")).option("header", "true").load(data_source_file)
    DDL_Df.show()
    DDL_Df.printSchema()
    DDL_Df.count()

    
    
    
    