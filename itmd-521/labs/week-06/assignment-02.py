#What were all the different types of fire calls in 2018?

#What months within the year 2018 saw the highest number of fire calls?

#Which neighborhood in San Francisco generated the most fire calls in 2018?

#Which neighborhoods had the worst response times to fire calls in 2018?

#Which week in the year in 2018 had the most fire calls?

#Is there a correlation between neighborhood, zip code, and number of fire calls?

#How can we use Parquet files or SQL tables to store this data and read it back?

#CallNumber,UnitID,IncidentNumber,CallType,CallDate,WatchDate,CallFinalDisposition,AvailableDtTm,Address,
# City,Zipcode,Battalion,StationArea,Box,OriginalPriority,Priority,FinalPriority,ALSUnit,CallTypeGroup,NumAlarms,
# UnitType,UnitSequenceInCallDispatch,FirePreventionDistrict,SupervisorDistrict,Neighborhood,Location,RowID,Delay

from select import select
import sys
from pyspark.sql import SparkSession
#from pyspark.sql.types import *
from pyspark.sql.types import StructType, StructField, IntegerType,StringType,BooleanType,FloatType

if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(-1)
        
    spark=(SparkSession.builder.appName("assignment-02").getOrCreate())
    data_source_file=sys.argv[1]
    
    #create a scheme programmatically
    fire_struct_schema = StructType([StructField('CallNumber', IntegerType(), True),
        StructField('UnitID', StringType(), True),
        StructField('IncidentNumber', IntegerType(), True),
        StructField('CallType', StringType(), True),
        StructField('CallDate', StringType(), True),
        StructField('WatchDate', StringType(), True),
        StructField('CallFinalDisposition', StringType(), True),
        StructField('AvailableDtTm', StringType(), True),
        StructField('Address', StringType(), True),
        StructField('City', StringType(), True),
        StructField('Zipcode', IntegerType(), True),
        StructField('Battalion', StringType(), True),
        StructField('StationArea', StringType(), True),
        StructField('Box', StringType(), True),
        StructField('OriginalPriority', StringType(), True),
        StructField('Priority', StringType(), True),
        StructField('FinalPriority', IntegerType(), True),
        StructField('ALSUnit', BooleanType(), True),
        StructField('CallTypeGroup', StringType(), True),
        StructField('NumAlarms', IntegerType(), True),
        StructField('UnitType', StringType(), True),
        StructField('UnitSequenceInCallDispatch', IntegerType(), True),
        StructField('FirePreventionDistrict', StringType(), True),
        StructField('SupervisorDistrict', StringType(), True),
        StructField('Neighborhood', StringType(), True),
        StructField('Location', StringType(), True),
        StructField('RowID', StringType(), True),
        StructField('Delay', FloatType(), True)])
    
    struc_fire_DF=(spark.read.schema(fire_struct_schema).format("csv")).option("header","true").option("fire_struct_schema","true").load(data_source_file)
    struc_fire_DF.show()
    
    struc_fire_DF.select("CallType").where(col("CallType").isNotNull()).agg(countDistinct("CallType").alias("DistinctCallTypes")).show()
    
