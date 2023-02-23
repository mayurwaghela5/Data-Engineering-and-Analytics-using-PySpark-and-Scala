from select import select
import sys
from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.types import StructType, StructField, IntegerType,StringType,BooleanType,FloatType
from pyspark.sql.functions import col,countDistinct,year,weekofyear,to_timestamp,month

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
    
    transformed_struct_fire_DF=struc_fire_DF.withColumn("IncidentCallDate", to_timestamp(col("CallDate"), "MM/dd/yyyy")).drop("CallDate") 
    transformed_struct_fire_DF.cache()
    
    
    #1. What were all the different types of fire calls in 2018?
    
    transformed_struct_fire_DF.filter(year("IncidentCallDate")=='2018').select("CallType").distinct().show()
    
    #+--------------------+
    #|            CallType|
    #+--------------------+
    #|Elevator / Escala...|
    #|              Alarms|
    #|Odor (Strange / U...|
    #|Citizen Assist / ...|
    #|              HazMat|
    #|        Vehicle Fire|
    #|               Other|
    #|        Outside Fire|
    #|   Traffic Collision|
    #|       Assist Police|
    #|Gas Leak (Natural...|
    #|        Water Rescue|
    #|   Electrical Hazard|
    #|      Structure Fire|
    #|    Medical Incident|
    #|          Fuel Spill|
    #|Smoke Investigati...|
    #|Train / Rail Inci...|
    #|           Explosion|
    #|  Suspicious Package|
    #+--------------------+
    
    #2. What months within the year 2018 saw the highest number of fire calls?
    
    transformed_struct_fire_DF.filter(year('IncidentCallDate') == 2018).groupBy(month('IncidentCallDate')).count().orderBy('count', ascending=False).show()
    #Answer: The month of October saw the highest number of fire calls in 2018
    #+-----------------------+-----+
    #|month(IncidentCallDate)|count|
    #+-----------------------+-----+
    #|                     10| 1068|
    #|                      5| 1047|
    #|                      3| 1029|
    #|                      8| 1021|
    #|                      1| 1007|
    #|                      7|  974|
    #|                      6|  974|
    #|                      9|  951|
    #|                      4|  947|
    #|                      2|  919|
    #|                     11|  199|
    #+-----------------------+-----+
    
    #3. Which neighborhood in San Francisco generated the most fire calls in 2018?
    transformed_struct_fire_DF.select("Neighborhood").filter(year("IncidentCallDate") == 2018).groupBy("Neighborhood").count().orderBy('count', ascending=False).show()
    #Answer: The Neighborhood Tenderloin generated the most fire calls in 2018
    #+--------------------+-----+
    #|        Neighborhood|count|
    #+--------------------+-----+
    #|          Tenderloin| 1393|   
    #|     South of Market| 1053|
    #|             Mission|  913|
    #|Financial Distric...|  772|
    #|Bayview Hunters P...|  522|
    #|    Western Addition|  352|
    #|     Sunset/Parkside|  346|
    #|            Nob Hill|  295|
    #|        Hayes Valley|  291|
    #|      Outer Richmond|  262|
    #| Castro/Upper Market|  251|
    #|         North Beach|  231|
    #|           Excelsior|  212|
    #|        Potrero Hill|  210|
    #|  West of Twin Peaks|  210|
    #|     Pacific Heights|  191|
    #|              Marina|  191|
    #|           Chinatown|  191|
    #|         Mission Bay|  178|
    #|      Bernal Heights|  170|
    #+--------------------+-----+
    #only showing top 20 rows
    
    #4. Which neighborhoods had the worst response times to fire calls in 2018?
    
    transformed_struct_fire_DF.select("Neighborhood","Delay").filter(year("IncidentCallDate") == 2018).orderBy(col("Delay").desc()).show(10,False)
    #Answer: The Neighborhood Chinatown has the worst response time to fire calls in 2018
    #+------------------------------+---------+
    #|Neighborhood                  |Delay    |
    #+------------------------------+---------+
    #|Chinatown                     |491.26666|
    #|Financial District/South Beach|406.63333|
    #|Tenderloin                    |340.48334|
    #|Haight Ashbury                |175.86667|
    #|Bayview Hunters Point         |155.8    |
    #|Financial District/South Beach|135.51666|
    #|Pacific Heights               |129.01666|
    #|Potrero Hill                  |109.8    |
    #|Inner Sunset                  |106.13333|
    #|South of Market               |94.71667 |
    #+------------------------------+---------+
    #only showing top 10 rows
    
    
    #5. Which week in the year in 2018 had the most fire calls?
    transformed_struct_fire_DF.filter(year('IncidentCallDate') == 2018).groupBy(weekofyear('IncidentCallDate')).count().orderBy('count', ascending=False).show()
    #Answer: The week 22 has the most fire calls in 2018
    #+----------------------------+-----+
    #|weekofyear(IncidentCallDate)|count|
    #+----------------------------+-----+
    #|                          22|  259|
    #|                          40|  255|
    #|                          43|  250|
    #|                          25|  249|
    #|                           1|  246|
    #|                          44|  244|
    #|                          13|  243|
    #|                          32|  243|
    #|                          11|  240|
    #|                           5|  236|
    #|                          18|  236|
    #|                          23|  235|
    #|                          42|  234|
    #|                           2|  234|
    #|                          31|  234|
    #|                          19|  233|
    #|                          34|  232|
    #|                           8|  232|
    #|                          10|  232|
    #|                          28|  231|
    #+----------------------------+-----+
    #only showing top 20 rows
    
    #6. Is there a correlation between neighborhood, zip code, and number of fire calls?
    #Answer: Yes there is correlation between neighborhood and zip code. But not between neighborhood and number of fire calls or zip code and number of fire calls.
    
    #7. How can we use Parquet files or SQL tables to store this data and read it back?
    #Answer:
    #transformed_struct_fire_DF.write.format("parquet").mode("overwrite").save("/tmp/SFParquet1/")   to save as file
    #transformed_struct_fire_DF.write.format("parquet").mode("overwrite").saveAsTable("SFParquet1")    to save it as table
    #to load it back and read it
    #transformed_struct_fire_DF = spark.read.format("parquet").load("/tmp/SFParquet1/")  
    #transformed_struct_fire_DF.show()
      
    
    
    
    