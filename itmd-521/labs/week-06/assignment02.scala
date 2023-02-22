import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object assignment02 {
    def main(args: Array[String]) {
        val spark = SparkSession
          .builder
          .appName("assignment02")
          .getOrCreate()
          
        if (args.length <= 0){
            System.exit(1)
        }

        val fire_struct_schema=StructType(Array(StructField("CallNumber",IntegerType,false),
        StructField("UnitID", StringType, false),
        StructField("IncidentNumber", IntegerType, false),
        StructField("CallType", StringType, false),
        StructField("CallDate", StringType, false),
        StructField("WatchDate", StringType, false),
        StructField("CallFinalDisposition", StringType, false),
        StructField("AvailableDtTm", StringType, false),
        StructField("Address", StringType, false),
        StructField("City", StringType, false),
        StructField("Zipcode", IntegerType, false),
        StructField("Battalion", StringType, false),
        StructField("StationArea", StringType, false),
        StructField("Box", StringType, false),
        StructField("OriginalPriority", StringType, false),
        StructField("Priority", StringType, false),
        StructField("FinalPriority", IntegerType, false),
        StructField("ALSUnit", BooleanType, false),
        StructField("CallTypeGroup", StringType, false),
        StructField("NumAlarms", IntegerType, false),
        StructField("UnitType", StringType, false),
        StructField("UnitSequenceInCallDispatch", IntegerType, false),
        StructField("FirePreventionDistrict", StringType, false),
        StructField("SupervisorDistrict", StringType, false),
        StructField("Neighborhood", StringType, false),
        StructField("Location", StringType, false),
        StructField("RowID", StringType, false),
        StructField("Delay", FloatType, false)
        ))
    }
}