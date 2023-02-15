package main.scala.chapter3
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object week05 {
    def main(args: Array[String]) {

        val spark = SparkSession
          .builder
          .appName("week05")
          .getOrCreate()
          
        if (args.length <= 0){
            println("Usage: DivvySet <Divvy_file_dataset.csv>")
            System.exit(1)
        }
    
        // Infering the Schema
        val data_source_file=args(0)
        val infer_DF = spark.read.format("csv") .option("header", "true") .option("inferSchema", "true") .load(data_source_file)
        infer_DF.show(false)
        println(infer_DF.printSchema)
        println("Infering the Schema In Scala. The number of records in this DataFrame is: "+ infer_DF.count())
        println("---------------------------------------------------------------------------------------------------------------")
        
        // Schema programmatically use StructFields
        
        val struct_schema = StructType(Array(StructField("trip_id",IntegerType,false),
        StructField("starttime",StringType,false),
        StructField("stoptime",StringType,false),
        StructField("bikeid",IntegerType,false), 
        StructField("tripduration",IntegerType,false),
        StructField("from_station_id",StringType,false),
        StructField("from_station_name",StringType,false),
        StructField("to_station_id",StringType,false),
        StructField("to_station_name",StringType,false),
        StructField("usertype",StringType,false),
        StructField("gender",StringType,false),
        StructField("birthyear",IntegerType,false)
        ))
        val structure_divvy_DF=spark.read.schema(struct_schema).format("csv").option("header","true").option("structureSchema","true").load(data_source_file)
        structure_divvy_DF.show(false)
        println(structure_divvy_DF.printSchema)
        println("Schema programmatically use StructFields. The number of records in this DataFrame is: "+ structure_divvy_DF.count())
        println("---------------------------------------------------------------------------------------------------------------")

        // Attaching a schema via DLL and reading the csv

        val schema_DDL= "trip_id INT, starttime STRING,stoptime STRING,bikeid INT,tripduration INT,from_station_id INT ,from_station_name STRING,to_station_id INT ,to_station_name STRING,usertype STRING,gender STRING,birthyear INT"
        val DDL_DF = (spark.read.schema(schema_DDL).format("csv")).option("header", "true").load(data_source_file)
        DDL_DF.show(false)
        print(DDL_DF.printSchema)
        println(DDL_DF.count())
        println("---------------------------------------------------------------------------------------------------------------")

    } 
}