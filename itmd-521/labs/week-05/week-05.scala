package main.scala.chapter3
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object week-05 {
    def main(args: Array[String]) {

        val spark = SparkSession
          .builder
          .appName("Example3_7")
          .getOrCreate()
          
        if (args.length <= 0){
            println("Usage: DivvySet <Divvy_file_dataset.csv>")
            System.exit(1)
        }

        val data_source_file=args(0)
        val infer_divvy_df = spark.read.format("csv") .option("header", "true") .option("inferSchema", "true") .load(data_source)
        infer_divvy_df.show(20)
        infer_divvy_df.printSchema()

        val struct_schema = StringType(Array(StructField("trip_id",IntegerType),
        StructField("starttime",StringType),
        StructField("stoptime",StringType),
        StructField("bikeid",IntegerType)
        ))

        val structure_divvy_DF=spark.read.schema(struct_schema).format("csv").option("header","true").option("structureSchema","true").load(data_source_file)
        structure_divvy_DF.show(10)
        structure_divvy_DF.printSchema()
    } 
}