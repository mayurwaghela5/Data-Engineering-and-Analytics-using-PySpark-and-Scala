package main.scala.week7
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
from pyspark.sql.functions import count
from pyspark.sql.functions import col
from pyspark.sql.functions import when, col
from pyspark.sql.sql.functions import from_unixtime, unix_timestamp

object assignment03 {
    def main(args: Array[String]) {
        val spark = SparkSession
          .builder
          .appName("assignment03")
          .getOrCreate()
          
        if (args.length <= 0){
            System.exit(1)
        }
        import spark.implicits._
        val departuredelay_file=args(0)

        val schema = "date STRING, delay INT, distance INT, origin STRING, destination STRING"
        val df = (spark.read.schema(schema).format("csv")).option("header", "true").load(departuredelay_file)
        //df.show(false)
        //converting date into day and month
        val format_flightDF = df.withColumn("dateMonth", from_unixtime(unix_timestamp(df.date, "MMddHHmm"), "MM")).withColumn("dateDay", from_unixtime(unix_timestamp(df.date, "MMddHHmm"), "dd"))


        //Part 1
        //Spark Sql examples on Page 87, converting into dataframe API


    }
}