package main.scala.week7
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object assignment02 {
    def main(args: Array[String]) {
        val spark = SparkSession
          .builder
          .appName("assignment02")
          .getOrCreate()
          
        if (args.length <= 0){
            System.exit(1)
        }
        import spark.implicits._
        val departuredelay_file=args(0)

        val schema = "date STRING, delay INT, distance INT, origin STRING, destination STRING"
        val df = (spark.read.schema(schema).format("csv")).option("header", "true").load(departuredelay_file)
        df.show(false)
    }
}