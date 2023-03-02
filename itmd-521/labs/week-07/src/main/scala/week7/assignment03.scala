package main.scala.week7
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


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
        val format_flightDF = df.withColumn("dateMonth", from_unixtime(unix_timestamp(col("date"), "MMddHHmm"), "MM")).withColumn("dateDay", from_unixtime(unix_timestamp(col("date"), "MMddHHmm"), "dd"))


        //Part 1
        //Spark Sql examples on Page 87, converting into dataframe API
        //query1
        format_flightDF.select(col("date"),col("delay"),col("origin"),col("destination"))
        .filter(col("delay")>120)
        .filter(col("origin")==="SFO")
        .filter(col("destination")==="ORD")
        .orderBy(col("delay").desc)
        .limit(10)
        .show(10)

        //query2
        df.select(
        col("delay"),
        col("origin"),
        col("destination"),
        when(col("delay")>360,"Very Long Delays")
            .when(col("delay").between(120,360),"Long Delays")
            .when(col("delay").between(60,120),"Short Delays")
            .when(col("delay").between(1,60),"Tolerable Delays")
            .when(col("delay")===0,"No Delays")
            .otherwise("Early")
            .alias("Flight Delays")
        ).orderBy(col("origin"),col("delay").desc)
        .show(10)


        //--------------------------------------------------------------------------

        //part2
        val df2 = spark.read.format("csv")
        .option("header", "true")
        .schema(schema)
        .load(departuredelay_file)

        df2.write.option("path","./spark-warehouse").mode(SaveMode.Overwrite).saveAsTable("us_delay_flights_tbl1")

        val temp_view_query=spark.sql("SELECT date,dateMonth,dateDay, delay, origin, destination FROM us_delay_flights_tbl where ORIGIN  like 'ORD' AND dateMonth = 03 AND dateDay >=1 AND dateDay <=15")
        temp_view_query.show(false)





        //----------------------------------------------------------------------------

        //part3


    }
}