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
    }
}