//1. Detect failing devices with battery levels below a threshold.
//2. Identify offending countries with high levels of CO2 emissions.
//3. Compute the min and max values for temperature, battery level, CO2, and humidity.
//4. Sort and group by average temperature, CO2, humidity, and country

package main.scala.chapter4
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

case class DeviceIoTData (battery_level: Long, c02_level: Long, cca2: String, cca3: String, cn: String, device_id: Long,
device_name: String, humidity: Long, ip: String, latitude: Double, lcd: String, longitude: Double, scale:String, temp: Long,
timestamp: Long)

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
        val iot_device_json=args(0)
        
        val ds = spark.read.json(iot_device_json).as[DeviceIoTData]
        ds.show(20, false)

        //3. Compute the min and max values for temperature, battery level, CO2, and humidity.

        ds.agg(min("temp"),max("temp")).show()
        ds.agg(min("battery_level"),max("battery_level")).show()
        ds.agg(min("c02_level"),max("c02_level")).show()
        ds.agg(min("humidity"),max("humidity")).show()

        
    }
}
