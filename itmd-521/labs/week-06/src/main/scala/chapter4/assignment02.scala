//1. Detect failing devices with battery levels below a threshold.

      
//4. Sort and group by average temperature, CO2, humidity, and country

package main.scala.chapter4
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


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

        val max_co2=ds.agg(max("c02_level"))
        //2. Identify offending countries with high levels of CO2 emissions.
        ds.select($"cn",$"c02_level").distinct().where(ds("c02_level")=="max_co2").show(70,false)

        //3. Compute the min and max values for temperature, battery level, CO2, and humidity.
        //ANSWER: Min Temperature: 10, Max Temperature: 34
        //        Min battery level: 0, Max battery level: 9
        //        Min Co2: 800, Max co2: 1599
        //        Min humidity: 25, Max humidity: 99   

        ds.agg(min("temp"),max("temp")).show(false)
        ds.agg(min("battery_level"),max("battery_level")).show(false)
        ds.agg(min("c02_level"),max("c02_level")).show(false)
        ds.agg(min("humidity"),max("humidity")).show(false)

        
    }
}
