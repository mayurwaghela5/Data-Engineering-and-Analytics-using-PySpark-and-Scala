//1. Detect failing devices with battery levels below a threshold.
//2. Identify offending countries with high levels of CO2 emissions.
//3. Compute the min and max values for temperature, battery level, CO2, and humidity.
//4. Sort and group by average temperature, CO2, humidity, and country


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


        //{"device_id": 1, "device_name": "meter-gauge-1xbYRYcj", "ip": "68.161.225.1", "cca2": "US", "cca3": "USA", "cn": "United States", "latitude": 38.000000, "longitude": -97.000000, "scale": "Celsius", "temp": 34, "humidity": 51, "battery_level": 8, "c02_level": 868, "lcd": "green", "timestamp" :1458444054093 }
        
        //creating df schema using DDL
        val fire_struct_schema="device_id Long, device_name String, ip, cca2, cca3, cn, latitude, longitude, scale, temp, humidity, battery_level, c02_level, lcd, timestamp"
    }
}