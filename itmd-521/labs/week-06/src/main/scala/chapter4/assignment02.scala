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

        //1. Detect failing devices with battery levels below a threshold.



        //2. Identify offending countries with high levels of CO2 emissions.
        //ds.select($"cn",$"c02_level").distinct().where(ds("c02_level")=="max_co2").show(70,false)

        //3. Compute the min and max values for temperature, battery level, CO2, and humidity.
        //ANSWER: Min Temperature: 10, Max Temperature: 34
        //        Min battery level: 0, Max battery level: 9
        //        Min Co2: 800, Max co2: 1599
        //        Min humidity: 25, Max humidity: 99   

        ds.agg(min("temp"),max("temp")).show(false)
        ds.agg(min("battery_level"),max("battery_level")).show(false)
        ds.agg(min("c02_level"),max("c02_level")).show(false)
        ds.agg(min("humidity"),max("humidity")).show(false)


        //4. Sort and group by average temperature, CO2, humidity, and country

        val dsAvg=ds.select("cca3","temp","c02_level","humidity").groupBy("cca3").avg()
                    .sort($"avg(temp)".desc,$"avg(c02_level)".desc ,$"avg(humidity)".desc).as("averages")
        dsAvg.show(50,false)

        //+----+------------------+------------------+------------------+
        //|cca3|avg(temp)         |avg(c02_level)    |avg(humidity)     |
        //+----+------------------+------------------+------------------+
        //|AIA |31.142857142857142|1165.142857142857 |50.714285714285715|
        //|GRL |29.5              |1099.5            |56.5              |
        //|GAB |28.0              |1523.0            |30.0              |
        //|VUT |27.3              |1175.3            |64.0              |
        //|LCA |27.0              |1201.6666666666667|61.833333333333336|
        //|MWI |26.666666666666668|1137.0            |59.55555555555556 |
        //|TKM |26.666666666666668|1093.0            |69.0              |
        //|IRQ |26.428571428571427|1225.5714285714287|62.42857142857143 |
        //|LAO |26.285714285714285|1291.0            |60.857142857142854|
        //|IOT |26.0              |1206.0            |65.0              |
        //|CUB |25.866666666666667|1222.5333333333333|49.53333333333333 |
        //|HTI |25.333333333333332|1291.3333333333333|64.58333333333333 |
        //|FJI |25.09090909090909 |1193.7272727272727|56.45454545454545 |
        //|DMA |24.73076923076923 |1214.3461538461538|70.46153846153847 |
        //|BEN |24.666666666666668|1038.0            |65.66666666666667 |
        //|SYR |24.6              |1345.8            |57.8              |
        //|BWA |24.5              |1302.6666666666667|73.75             |
        //|TLS |24.333333333333332|1310.0            |59.0              |
        //|MNP |24.333333333333332|1164.111111111111 |52.333333333333336|
        //|BHS |24.27777777777778 |1177.388888888889 |68.61111111111111 |
        //|AGO |24.107142857142858|1115.142857142857 |66.03571428571429 |
        //|AFG |24.05263157894737 |1228.4736842105262|66.6842105263158  |
        //|FSM |24.0              |1261.0            |68.33333333333333 |
        //|VGB |24.0              |1221.3333333333333|55.5              |
        //|KGZ |23.86046511627907 |1242.953488372093 |57.95348837209303 |
        //|BLZ |23.846153846153847|1223.3846153846155|57.92307692307692 |
        //|ZMB |23.714285714285715|1151.857142857143 |61.142857142857146|
        //|TZA |23.69811320754717 |1182.1132075471698|64.9622641509434  |
        //|GUM |23.25             |1210.0            |62.21875          |
        //|PNG |23.25             |1198.75           |63.6875           |
        //|PER |23.21551724137931 |1259.6810344827586|60.36206896551724 |
        //|BRB |23.210526315789473|1257.5526315789473|58.36842105263158 |
        //|MDA |23.164556962025316|1187.1898734177216|59.063291139240505|
        //|OMN |23.16             |1159.76           |59.32             |
        //|ALA |23.05             |1246.45           |63.35             |
        //|NAM |23.0              |1208.9666666666667|62.4              |
        //|MAF |23.0              |1204.0            |48.0              |
        //|SEN |23.0              |1140.32           |63.32             |
        //|BMU |22.943396226415093|1210.867924528302 |64.64150943396227 |
        //|MNG |22.923076923076923|1247.871794871795 |57.48717948717949 |
        //|CYM |22.923076923076923|1163.8076923076924|61.61538461538461 |
        //|IRN |22.883928571428573|1194.6488095238096|60.11607142857143 |
        //|HRV |22.854922279792746|1229.8186528497408|60.854922279792746|
        //|RWA |22.833333333333332|1136.3333333333333|64.33333333333333 |
        //|MTQ |22.833333333333332|1135.3333333333333|59.833333333333336|
        //|SGP |22.771375464684017|1210.9061338289962|62.48141263940521 |
        //|KHM |22.767441860465116|1205.9302325581396|65.16279069767442 |
        //|IRL |22.739910313901344|1206.1838565022422|60.30717488789238 |
        //|SVK |22.721311475409838|1195.8524590163934|62.42295081967213 |
        //|NPL |22.696969696969695|1211.5454545454545|56.666666666666664|
        //+----+------------------+------------------+------------------+
        //only showing top 50 rows
        //+----+------------------+------------------+------------------+
        
    }
}
