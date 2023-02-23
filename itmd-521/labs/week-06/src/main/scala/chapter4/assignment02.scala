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
        val dsFailDevice = ds.select("*").where("battery_level < 7").as[DeviceIoTData]
        dsFailDevice.show(20, false)
        //Answer: Below are the failing devices with battery_level below 7 which is set as the threshold
        //+-------------+---------+----+----+-----------------+---------+------------------------+--------+---------------+--------+------+---------+-------+----+-------------+
        //|battery_level|c02_level|cca2|cca3|cn               |device_id|device_name             |humidity|ip             |latitude|lcd   |longitude|scale  |temp|timestamp    |
        //+-------------+---------+----+----+-----------------+---------+------------------------+--------+---------------+--------+------+---------+-------+----+-------------+
        //|2            |1556     |IT  |ITA |Italy            |3        |device-mac-36TWSKiT     |44      |88.36.5.1      |42.83   |red   |12.83    |Celsius|19  |1458444054120|
        //|6            |1080     |US  |USA |United States    |4        |sensor-pad-4mzWkz       |32      |66.39.173.154  |44.06   |yellow|-121.32  |Celsius|28  |1458444054121|
        //|4            |931      |PH  |PHL |Philippines      |5        |therm-stick-5gimpUrBB   |62      |203.82.41.9    |14.58   |green |120.97   |Celsius|25  |1458444054122|
        //|3            |1210     |US  |USA |United States    |6        |sensor-pad-6al7RTAobR   |51      |204.116.105.67 |35.93   |yellow|-85.46   |Celsius|27  |1458444054122|
        //|3            |1129     |CN  |CHN |China            |7        |meter-gauge-7GeDoanM    |26      |220.173.179.1  |22.82   |yellow|108.32   |Celsius|18  |1458444054123|
        //|0            |1536     |JP  |JPN |Japan            |8        |sensor-pad-8xUD6pzsQI   |35      |210.173.177.1  |35.69   |red   |139.69   |Celsius|27  |1458444054123|
        //|3            |807      |JP  |JPN |Japan            |9        |device-mac-9GcjZ2pw     |85      |118.23.68.227  |35.69   |green |139.69   |Celsius|13  |1458444054124|
        //|3            |1544     |IT  |ITA |Italy            |11       |meter-gauge-11dlMTZty   |85      |88.213.191.34  |42.83   |red   |12.83    |Celsius|16  |1458444054125|
        //|0            |1260     |US  |USA |United States    |12       |sensor-pad-12Y2kIm0o    |92      |68.28.91.22    |38.0    |yellow|-97.0    |Celsius|12  |1458444054126|
        //|6            |1007     |IN  |IND |India            |13       |meter-gauge-13GrojanSGBz|92      |59.144.114.250 |28.6    |yellow|77.2     |Celsius|13  |1458444054127|
        //|1            |1346     |NO  |NOR |Norway           |14       |sensor-pad-14QL93sBR0j  |90      |193.156.90.200 |59.95   |yellow|10.75    |Celsius|16  |1458444054127|
        //|4            |1425     |US  |USA |United States    |16       |sensor-pad-16aXmIJZtdO  |53      |68.85.85.106   |38.0    |red   |-97.0    |Celsius|15  |1458444054128|
        //|0            |1466     |US  |USA |United States    |17       |meter-gauge-17zb8Fghhl  |98      |161.188.212.254|39.95   |red   |-75.16   |Celsius|31  |1458444054129|
        //|4            |1096     |CN  |CHN |China            |18       |sensor-pad-18XULN9Xv    |25      |221.3.128.242  |25.04   |yellow|102.72   |Celsius|31  |1458444054130|
        //|5            |939      |AT  |AUT |Austria          |21       |device-mac-21sjz5h      |44      |193.200.142.254|48.2    |green |16.37    |Celsius|30  |1458444054131|
        //|5            |1245     |IN  |IND |India            |23       |meter-gauge-230IupA     |47      |59.90.65.1     |12.98   |yellow|77.58    |Celsius|23  |1458444054133|
        //|4            |880      |US  |USA |United States    |25       |therm-stick-25kK6VyzIFB |78      |24.154.45.90   |41.1    |green |-80.76   |Celsius|27  |1458444054134|
        //|5            |1597     |KR  |KOR |Republic of Korea|27       |device-mac-27P5wf2      |73      |218.239.168.1  |37.57   |red   |126.98   |Celsius|10  |1458444054135|
        //|3            |1502     |KR  |KOR |Republic of Korea|28       |sensor-pad-28Tsudcoikw  |64      |211.238.224.77 |37.29   |red   |127.01   |Celsius|25  |1458444054136|
        //|6            |1095     |NL  |NLD |Netherlands      |29       |meter-gauge-29lyNVxIS   |69      |83.98.224.49   |52.37   |yellow|4.9      |Celsius|15  |1458444054137|
        //+-------------+---------+----+----+-----------------+---------+------------------------+--------+---------------+--------+------+---------+-------+----+-------------+
        //only showing top 20 rows
        

        //2. Identify offending countries with high levels of CO2 emissions.
        val coun_high_co2=ds.select("cn","c02_level").distinct().orderBy(desc("c02_level"))
        coun_high_co2.show(false)
        //Answer: Below are the offending countries with high level of CO2 emmission.
        //+---------------------+---------+
        //|cn                   |c02_level|
        //+---------------------+---------+
        //|Australia            |1599     |
        //|Thailand             |1599     |
        //|Bulgaria             |1599     |
        //|                     |1599     |
        //|Russia               |1599     |
        //|Poland               |1599     |
        //|Japan                |1599     |
        //|Germany              |1599     |
        //|Sweden               |1599     |
        //|Malaysia             |1599     |
        //|Turkey               |1599     |
        //|United Arab Emirates |1599     |
        //|Bermuda              |1599     |
        //|Latvia               |1599     |
        //|France               |1599     |
        //|Mexico               |1599     |
        //|Romania              |1599     |
        //|Saint Kitts and Nevis|1599     |
        //|Denmark              |1599     |
        //|Finland              |1599     |
        //+---------------------+---------+
        //only showing top 20 rows
        

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
