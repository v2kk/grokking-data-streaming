package grokking.data.streaming.storage

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.streaming.Minutes
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.sql.SaveMode
import java.util.Properties
import org.apache.spark.sql.Row
import org.apache.spark.streaming.Seconds
import grokking.data.utils.DateTimeUtils
import java.sql.Timestamp
import scala.util.Random

object DummyPostgresqlStorage {

    // run every hour
    private val appName = "PostgresqlStorage"
    
    def main(args: Array[String]){
        
        val seconds = args(0)
        
        val spark = {
            SparkSession.builder.master("yarn").appName(appName)
            .getOrCreate()
        }

        val ssc = new StreamingContext(spark.sparkContext, Seconds(seconds.toInt))
        
        val kafkaStream = KafkaUtils.createStream(
                ssc, "s2:2181,s1:2181",
                "group", Map("grokking" -> 2)
        )
        
        kafkaStream.foreachRDD(rdd => {
            /* write to postgres */
            val sqlContext = spark.sqlContext
            import sqlContext.implicits._
            val schema = 
                StructType(
                    Array(
                        StructField("metric", StringType, true),
                        StructField("log_date", TimestampType, true),
                        StructField("data", StringType, true)
                    )
                )
            val metrics = Seq("video_play", "login", "buy")
            val devices = Seq("Samsung", "iPhone", "nokia", "motorola", "Sharp", "apple", "sony", "lumia")
            val ips = Seq("207.133.165.175", "66.37.216.225", "62.22.133.75", "106.230.28.62", "17.4.119.71")
            val hosts = Seq("vnexpress.vn", "tuoitre.vn", "java.com", "apache.org", "vinaclips.com", "zing.vn")
            val videos = Seq("dkrfQeQvw", "VRlIDAd3c", "XG4hxQNXo", "YFesMqxQU", "-29wihKRco")
            val products = Seq("10995HLAA16YIQVNAMZ", "13806OTAA0XG6WVNAMZ", "13806OTAA0XG6ZVNAMZ", "19704MEAA0XZ4QVNAMZ", "19704MEAA0Y0M6VNAMZ", "1E149OTAA1O21CVNAMZ")
            val random = Random
            
            val tmpDF = spark.sparkContext.parallelize(0 to 0, 1).map{
                
                val metric = metrics(random.nextInt(3))
                var mp = scala.collection.mutable.Map[String, Any]()
                mp += ("device" -> devices(random.nextInt(8)))
                mp += ("user_id" -> random.nextInt(100))
                mp += ("ip" -> ips(random.nextInt(5)))
                mp += ("host" -> hosts(random.nextInt(6)))
                mp += ("quantity" -> random.nextInt(5))
                mp += ("revenue" -> random.nextInt(100) * 1000)
                mp += ("product_id" -> products(random.nextInt(6)))
                mp += ("video_id" -> videos(random.nextInt(5)))
                    
                val jsonObj = scala.util.parsing.json.JSONObject(mp.toMap)
                val json = jsonObj.toString()
                line => Row(metric, new Timestamp(System.currentTimeMillis), json)
            }
            
            val logDF = spark.createDataFrame(tmpDF, schema)
            val prop = new Properties() 
            prop.put("user", "stackops")
            prop.put("password", "stpg@team2")
            prop.put("driver", "org.postgresql.Driver")
            logDF.write.mode(SaveMode.Append).jdbc("jdbc:postgresql://s2:5432/svcdb?stringtype=unspecified", "log_event", prop)
            /* end write */
        })
        // Start Spark Streaming process
        ssc.start()
        ssc.awaitTermination()
    }
    
    def test(spark: SparkSession): Unit = {
        /* read from postgres example */
        val opts = Map(
            "url" -> "jdbc:postgresql://s2:5432/svcdb?user=stackops&password=stpg@team2",
            "dbtable" -> "log_event",
            "driver" -> "org.postgresql.Driver"
        )
        val df = spark.read.format("jdbc").options(opts).load
        /* end read */
        
        /* write to postgres */
        val sqlContext = spark.sqlContext
        import sqlContext.implicits._
        
        val storeRDD = spark.sparkContext.parallelize(0 to 100, 100)
        val schema = StructType(Array(StructField("log",StringType,true)))
        val storeDF = spark.createDataFrame(storeRDD.map(line => Row(line.toString)), schema)
        
        val prop = new Properties() 
        prop.put("user", "stackops")
        prop.put("password", "stpg@team2")
        prop.put("driver", "org.postgresql.Driver")
        
        storeDF.write.mode(SaveMode.Append).jdbc("jdbc:postgresql://s2:5432/svcdb", "logs", prop)
        /* end write */
    }
}