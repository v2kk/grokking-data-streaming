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
import org.apache.spark.streaming.dstream.DStream

object BatchPostgresqlStorage {

    // run every hour
    private val appName = "PostgresqlStorage"
    
    def createKafkaStream(ssc: StreamingContext, kafkaTopic: String): DStream[(String, String)] = {
        
        KafkaUtils.createStream(
                ssc, "s2:2181,s1:2181",
                "group", Map(kafkaTopic -> 2)
        )
    }
    
    def main(args: Array[String]){
        
        val seconds = args(0)
        
        val spark = {
            SparkSession.builder.master("yarn").appName(appName)
            .enableHiveSupport().getOrCreate()
        }

        val ssc = new StreamingContext(spark.sparkContext, Seconds(seconds.toInt))
        
        // set runtime configuration
        spark.conf.set("spark.streaming.concurrentJobs", "5")
        
        val pageviewStream = createKafkaStream(ssc, "demo-pageview")
        val clickStream = createKafkaStream(ssc, "demo-click")
        val orderStream = createKafkaStream(ssc, "demo-order")
        
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
            
        // calculate page views and video views
        pageviewStream.foreachRDD(rdd => {
          
            val logDF = spark.createDataFrame(rdd.map{
                
                event => 
                    Row("pageview", new Timestamp(event._1.toLong), event._2)
            }, schema)
            
            val prop = new Properties() 
            prop.put("user", "stackops")
            prop.put("password", "stpg@team2")
            prop.put("driver", "org.postgresql.Driver")
            
            logDF.write.mode(SaveMode.Append).jdbc("jdbc:postgresql://s2:5432/svcdb?stringtype=unspecified", "log_event", prop)
        })
        
        clickStream.foreachRDD(rdd => {
          
            val logDF = spark.createDataFrame(rdd.map{
                
                line => 
                    Row("click", new Timestamp(line._1.toLong), line._2)
            }, schema)
            
            val prop = new Properties() 
            prop.put("user", "stackops")
            prop.put("password", "stpg@team2")
            prop.put("driver", "org.postgresql.Driver")
            
            logDF.write.mode(SaveMode.Append).jdbc("jdbc:postgresql://s2:5432/svcdb?stringtype=unspecified", "log_event", prop)
        
        })
        
        orderStream.foreachRDD(rdd => {
          
            val logDF = spark.createDataFrame(rdd.map{
                
                event => 
                    Row("order", new Timestamp(event._1.toLong), event._2)
            }, schema)
            
            val prop = new Properties() 
            prop.put("user", "stackops")
            prop.put("password", "stpg@team2")
            prop.put("driver", "org.postgresql.Driver")
            
            logDF.write.mode(SaveMode.Append).jdbc("jdbc:postgresql://s2:5432/svcdb?stringtype=unspecified", "log_event", prop)
        })
            
        // Start Spark Streaming process
        ssc.start()
        ssc.awaitTermination()
    }
}