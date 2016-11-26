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

object PostgresqlStorage {

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
        
        val pageviewStream = createKafkaStream(ssc, "production-pageview")
        val clickStream = createKafkaStream(ssc, "production-click")
        val orderStream = createKafkaStream(ssc, "production-order")
        
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
    
    def test(spark: SparkSession): Unit = {
        /* read from postgres example */
        val opts = Map(
            "url" -> "jdbc:postgresql://s2:5432/svcdb?user=stackops&password=stpg@team2",
            "dbtable" -> "logs",
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