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

object PostgresqlStorage {

    // run every hour
    private val appName = "PostgresqlStorage"
    
    def main(args: Array[String]){
        
        val metric = args(0)
        
        val spark = {
            SparkSession.builder.master("yarn").appName(appName)
            .enableHiveSupport().getOrCreate()
        }

        val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
        
        val kafkaStream = KafkaUtils.createStream(
                ssc, "s2:2181,s1:2181",
                "group", Map(metric -> 2)
        )
        
        kafkaStream.foreachRDD(rdd => {
            
            if(rdd.count > 0){
                
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
                /*val seq = Seq(("page-views", new Timestamp(1478776892638L), "{\"metric\":\"page-views\",\"username\":\"vinhdp\",\"timestamp\":\"1478776892638\"}"),
                        ("page-views", new Timestamp(1478776892638L), "{\"metric\":\"page-views\",\"username\":\"vinhdp\",\"timestamp\":\"1478776892638\"}"))
                var df = seq.toDF("metric", "log_date", "data")*/
                
                val logDF = spark.createDataFrame(rdd.map{
                    
                    line => 
                        Row(metric, new Timestamp(line._1.toLong), line._2)
                }, schema)
                
                val prop = new Properties() 
                prop.put("user", "stackops")
                prop.put("password", "stpg@team2")
                prop.put("driver", "org.postgresql.Driver")
                
                logDF.write.mode(SaveMode.Append).jdbc("jdbc:postgresql://s2:5432/svcdb?stringtype=unspecified", "logs", prop)
                /* end write */
            }
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