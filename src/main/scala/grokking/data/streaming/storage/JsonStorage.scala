package grokking.data.streaming.storage

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.Minutes
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.sql.SaveMode

object JsonStorage {

    private val appName = "JsonStorage"
    
    def main(args: Array[String]){
        
        val metric = args(0)
        
        val spark = {
            SparkSession.builder.master("yarn").appName(appName)
            .enableHiveSupport().getOrCreate()
        }

        val ssc = new StreamingContext(spark.sparkContext, Minutes(60))
        
        val kafkaStream = KafkaUtils.createStream(
                ssc, "s2:2181,s1:2181",
                "group", Map("page-views" -> 2)
        )
        
        kafkaStream.foreachRDD(rdd => {
            
            if(rdd.count > 0){
                
                val df = rdd.map(x => x._2)
                rdd.saveAsTextFile("/data/svcdb/" + metric + "/2016-11-11")
            }
        })
    }
}