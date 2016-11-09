package grokking.data.streaming.storage

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.sql.SaveMode
import org.apache.spark.streaming.Minutes

object ParquetStorage {

    // run every hour
    private val appName = "ParquetStorage"
    
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
                
                val df = spark.sqlContext.read.json(rdd.map(x => x._2))
                df.coalesce(2).write.mode(SaveMode.Append).format("parquet")
                .save("/data/svcdb/" + metric + "/2016-11-11")
            }
        })
    }
}