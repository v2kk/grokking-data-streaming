package grokking.data.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext

object TestStreaming {

    def main(args: Array[String]){
        
        val spark = SparkSession.builder
        .master("yarn")
        .appName("spark-streaming-demo")
        .enableHiveSupport()
        .getOrCreate()
        import spark.implicits._
        
        val sc = spark.sparkContext
        val ssc = new StreamingContext(sc, Seconds(2))

        val kafkaStream = KafkaUtils.createStream(
                ssc, 
                "s2:2181,s1:2181",
                "spark-streaming-consumer-group",
                Map("page-views" -> 2)
        )
        val sqlContext = spark.sqlContext
        import sqlContext.implicits._
            
        kafkaStream.foreachRDD(rdd => {
            
            if(rdd.count > 0){
                
                val df = sqlContext.read.json(rdd.map(x => x._2))
                df.coalesce(2).write.mode(SaveMode.Append).format("parquet").save("/data/svcdb/page_views/2016-11-11")
            }
        })
        
        ssc.start
        ssc.awaitTermination()
        ssc.stop()
    }
}