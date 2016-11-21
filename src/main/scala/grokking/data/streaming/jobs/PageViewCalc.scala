package grokking.data.streaming.jobs

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import net.liftweb.json.DefaultFormats
import grokking.data.utils.DateTimeUtils
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.dstream._
import kafka.serializer.StringDecoder
import grokking.data.streaming.redis.RedisClient
import org.apache.spark.SparkConf

object PageViewCalc {
    
    private val appName = "PageViewCalc"
    
    def createKafkaStream(ssc: StreamingContext, kafkaTopic: String): DStream[(String, String)] = {
        
        KafkaUtils.createStream(
                ssc, "s2:2181,s1:2181",
                "group", Map(kafkaTopic -> 1)
        )
    }
    
    def main(args: Array[String]){
        
        val metric = args(0)
        val time = args(1)
        
        // create spark session
        val spark = {
            SparkSession.builder.master("yarn").appName(appName)
            .getOrCreate()
        }
        
        // set runtime configuration
        spark.conf.set("spark.streaming.concurrentJobs", "5")
        
        // create a StreamingContext
        val ssc = new StreamingContext(spark.sparkContext, Seconds(time.toInt))
        
        // create stream
        val pageviewStream = createKafkaStream(ssc, "demo-pageview")
        val clickStream = createKafkaStream(ssc, "demo-click")
        val orderStream = createKafkaStream(ssc, "demo-order")
        
        // calculate page views and video views
        pageviewStream.foreachRDD(rdd => {
          
            rdd.foreach(event => {
                
                // get event data
                val timestamp = event._1
                val json = event._2
                implicit val formats = net.liftweb.json.DefaultFormats
                
                // parse json string to object
                val jsonObject = net.liftweb.json.parse(json).extract[PageViewEvent]
                val videoId = jsonObject.video
                
                // get redis resource
                val jedis = RedisClient.pool.getResource
                
                val minutes = DateTimeUtils.format(timestamp, "yyyyMMddHHmm")
                val pageViewKey = "pageviews:" + minutes
                
                jedis.incrBy(pageViewKey, 1)    // incr
                jedis.expire(pageViewKey, 1800)    // expire after 30 minutes
                
                val videoViewKey = "videoviews:" + videoId + ":" + minutes
                jedis.incrBy(videoViewKey, 1)
                jedis.expire(videoViewKey, 21600)    // expire after 6 hours
                
                // release redis resource
                RedisClient.pool.returnResource(jedis)
            })
        })
        
        clickStream.foreachRDD(rdd => {
          
            rdd.foreach(event => {
                // get event data
                val timestamp = event._1
                val json = event._2
                implicit val formats = net.liftweb.json.DefaultFormats
                
                // parse json string to object
                val jsonObject = net.liftweb.json.parse(json).extract[ClickEvent]
                val videoId = jsonObject.video
                val userId = jsonObject.viewer
                
                // get redis resource
                val jedis = RedisClient.pool.getResource
                
                val minutes = DateTimeUtils.format(timestamp, "yyyyMMddHHmm")
                val clickKey = "clicks:" + minutes
                
                jedis.zincrby(clickKey, 1, userId)
                jedis.expire(clickKey, 600)    // expire after 10 minutes
                
                // release redis resource
                RedisClient.pool.returnResource(jedis)
            })
        })
        
        ssc.start()
        ssc.awaitTermination()
    }
    
    def caclVideoViews(): Unit = {
        
    }
}

case class PageViewEvent(referrer: String, product: String, metric: String, location: String,
        video: String, uuid: String, url: String, timestamp: String)
case class ClickEvent(referrer: String, viewer: String, product: String, metric: String, location: String,
        video: String, uuid: String, url: String, timestamp: String)