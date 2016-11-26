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
import redis.clients.jedis.Jedis

object PageViewCalc {
    
    private val appName = "PageViewCalc"
    
    /**
     * Create DStream for specific kafka's topic
     */
    def createKafkaStream(ssc: StreamingContext, kafkaTopic: String): DStream[(String, String)] = {
        
        KafkaUtils.createStream(
                ssc, "s2:2181,s1:2181",
                "group", Map(kafkaTopic -> 2)
        )
    }

    /** 
     *  Add uid to redis server using sorted set
     *  May be using HyperLogLog for fast and precision is not necessary
     */
    def calcActiveUser(jedis: Jedis, key: String, uid: String, time: String): Unit = {

        jedis.zadd(key, time.toDouble, uid)
    }
    
    /**
     * Video views by minute
     * Add videoId to redis server, using redis string
     */
    def calcVideoViewByMinute(jedis: Jedis, key: String, videoId: String, time: String, expireSeconds: Int): Unit = {
        
        val videoViewKey = key + ":" + time
        
        jedis.incrBy(videoViewKey, 1)    // incr by 1
        jedis.expire(videoViewKey, expireSeconds)
    }
    
    /**
     * Trending video view
     */
    def calcTopVideoView(jedis: Jedis, key: String, videoId: String, time: String, expireSeconds: Int): Unit = {
        
        val timeBasedKey = key + ":" + time
                
        jedis.zincrby(timeBasedKey, 1, videoId)
        jedis.expire(timeBasedKey, expireSeconds)
    }
    
    /**
     * Expire set item by lex
     */
    def expireSetItemByLex(jedis: Jedis, key: String, from: String, to: String): Unit = {
        
        jedis.zremrangeByLex(key, from, to)
    }
    
    /**
     * Expire set item by score
     */
    def expireSetItembyScore(jedis: Jedis, key: String, from: String, to: String): Unit = {
        
        jedis.zremrangeByScore(key, from, to)
    }
    
    def main(args: Array[String]){
        
        val prefix = args(0)
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
        val pageviewStream = createKafkaStream(ssc, prefix + "-pageview")
        val clickStream = createKafkaStream(ssc, prefix + "-click")
        val orderStream = createKafkaStream(ssc, prefix + "-order")
        
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
                
                var uid = jsonObject.viewer.getOrElse("")
                if( uid == ""){
                    
                    uid = jsonObject.uuid
                }
                
                // get redis resource
                val jedis = RedisClient.pool.getResource
                val minute = DateTimeUtils.format(timestamp, "yyyyMMddHHmm")
                val hour = DateTimeUtils.format(timestamp, "yyyyMMddHH")
                
                calcActiveUser(jedis, "activeusers", uid, timestamp)    //manual expire item by timestamp score
                calcVideoViewByMinute(jedis, "videoviews", videoId, minute, 1800)    // auto expire after 30 minutes
                calcTopVideoView(jedis, "topvideos_min", videoId, minute, 21600)    // auto expire after 6 hours
                calcTopVideoView(jedis, "topvideos_hour", videoId, hour, 21600)    // auto expire after 6 hours
                
                // release redis resource
                RedisClient.pool.returnResource(jedis)
            })
            
            // clear redis sorted set from -inf to 5 minutes ago
            val jedis = RedisClient.pool.getResource
            
            /**
             * delete active user after 6 minutes programmatically (score is timestamp)
             */
            val processTime = System.currentTimeMillis();
            var endTime = (processTime - 360000).toString()
            expireSetItembyScore(jedis, "activeusers", "0", endTime)
            
            RedisClient.pool.returnResource(jedis)
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
                val userId = jsonObject.viewer.getOrElse("")
                
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
}

case class PageViewEvent(referrer: String, viewer: Option[String], product: String, metric: String, location: String,
        video: String, uuid: String, url: String, timestamp: String)
case class ClickEvent(referrer: String, viewer: Option[String], product: String, metric: String, location: String,
        video: String, uuid: String, url: String, timestamp: String)