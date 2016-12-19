package grokking.data.streaming.jobs

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.kafka._

import grokking.data.streaming.redis.RedisClient
import grokking.data.utils.DateTimeUtils
import net.liftweb.json.DefaultFormats
import redis.clients.jedis.Jedis

object PageViewCalc {
    
    private val appName = "PageViewCalc"
    
    /**
     * Create DStream for specific kafka's topic
     */
    def createKafkaStream(ssc: StreamingContext, kafkaTopic: String, numberPartitions: Int): DStream[(String, String)] = {
        
        /*val kafkaDStreams = (1 to numberPartitions).map { e => 
            
            
        }
        KafkaUtils.createStream(
                ssc, "s2:2181,s1:2181",
                "group", Map(kafkaTopic -> 4)
            )
        val unionDStream =  ssc.union(kafkaDStreams)
        val processingParallelism = 4
        val processingDStream = unionDStream.repartition(processingParallelism)
        
        unionDStream*/
        
        KafkaUtils.createStream(
            ssc, "Team2-Server2:2181,Team2-Server1:2181",
            "group", Map(kafkaTopic -> 4)
        )
    }

    /** 
     *  Add uid to redis server using sorted set
     *  May be using HyperLogLog for fast and precision is not necessary
     */
    def putRedisSortedSet(jedis: Jedis, key: String, uid: String, score: String): Unit = {

        jedis.zadd(key, score.toDouble, uid)
    }
    
    /**
     * Video views by minute
     * Add videoId to redis server, using redis string
     */
    def incrRedisKey(jedis: Jedis, key: String, time: String, expireSeconds: Int): Unit = {
        
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
    
    /**
     * Using topvideos minute & hour to calculate topvideos
     */
    def rearrangeTopVideoView(jedis: Jedis, key: String, timestamp: Long): Unit = {
        
        var lstHourKeys = DateTimeUtils.takeListHourBefore(timestamp, 5)
        lstHourKeys = lstHourKeys.map { hour => key + "_h:" + hour }
        var lstMinuteKeys = DateTimeUtils.takeListMinute(timestamp, 5)
        lstMinuteKeys = lstMinuteKeys.map { minute => key + "_m:" + minute }
        var lstKeys = lstHourKeys ::: lstMinuteKeys
        
        jedis.zunionstore(key, lstKeys:_*)
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
        val pageviewStream = createKafkaStream(ssc, prefix + "-pageview", 4)
        val clickStream = createKafkaStream(ssc, prefix + "-click", 4)
        val orderStream = createKafkaStream(ssc, prefix + "-order", 4)
        
        // calculate page views and video views
        pageviewStream.foreachRDD(rdd => {
          
            println("VINHDP: " + rdd.count)
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
                
                putRedisSortedSet(jedis, key = "activeusers", uid, score = timestamp)    //manual expire item by timestamp score
                incrRedisKey(jedis, "videoviews", minute, 1800)    // auto expire after 30 minutes
                calcTopVideoView(jedis, "topvideos_m", videoId, minute, 18000)    // auto expire after 5 hours
                calcTopVideoView(jedis, "topvideos_h", videoId, hour, 14400)    // auto expire after 4 hours
                
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
            
            /**
             * calculate trending video
             */
            rearrangeTopVideoView(jedis, "topvideos", processTime)
            
            RedisClient.pool.returnResource(jedis)
        })
        
        clickStream.foreachRDD(rdd => {
          
            println("VINHDP: " + rdd.count)
            rdd.foreach(event => {
                
                val timestamp = event._1
                val json = event._2
                implicit val formats = net.liftweb.json.DefaultFormats
                
                // parse json string to object
                val jsonObject = net.liftweb.json.parse(json).extract[ClickEvent]
                val videoId = jsonObject.video
                var uid = jsonObject.viewer
                
                val jedis = RedisClient.pool.getResource
                val minute = DateTimeUtils.format(timestamp, "yyyyMMddHHmm")
                val hour = DateTimeUtils.format(timestamp, "yyyyMMddHH")
                
                incrRedisKey(jedis, "pageclicks", minute, 360)    // expire after 6 minutes
                
                RedisClient.pool.returnResource(jedis)
            })
        })
        
        orderStream.foreachRDD(rdd => {
          
            println("VINHDP: " + rdd.count)
            rdd.foreach(event => {
                
                val timestamp = event._1
                val json = event._2
                implicit val formats = net.liftweb.json.DefaultFormats
                
                // parse json string to object
                val jsonObject = net.liftweb.json.parse(json).extract[OrderEvent]
                val videoId = jsonObject.video
                
                var uid = jsonObject.viewer
                
                // get redis resource
                val jedis = RedisClient.pool.getResource
                val minute = DateTimeUtils.format(timestamp, "yyyyMMddHHmm")
                val hour = DateTimeUtils.format(timestamp, "yyyyMMddHH")
                
                putRedisSortedSet(jedis, key = "orderusers", uid, score = timestamp)    //manual expire item by timestamp score
                incrRedisKey(jedis, "orders", minute, 360)    // expire after 6 minutes
                
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
            expireSetItembyScore(jedis, "orderusers", "0", endTime)

            RedisClient.pool.returnResource(jedis)
        })
        println("VINHDP: END")
        ssc.start()
        ssc.awaitTermination()
    }
}

case class PageViewEvent(referrer: String, viewer: Option[String], product: String, metric: String, location: String,
        video: String, uuid: String, url: String, timestamp: String)
case class ClickEvent(referrer: String, viewer: Option[String], product: String, metric: String, location: String,
        video: String, uuid: String, url: String, timestamp: String)
case class OrderEvent(referrer: String, viewer: String, product: String, metric: String, location: String,
        video: String, uuid: String, url: String, order: String, timestamp: String)