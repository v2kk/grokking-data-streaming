package grokking.data.streaming.jobs

import scala.util.Random
import org.apache.spark.sql.SparkSession
import net.liftweb.json._

object Test {

    def main(args: Array[String]) {
        
        val metrics = Seq("video_play", "login", "buy")
        val devices = Seq("Samsung", "iPhone", "nokia", "motorola", "Sharp", "apple", "sony", "lumia")
        val ips = Seq("207.133.165.175", "66.37.216.225", "62.22.133.75", "106.230.28.62", "17.4.119.71")
        val hosts = Seq("vnexpress.vn", "tuoitre.vn", "java.com", "apache.org", "vinaclips.com", "zing.vn")
        val random = Random
        
        val metric = metrics(random.nextInt(3))
        var mp = scala.collection.mutable.Map[String, Any]()
        mp += ("device" -> devices(random.nextInt(8)))
        mp += ("user_id" -> random.nextInt(100))
        mp += ("ip" -> ips(random.nextInt(5)))
        mp += ("host" -> hosts(random.nextInt(6)))
        mp += ("revenue" -> random.nextInt(100) * 1000)
            
        val jsonObj = scala.util.parsing.json.JSONObject(mp.toMap)
        val json = jsonObj.toString()
        println(json)
        
        val spark = SparkSession.builder
        .master("yarn")
        .appName("spark-streaming-demo")
        .getOrCreate()
        
        
        val sc = spark.sparkContext
        val sqlContext = spark.sqlContext
        import sqlContext.implicits._
        
        val data = Seq(("1479399142492", json), ("1479399172492", json), ("1479399152492", json), ("1479399162492", json))
        val rdd = sc.parallelize(data)
        
        case class PageViewEvent(referrer: String, viewer: String, product: String, metric: String, location: String,
        video: String, uuid: String, url: String, timestamp: String)
        case class Event(ip: String, user_id: Int, device: String, host: String, revenue: Int)
        
        println("TEST: 4")
        println("TEST: " + rdd.count)
                
        println("TEST: map")
        val count = rdd.map(line => {
            
            /*val redis = new RedisClient("s2", 6379)
            println("TEST: " + line._1)
            redis.zincrby("pageview", 1, "201611221122")*/
            line._1
        }).toDF.count()
        
        println("TEST: end " + count)
        spark.stop()
    }
}