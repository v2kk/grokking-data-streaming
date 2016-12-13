package grokking.data.batch.storage

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.kafka.OffsetRange
import kafka.serializer.StringDecoder
import java.sql.Timestamp
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.SaveMode
import java.util.Properties
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.streaming.kafka.HasOffsetRanges
import kafka.api.TopicMetadataRequest
import kafka.consumer.SimpleConsumer
import kafka.api.PartitionMetadata
import java.util.Collections
import grokking.data.utils.KafkaSimpleConsumerUtils
import scalikejdbc._
import grokking.data.utils.SetupJdbc

object KafkaToPostgresql {

    private val appName = "KafkaToPostgresql"
    private val jdbcDriver = "org.postgresql.Driver"
    private val jdbcUrl = "jdbc:postgresql://Team2-Server1:5432/svcdb?stringtype=unspecified&rewriteBatchedStatements=true"
    private val jdbcUser = "stackops"
    private val jdbcPassword = "stpg@team2"
    
    def createKafkaRdd(sc: SparkContext, kafkaParams: Map[String, String],
            offsetRanges: Array[OffsetRange]): RDD[(String, String)] = {
        
        KafkaUtils.createRDD[String, String, StringDecoder, StringDecoder](sc, kafkaParams, offsetRanges)
    }
    
    def main(args: Array[String]){
        
        val topic = "prod-" + args(0)
        val metric = args(0)
        
        val spark = {
            SparkSession.builder.master("yarn").appName(appName)
            .getOrCreate()
        }
        
        val kafkaParams = Map[String, String](
            "bootstrap.servers" -> "Team2-Server2:6667"
        )
        
        val schema = 
            StructType(
                Array(
                    StructField("metric", StringType, true),
                    StructField("log_date", TimestampType, true),
                    StructField("data", StringType, true)
            )
        )
        
        val lstBrokers = List[String]("Team2-Server2")
        val topicOffset = KafkaSimpleConsumerUtils.getTopicOffset(lstBrokers, 6667, "getTopicMeta", topic)
        
        SetupJdbc(jdbcDriver, jdbcUrl, jdbcUser, jdbcPassword)
        
        // begin from the the offsets committed to the database
        val fromOffsets = DB.readOnly { implicit session =>
          sql"""select part, off from svcdb.topic_offsets where topic = ${topic}""".
            map { resultSet =>
              resultSet.int(1) -> resultSet.long(2)
            }.list.apply().toMap
        }
        
        var offsetRanges: Array[OffsetRange] = Array()
        for(item <- topicOffset){
         
            val partitionId = item._1
            val latestOffset = item._2
            var fromOffset = 0L
            
            if(fromOffsets.getOrElse(partitionId, 0L) != 0L){
                fromOffset = fromOffsets(partitionId)
            }
            
            offsetRanges :+= OffsetRange(topic, partitionId, fromOffset, latestOffset)
        }
        
        val rdd = createKafkaRdd(spark.sparkContext, kafkaParams, offsetRanges)
        val ranges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        
        val logDF = spark.createDataFrame(rdd.map{
                
            event => 
                Row(metric, new Timestamp(event._1.toLong), event._2)
        }, schema)
        
        // insert line by line
        //logDF.write.mode(SaveMode.Append).jdbc("jdbc:postgresql://Team2-Server2:5432/svcdb?stringtype=unspecified", "svcdb.log_event", prop)
        
        DB.localTx { implicit session =>
            
            val prop = new Properties() 
        
            prop.put("user", jdbcUser)
            prop.put("password", jdbcPassword)
            prop.put("driver", jdbcDriver)
            prop.put("batchsize", "10000")
        
            // store metric data for this partition
            // batch insert
            JdbcUtils.saveTable(logDF, jdbcUrl, "svcdb.log_event", prop)
            
            // store offset
            for(item <- topicOffset){
         
                val partitionId = item._1
                val latestOffset = item._2
                
                val offsetRows = sql"""update svcdb.topic_offsets set off = ${latestOffset}
                 where topic = ${topic} and part = ${partitionId}""".update.apply()
                if (offsetRows != 1) {
                    throw new Exception(s"""Got $offsetRows rows affected instead of 1 when attempting to update offsets""")
                }
            }
        }
    }
}