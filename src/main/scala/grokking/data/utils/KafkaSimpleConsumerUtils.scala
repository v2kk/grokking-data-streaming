package grokking.data.utils

import kafka.consumer.SimpleConsumer
import kafka.common.TopicAndPartition
import kafka.api._
import scala.annotation.tailrec
import scala.util._
import kafka.common.KafkaException

object KafkaSimpleConsumerUtils {

    def getLastOffset(consumer: SimpleConsumer, topic: String, partition: Int, whichTime: Long, clientName: String): Try[Long] = {
        val tap = new TopicAndPartition(topic, partition)
        val request = new kafka.api.OffsetRequest(Map(tap -> PartitionOffsetRequestInfo(whichTime, 1)))
        val response = consumer.getOffsetsBefore(request)

        if (response.hasError) {
            val err = response.partitionErrorAndOffsets(tap).error
            Failure(new KafkaException("Error fetching data Offset Data the Broker. Reason: " + err))
        } else {
            //offsets is sorted in descending order, we always want the first
            Success(response.partitionErrorAndOffsets(tap).offsets(0))
        }
    }

    @tailrec
    def findConsumerAndDo[T](seedBrokers: List[String], port: Int, consumerName: String)(f: SimpleConsumer => T): Option[(SimpleConsumer, T)] =
        seedBrokers match {
            case Nil => None
            case seed :: others =>
                Try {
                    val consumer = new SimpleConsumer(seed, port, 100000, 64 * 1024, consumerName)
                    val r = f(consumer)
                    Some((consumer, r))
                } match {
                    case Success(r) => r
                    case Failure(ex) =>
                        findConsumerAndDo(others, port, consumerName)(f)
                }
        }

    def findLeader(seedBrokers: List[String], port: Int, consumerName: String, topic: String, partition: Int): Option[PartitionMetadata] = {
        findConsumerAndDo(seedBrokers, port, consumerName) { consumer =>
            findLeader(consumer, topic, partition)
        }.map { _._2 }.flatten
    }

    def findLeader(consumer: SimpleConsumer, topic: String, partition: Int): Option[PartitionMetadata] = {
        val topics = Seq(topic)
        val req: TopicMetadataRequest = new TopicMetadataRequest(topics, 0)
        val resp: TopicMetadataResponse = consumer.send(req)
        val metaData = resp.topicsMetadata
        (for {
            item <- metaData.iterator
            part <- item.partitionsMetadata.iterator
        } yield part).find(part => part.partitionId == partition)
    }
    
    def getTopicOffset(seedBrokers: List[String], port: Int, consumerName: String, topic: String): Map[Int, Long] = {
        
        var results: Map[Int, Long] = Map[Int, Long]()
        findConsumerAndDo(seedBrokers, port, consumerName) { consumer =>
            results ++= findLeader(consumer, topic)
        }
        
        results
    }
    
    def findLeader(consumer: SimpleConsumer, topic: String): Map[Int, Long] = {
        val topics = Seq(topic)
        val req: TopicMetadataRequest = new TopicMetadataRequest(topics, 0)
        val resp: TopicMetadataResponse = consumer.send(req)
        val metaData = resp.topicsMetadata

        val partitionsMetadata = (for {
            item <- metaData.iterator
            part <- item.partitionsMetadata.iterator
        } yield part)
        
        var results: Map[Int, Long] = Map[Int, Long]()
        partitionsMetadata.foreach { partition => 
            
            val offset = getLastOffset(consumer, topic, partition.partitionId, OffsetRequest.LatestTime, "getLastOffset")
            
            results += (partition.partitionId -> offset.getOrElse(-1))
        }
        
        results
    }
}