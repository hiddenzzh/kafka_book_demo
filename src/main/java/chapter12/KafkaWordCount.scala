package chapter12

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

/**
  * Created by 朱小厮 on 2018/8/16.
  * demo: http://spark.apache.org/docs/2.3.1/streaming-kafka-0-10-integration.html
  */
object KafkaWordCount {
  private val brokers = "localhost:9092"
  private val topic = "topic-spark"
  private val group = "group-spark"
  private val checkpointDir = "/opt/kafka/checkpoint"

  def main(args: Array[String]): Unit ={
    val sparkConf = new SparkConf().setMaster("local").setAppName("KafkaWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint(checkpointDir)

    val kafkaParam = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.GROUP_ID_CONFIG -> group,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false:java.lang.Boolean)
    )
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc, PreferConsistent, Subscribe[String, String](List(topic), kafkaParam))

    val words = stream.map(record=>record.value()).flatMap(_.split(" "))
    val wordCount = words.map(x => (x, 1)).reduceByKeyAndWindow(_ + _, _-_,Minutes(5),Seconds(2),2)
    wordCount.print

    ssc.start
    ssc.awaitTermination
  }
}
